use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;

pub const NAME: &str = "boardswarm.name";
pub const INSTANCE: &str = "boardswarm.instance";
pub const PROVIDER: &str = "boardswarm.provider";
pub const PROVIDER_NAME: &str = "boardswarm.provider.name";

#[derive(Clone, Debug)]
pub struct Properties {
    properties: HashMap<String, String>,
}

impl Properties {
    pub fn new<N: Into<String>>(name: N) -> Self {
        let mut properties = HashMap::new();
        properties.insert(NAME.to_string(), name.into());

        Self { properties }
    }

    pub fn name(&self) -> &str {
        self.get(NAME).unwrap_or_default()
    }

    pub fn instance(&self) -> Option<&str> {
        self.get(INSTANCE)
    }

    pub fn get(&self, prop: &str) -> Option<&str> {
        self.properties.get(prop).map(String::as_ref)
    }

    /// Tests if matches is a subset of the properties
    ///
    /// If properties is from a remote instance (`boardswarm.instance` is set) that has to be
    /// explicitly matched otherwise it's a pure subset match (e.g. an empty set matches)
    pub fn matches<K, V, I>(&self, matches: I) -> bool
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut matched_instance = false;
        let matched = matches.into_iter().all(|(k, v)| {
            matched_instance |= k.as_ref() == INSTANCE;
            if let Some(prop) = self.get(k.as_ref()) {
                prop == v.as_ref()
            } else {
                false
            }
        });

        // All the properties need to match and if the instance is declared in the properties that
        // also needed to be matched against
        matched && matched_instance == self.instance().is_some()
    }

    pub fn insert<K, V>(&mut self, key: K, value: V)
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.properties.insert(key.into(), value.into());
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.properties.iter()
    }
}

impl<K, V> Extend<(K, V)> for Properties
where
    K: Into<String>,
    V: Into<String>,
{
    fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
        for (key, value) in iter.into_iter() {
            self.properties.insert(key.into(), value.into());
        }
    }
}

impl<'a, K, V> Extend<&'a (K, V)> for Properties
where
    K: ToString,
    V: ToString,
{
    fn extend<T: IntoIterator<Item = &'a (K, V)>>(&mut self, iter: T) {
        for (key, value) in iter.into_iter() {
            self.properties.insert(key.to_string(), value.to_string());
        }
    }
}

impl From<HashMap<String, String>> for Properties {
    fn from(properties: HashMap<String, String>) -> Self {
        Properties { properties }
    }
}

#[derive(Clone, Debug)]
pub struct Item<A, T> {
    acl: Arc<A>, // acl
    properties: Arc<Properties>,
    item: T,
}

impl<A, T> std::fmt::Display for Item<A, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(instance) = self.instance() {
            write!(f, "{} on {}", self.name(), instance)
        } else {
            write!(f, "{}", self.name())
        }
    }
}

impl<A, T> Item<A, T> {
    pub fn new(acl: A, properties: Properties, item: T) -> Self {
        Item {
            acl: Arc::new(acl),
            properties: Arc::new(properties),
            item,
        }
    }
    pub fn name(&self) -> &str {
        self.properties.name()
    }

    pub fn instance(&self) -> Option<&str> {
        self.properties.instance()
    }

    pub fn properties(&self) -> Arc<Properties> {
        self.properties.clone()
    }

    pub fn acl(&self) -> &A {
        &self.acl
    }

    pub fn inner(&self) -> &T {
        &self.item
    }

    pub fn into_inner(self) -> T {
        self.item
    }
}

/// Trait a type needs to implement to be used as an index of the registery.
///
/// Each index has to be unique for the duration of the process. A simple way to do so is to simply
/// increment a suitable big number type. e.g a u64 will only overflow after about 584
/// years when incrementing by one every nanosecond (aka at a 1ghz rate).
pub trait RegistryIndex: Eq + PartialEq + Copy + Default + Ord + std::hash::Hash {
    /// Given the current index, get the next unused index
    fn next(&self) -> Self;
}

impl RegistryIndex for u64 {
    fn next(&self) -> Self {
        self + 1
    }
}

pub trait Verifier: Clone {
    type Credential: Clone + Send + Sync;
    type Acl: Clone + Send + Sync + 'static;
    fn verify(&self, tokens: &Self::Credential, acl: &Self::Acl) -> bool;
}

#[derive(Clone)]
pub struct NoVerification {}
impl Verifier for NoVerification {
    type Credential = ();
    type Acl = ();

    fn verify(&self, _tokens: &Self::Credential, _acl: &Self::Acl) -> bool {
        true
    }
}

#[derive(Clone)]
pub enum RegistryChange<I, A, T> {
    Added { id: I, item: Item<A, T> },
    Removed(I),
}

impl<I, A, T> From<RegistryChangeInternal<I, A, T>> for RegistryChange<I, A, T> {
    fn from(val: RegistryChangeInternal<I, A, T>) -> Self {
        match val {
            RegistryChangeInternal::Added { id, item } => Self::Added { id, item },
            RegistryChangeInternal::Removed { id, .. } => Self::Removed(id),
        }
    }
}

#[derive(Clone)]
enum RegistryChangeInternal<I, A, T> {
    Added { id: I, item: Item<A, T> },
    Removed { id: I, acl: Arc<A> },
}

impl<I, A, T> RegistryChangeInternal<I, A, T> {
    fn acl(&self) -> &A {
        match self {
            RegistryChangeInternal::Added { item, .. } => item.acl(),
            RegistryChangeInternal::Removed { acl, .. } => acl,
        }
    }
}

#[derive(Debug)]
struct RegistryInner<I, A, T> {
    next: I,
    contents: BTreeMap<I, Item<A, T>>,
}

#[derive(Debug)]
pub struct Registry<V: Verifier, I, T> {
    monitor: broadcast::Sender<RegistryChangeInternal<I, V::Acl, T>>,
    verifier: V,
    inner: RwLock<RegistryInner<I, V::Acl, T>>,
}

impl<V, I, T> Registry<V, I, T>
where
    I: RegistryIndex,
    T: Clone,
    V: Verifier + Clone,
{
    pub fn new(verifier: V) -> Self {
        Self {
            verifier,
            monitor: broadcast::channel(1024).0,
            inner: RwLock::new(RegistryInner {
                next: I::default(),
                contents: BTreeMap::new(),
            }),
        }
    }

    pub fn add(&self, acl: V::Acl, properties: Properties, item: T) -> (I, Item<V::Acl, T>) {
        let item = Item::new(acl, properties, item);
        let mut inner = self.inner.write().unwrap();
        let id = inner.next;
        inner.next = inner.next.next();
        inner.contents.insert(id, item.clone());
        let _ = self.monitor.send(RegistryChangeInternal::Added {
            id,
            item: item.clone(),
        });
        (id, item)
    }

    pub fn remove(&self, id: I) {
        let mut inner = self.inner.write().unwrap();
        if let Some(item) = inner.contents.remove(&id) {
            let _ = self
                .monitor
                .send(RegistryChangeInternal::Removed { id, acl: item.acl });
        }
    }

    pub fn lookup(&self, id: I, cred: &V::Credential) -> Option<Item<V::Acl, T>> {
        let inner = self.inner.read().unwrap();
        let item = inner.contents.get(&id)?;
        if self.verifier.verify(cred, &item.acl) {
            Some(item.clone())
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn ids(&self, cred: &V::Credential) -> Vec<I> {
        let inner = self.inner.read().unwrap();
        inner
            .contents
            .iter()
            .filter_map(|(&id, item)| {
                if self.verifier.verify(cred, &item.acl) {
                    Some(id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn contents(&self, token: &V::Credential) -> Vec<(I, Item<V::Acl, T>)> {
        let inner = self.inner.read().unwrap();
        inner
            .contents
            .iter()
            .filter_map(|(&id, item)| {
                if self.verifier.verify(token, &item.acl) {
                    Some((id, item.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn contents_no_auth(&self) -> Vec<(I, Item<V::Acl, T>)> {
        let inner = self.inner.read().unwrap();
        inner
            .contents
            .iter()
            .map(|(&id, item)| (id, item.clone()))
            .collect()
    }

    pub fn find<'a, K, Val, IT>(
        &self,
        matches: &'a IT,
        token: &V::Credential,
    ) -> Option<(I, Item<V::Acl, T>)>
    where
        K: AsRef<str>,
        Val: AsRef<str>,
        &'a IT: IntoIterator<Item = (K, Val)>,
    {
        let inner = self.inner.read().unwrap();
        inner
            .contents
            .iter()
            .find(|(&_id, item)| {
                self.verifier.verify(token, &item.acl) && item.properties.matches(matches)
            })
            .map(|(&id, item)| (id, item.clone()))
    }

    pub fn monitor(&self, cred: V::Credential) -> RegistryMonitor<V, I, T> {
        RegistryMonitor {
            verifier: self.verifier.clone(),
            recv: self.monitor.subscribe(),
            cred: cred.clone(),
        }
    }

    pub fn monitor_no_auth(&self) -> RegistryMonitorNoAuth<V, I, T> {
        RegistryMonitorNoAuth(self.monitor.subscribe())
    }
}

pub struct RegistryMonitor<V: Verifier, I, T> {
    verifier: V,
    recv: Receiver<RegistryChangeInternal<I, V::Acl, T>>,
    cred: V::Credential,
}

impl<V, I, T> RegistryMonitor<V, I, T>
where
    I: Clone,
    T: Clone,
    V: Verifier,
{
    pub async fn recv(&mut self) -> Result<RegistryChange<I, V::Acl, T>, RecvError> {
        loop {
            let i = self.recv.recv().await?;
            if self.verifier.verify(&self.cred, i.acl()) {
                return Ok(i.into());
            }
        }
    }
}

pub struct RegistryMonitorNoAuth<V: Verifier, I, T>(Receiver<RegistryChangeInternal<I, V::Acl, T>>);

impl<V, I, T> RegistryMonitorNoAuth<V, I, T>
where
    V: Verifier,
    I: Clone,
    T: Clone,
{
    pub async fn recv(&mut self) -> Result<RegistryChange<I, V::Acl, T>, RecvError> {
        self.0.recv().await.map(Into::into)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn properties() {
        let mut props = Properties::new("test");
        props.insert("udev.BADGER", "5");

        assert_eq!(props.get(NAME), Some("test"));
        assert_eq!(props.name(), "test");

        let mut t = HashMap::new();
        t.insert(NAME.to_string(), "test".to_string());
        assert!(props.matches(&t));

        let empty: HashMap<String, String> = HashMap::new();
        assert!(props.matches(empty));

        assert!(props.matches([(NAME, "test")]));
        assert!(props.matches([("udev.BADGER", "5")]));

        assert!(!props.matches([(NAME, "no")]));
        assert!(!props.matches([("udev.BADGER", "7")]));
        assert!(!props.matches([("udev.SNAKE", "7")]));

        assert!(props.matches([(NAME, "test"), ("udev.BADGER", "5")]));
        assert!(!props.matches([(NAME, "test"), ("udev.BADGER", "7")]));
        assert!(!props.matches([(NAME, "test"), ("udev.SNAKE", "5")]));
    }
}
