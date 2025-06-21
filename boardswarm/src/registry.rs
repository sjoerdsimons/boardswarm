use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use tokio::sync::broadcast;
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
pub struct Item<T> {
    properties: Arc<Properties>,
    item: T,
}

impl<T> std::fmt::Display for Item<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(instance) = self.instance() {
            write!(f, "{} on {}", self.name(), instance)
        } else {
            write!(f, "{}", self.name())
        }
    }
}

impl<T> Item<T> {
    pub fn new(properties: Properties, item: T) -> Self {
        Item {
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

    pub fn inner(&self) -> &T {
        &self.item
    }

    pub fn into_inner(self) -> T {
        self.item
    }
}

pub trait RegistryIndex: Eq + PartialEq + Copy + Default + Ord + std::hash::Hash {
    fn next(&self) -> Self;
}

impl RegistryIndex for u64 {
    fn next(&self) -> Self {
        self + 1
    }
}

#[derive(Clone)]
pub enum RegistryChange<I, T> {
    Added { id: I, item: Item<T> },
    Removed(I),
}

#[derive(Debug)]
struct RegistryInner<I, T> {
    next: I,
    contents: BTreeMap<I, Item<T>>,
}

#[derive(Debug)]
pub struct Registry<I, T> {
    monitor: broadcast::Sender<RegistryChange<I, T>>,
    inner: RwLock<RegistryInner<I, T>>,
}

impl<I, T> Registry<I, T>
where
    I: RegistryIndex,
    T: Clone,
{
    pub fn new() -> Self {
        Self {
            monitor: broadcast::channel(16).0,
            inner: RwLock::new(RegistryInner {
                next: I::default(),
                contents: BTreeMap::new(),
            }),
        }
    }

    pub fn add(&self, properties: Properties, item: T) -> (I, Item<T>) {
        let item = Item::new(properties, item);
        let mut inner = self.inner.write().unwrap();
        let id = inner.next;
        inner.next = inner.next.next();
        inner.contents.insert(id, item.clone());
        let _ = self.monitor.send(RegistryChange::Added {
            id,
            item: item.clone(),
        });
        (id, item)
    }

    pub fn remove(&self, id: I) {
        let mut inner = self.inner.write().unwrap();
        if let Some(_item) = inner.contents.remove(&id) {
            let _ = self.monitor.send(RegistryChange::Removed(id));
        }
    }

    pub fn lookup(&self, id: I) -> Option<Item<T>> {
        let inner = self.inner.read().unwrap();
        inner.contents.get(&id).cloned()
    }

    #[allow(dead_code)]
    pub fn ids(&self) -> Vec<I> {
        let inner = self.inner.read().unwrap();
        inner.contents.keys().copied().collect()
    }

    pub fn contents(&self) -> Vec<(I, Item<T>)> {
        let inner = self.inner.read().unwrap();
        inner
            .contents
            .iter()
            .map(|(&id, item)| (id, item.clone()))
            .collect()
    }

    pub fn find<'a, K, V, IT>(&self, matches: &'a IT) -> Option<(I, Item<T>)>
    where
        K: AsRef<str>,
        V: AsRef<str>,
        &'a IT: IntoIterator<Item = (K, V)>,
    {
        let inner = self.inner.read().unwrap();
        inner
            .contents
            .iter()
            .find(|(&_id, item)| item.properties.matches(matches))
            .map(|(&id, item)| (id, item.clone()))
    }

    pub fn monitor(&self) -> Receiver<RegistryChange<I, T>> {
        self.monitor.subscribe()
    }
}

impl<I, T> Default for Registry<I, T>
where
    I: RegistryIndex,
    T: Clone,
{
    fn default() -> Self {
        Self::new()
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
