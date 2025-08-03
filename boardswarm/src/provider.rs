use std::sync::Arc;

use crate::{
    config,
    registry::{self, Properties},
    ActuatorId, ConsoleId, DeviceId, Server, VolumeId,
};

#[derive(Clone)]
pub struct Provider {
    server: Server,
    config: Arc<config::Provider>,
}

impl Provider {
    pub fn new(server: Server, config: config::Provider) -> Self {
        Provider {
            server,
            config: Arc::new(config),
        }
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn provider(&self) -> &str {
        &self.config.provider
    }

    pub fn parameters(&self) -> Option<&serde_yaml::Value> {
        self.config.parameters.as_ref()
    }

    pub fn server(&self) -> &Server {
        &self.server
    }

    fn extend_properties(&self, mut properties: Properties) -> Properties {
        let provider_properties = &[
            (registry::PROVIDER_NAME, self.name()),
            (registry::PROVIDER, self.provider()),
        ];
        properties.extend(provider_properties);
        properties
    }

    fn match_acl(&self, properties: &Properties) -> Vec<String> {
        self.config
            .acls
            .iter()
            .find(|p| properties.matches(&p.match_))
            .map(|p| p.roles.clone())
            .unwrap_or_default()
    }

    pub fn register_actuator<A>(&self, properties: Properties, item: A) -> ActuatorId
    where
        A: crate::Actuator + 'static,
    {
        let properties = self.extend_properties(properties);
        let acl = self.match_acl(&properties);
        self.server.register_actuator(properties, acl, item)
    }

    pub fn unregister_actuator(&self, id: ActuatorId) {
        self.server.unregister_actuator(id);
    }

    pub fn register_console<C>(&self, properties: Properties, item: C) -> ConsoleId
    where
        C: crate::Console + 'static,
    {
        let properties = self.extend_properties(properties);
        let acl = self.match_acl(&properties);
        self.server.register_console(properties, acl, item)
    }

    pub fn unregister_console(&self, id: ConsoleId) {
        self.server.unregister_console(id);
    }

    pub fn register_volume<V>(&self, properties: Properties, item: V) -> VolumeId
    where
        V: crate::Volume + 'static,
    {
        let properties = self.extend_properties(properties);
        let acl = self.match_acl(&properties);
        self.server.register_volume(properties, acl, item)
    }

    pub fn unregister_volume(&self, id: VolumeId) {
        self.server.unregister_volume(id);
    }

    pub fn register_device<D>(&self, properties: Properties, item: D) -> DeviceId
    where
        D: crate::Device + 'static,
    {
        let properties = self.extend_properties(properties);
        let acl = self.match_acl(&properties);
        self.server.register_device(properties, acl, item)
    }

    pub fn unregister_device(&self, id: DeviceId) {
        self.server.unregister_device(id);
    }
}
