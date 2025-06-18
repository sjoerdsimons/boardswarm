use std::{
    collections::{HashMap, VecDeque},
    ffi::OsStr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::{registry::Properties, ActuatorId, ConsoleId, Server, VolumeId};
use futures::{ready, Stream};
use tokio_udev::{AsyncMonitorSocket, Enumerator};
use tracing::{info, warn};

#[derive(Debug, Clone, Copy)]
enum Registration {
    Volume(VolumeId),
    Actuator(ActuatorId),
    Console(ConsoleId),
}

#[derive(Debug)]
enum RegistrationState {
    /// Pending registration for a given udev sequence number
    Pending(u64),
    /// Registered with volume id
    Registered(Vec<Registration>),
}

#[derive(Clone)]
pub struct DeviceRegistrations {
    server: Server,
    registrations: Arc<Mutex<HashMap<PathBuf, RegistrationState>>>,
}

impl DeviceRegistrations {
    pub fn new(server: Server) -> Self {
        DeviceRegistrations {
            server,
            registrations: Default::default(),
        }
    }

    /// Mark a device as being a prepared for final registration. This ensures on the iteration
    /// with the given sequence number can get registered, avoiding races where a device disappears
    /// and re-appears while the preperation process is ongoing
    pub fn pre_register(&self, device: &Device, seqnum: u64) -> PreRegistration {
        let mut registrations = self.registrations.lock().unwrap();
        let syspath = device.syspath().to_path_buf();
        if let Some(existing) =
            registrations.insert(syspath.clone(), RegistrationState::Pending(seqnum))
        {
            warn!("Pre-registering with known previous item: {:?}", existing);
            if let RegistrationState::Registered(registrations) = existing {
                self.unregister(&registrations);
            }
        }
        PreRegistration {
            syspath: device.syspath().to_path_buf(),
            seqnum,
            pending: self.clone(),
        }
    }

    pub fn remove(&self, device: &Device) {
        let mut registrations = self.registrations.lock().unwrap();
        if let Some(RegistrationState::Registered(registrations)) =
            registrations.remove(device.syspath())
        {
            self.unregister(&registrations);
        }
    }

    fn unregister(&self, registrations: &[Registration]) {
        for r in registrations {
            match r {
                Registration::Volume(id) => self.server.unregister_volume(*id),
                Registration::Actuator(id) => self.server.unregister_actuator(*id),
                Registration::Console(id) => self.server.unregister_console(*id),
            }
        }
    }
}

pub struct RegistrationGuard<'a> {
    server: &'a Server,
    registrations: Vec<Registration>,
}

impl RegistrationGuard<'_> {
    pub fn register_actuator<A>(&mut self, properties: Properties, item: A)
    where
        A: crate::Actuator + 'static,
    {
        let id = self.server.register_actuator(properties, item);
        self.registrations.push(Registration::Actuator(id));
    }

    pub fn register_console<C>(&mut self, properties: Properties, item: C)
    where
        C: crate::Console + 'static,
    {
        let id = self.server.register_console(properties, item);
        self.registrations.push(Registration::Console(id));
    }

    pub fn register_volume<V>(&mut self, properties: Properties, item: V)
    where
        V: crate::Volume + 'static,
    {
        let id = self.server.register_volume(properties, item);
        self.registrations.push(Registration::Volume(id));
    }
}

pub struct PreRegistration {
    syspath: PathBuf,
    seqnum: u64,
    pending: DeviceRegistrations,
}

impl PreRegistration {
    pub fn register_batch<F>(self, r: F)
    where
        F: FnOnce(&mut RegistrationGuard),
    {
        let mut registrations = self.pending.registrations.lock().unwrap();
        match registrations.get(&self.syspath) {
            Some(RegistrationState::Pending(s)) if *s == self.seqnum => {
                let mut guard = RegistrationGuard {
                    server: &self.pending.server,
                    registrations: vec![],
                };

                r(&mut guard);

                registrations.insert(
                    self.syspath.clone(),
                    RegistrationState::Registered(guard.registrations),
                );
            }
            _ => {
                info!("Ignoring outdated registration (seqnum: {})", self.seqnum)
            }
        }
    }

    #[allow(dead_code)]
    pub fn register_actuator<A>(self, properties: Properties, item: A)
    where
        A: crate::Actuator + 'static,
    {
        self.register_batch(|r| r.register_actuator(properties, item));
    }

    #[allow(dead_code)]
    pub fn register_console<C>(self, properties: Properties, item: C)
    where
        C: crate::Console + 'static,
    {
        self.register_batch(|r| r.register_console(properties, item));
    }

    pub fn register_volume<V>(self, properties: Properties, item: V)
    where
        V: crate::Volume + 'static,
    {
        self.register_batch(|r| r.register_volume(properties, item));
    }
}

impl Drop for PreRegistration {
    fn drop(&mut self) {
        let mut registrations = self.pending.registrations.lock().unwrap();
        match registrations.get(&self.syspath) {
            Some(RegistrationState::Pending(s)) if *s == self.seqnum => {
                registrations.remove(&self.syspath);
            }
            _ => {}
        }
    }
}

pub struct DeviceStream {
    existing: VecDeque<(u64, Device)>,
    monitor: AsyncMonitorSocket,
}

impl DeviceStream {
    pub fn new<O: AsRef<OsStr>>(subsystem: O) -> Result<Self, std::io::Error> {
        let monitor = tokio_udev::MonitorBuilder::new()?
            .match_subsystem(&subsystem)?
            .listen()?;
        let monitor = tokio_udev::AsyncMonitorSocket::new(monitor)?;

        let mut enumerator = Enumerator::new()?;
        enumerator.match_subsystem(&subsystem)?;
        let existing = enumerator
            .scan_devices()?
            .map(Device)
            .enumerate()
            .map(|(i, d)| (i as u64, d))
            .collect();

        Ok(Self { existing, monitor })
    }
}

pub enum DeviceEvent {
    Add { device: Device, seqnum: u64 },
    Remove(Device),
}

impl Stream for DeviceStream {
    type Item = DeviceEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let me = self.get_mut();
        if let Some((seqnum, device)) = me.existing.pop_front() {
            Poll::Ready(Some(DeviceEvent::Add { device, seqnum }))
        } else {
            loop {
                let Some(event) = ready!(Pin::new(&mut me.monitor).poll_next(cx)) else {
                    return Poll::Ready(None);
                };
                let event = match event {
                    Ok(event) => event,
                    Err(e) => {
                        warn!("Udev event monitor error: {:?}", e);
                        continue;
                    }
                };
                match event.event_type() {
                    tokio_udev::EventType::Add => {
                        return Poll::Ready(Some(DeviceEvent::Add {
                            device: Device(event.device()),
                            seqnum: event.sequence_number(),
                        }))
                    }
                    tokio_udev::EventType::Remove => {
                        return Poll::Ready(Some(DeviceEvent::Remove(Device(event.device()))))
                    }
                    _ => continue,
                }
            }
        }
    }
}

const PROPERTY_BLACKLIST: &[&str] = &[
    "ACTION",
    "DRIVER",
    "MAJOR",
    "MODALIAS",
    "MINOR",
    "USEC_INITIALIZED",
];

pub struct Device(tokio_udev::Device);
impl Device {
    #[allow(dead_code)]
    pub fn udev_device(&self) -> &tokio_udev::Device {
        &self.0
    }

    pub fn syspath(&self) -> &Path {
        self.0.syspath()
    }

    pub fn devnode(&self) -> Option<&Path> {
        self.0.devnode()
    }

    pub fn properties<S: Into<String>>(&self, name: S) -> Properties {
        fn find_reasonable_parent(device: tokio_udev::Device) -> VecDeque<tokio_udev::Device> {
            let reasonable = matches!(
                (
                    device.subsystem().and_then(|s| s.to_str()),
                    device.devtype().and_then(|s| s.to_str()),
                ),
                (Some("pci"), _) | (Some("platform"), _) | (Some("usb"), Some("usb_device"))
            );
            let mut v = if reasonable {
                VecDeque::new()
            } else if let Some(p) = device.parent() {
                find_reasonable_parent(p)
            } else {
                VecDeque::new()
            };
            v.push_back(device);
            v
        }

        let mut properties = Properties::new(name);
        let chain = find_reasonable_parent(self.0.clone());
        for d in chain {
            for p in d.properties() {
                let name = p.name().to_string_lossy();
                if PROPERTY_BLACKLIST.contains(&&*name) {
                    continue;
                }
                let key = format!("udev.{}", name);
                let value = p.value().to_string_lossy();
                properties.insert(key, value);
            }
        }

        properties
    }

    pub fn property(&self, property: &str) -> Option<&str> {
        self.0.property_value(property)?.to_str()
    }

    pub fn property_u64(&self, property: &str, radix: u32) -> Option<u64> {
        self.0
            .property_value(property)?
            .to_str()
            .and_then(|u| u64::from_str_radix(u, radix).ok())
    }

    pub fn parent(&self) -> Option<Device> {
        self.0.parent().map(Device)
    }

    pub fn is_usb_device(&self) -> bool {
        self.0.devtype() == Some(OsStr::new("usb_device"))
    }

    pub fn usb_interfaces(&self) -> Option<Vec<UsbInterface>> {
        if self.is_usb_device() {
            Some(UsbInterface::from_udev(
                self.0.property_value("ID_USB_INTERFACES")?.to_str()?,
            ))
        } else {
            None
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct UsbInterface {
    pub class: u8,
    pub subclass: u8,
    pub protocol: u8,
}

impl UsbInterface {
    // Create from udev ID_USB_INTERFACES property
    pub fn from_udev(property: &str) -> Vec<UsbInterface> {
        property
            .split(':')
            .filter_map(|intf| {
                if intf.len() == 6 {
                    let class = u8::from_str_radix(&intf[0..2], 16).ok()?;
                    let subclass = u8::from_str_radix(&intf[2..4], 16).ok()?;
                    let protocol = u8::from_str_radix(&intf[4..6], 16).ok()?;
                    Some(UsbInterface {
                        class,
                        subclass,
                        protocol,
                    })
                } else {
                    None
                }
            })
            .collect()
    }
}

#[test]
fn udev_interface_string() {
    let tests: &[(_, &[_])] = &[
        (
            ":123456:",
            &[UsbInterface {
                class: 0x12,
                subclass: 0x34,
                protocol: 0x56,
            }],
        ),
        (
            ":123456:673489:",
            &[
                UsbInterface {
                    class: 0x12,
                    subclass: 0x34,
                    protocol: 0x56,
                },
                UsbInterface {
                    class: 0x67,
                    subclass: 0x34,
                    protocol: 0x89,
                },
            ],
        ),
        ("::", &[]),
        (":", &[]),
        ("", &[]),
    ];
    for (prop, expected) in tests {
        let v = UsbInterface::from_udev(prop);
        assert_eq!(&v, expected, "Unexpected result for {prop}");
    }
}
