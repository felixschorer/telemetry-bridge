use chrono::{DateTime, Utc};
use rumqttc::v5::mqttbytes::v5::Publish;

#[derive(Clone, Debug)]
pub struct Message {
    pub timestamp: DateTime<Utc>,
    pub packet: Publish,
}

impl Message {
    pub fn new(timestamp: DateTime<Utc>, packet: Publish) -> Self {
        Self { timestamp, packet }
    }

    pub fn with_current_timestamp(packet: Publish) -> Self {
        Self::new(Utc::now(), packet)
    }
}
