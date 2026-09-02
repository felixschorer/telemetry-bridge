use rumqttc::v5::mqttbytes::v5::Publish;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct TimestampedMessage {
    pub received_at: u128,
    pub publish: Publish,
}

impl TimestampedMessage {
    pub fn at_current_time(publish: Publish) -> Self {
        let received_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Unexpected system time anomaly.")
            .as_millis();

        Self {
            received_at,
            publish,
        }
    }
}
