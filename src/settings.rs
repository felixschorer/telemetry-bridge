use crate::{ingest, publish_message};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub mqtt: MqttSettings,
    pub influx: InfluxSettings,

    pub ingest: Option<ingest::Settings>,

    pub publish_messages: Vec<publish_message::Settings>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InfluxSettings {
    pub url: Url,
    pub org: String,
    pub token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MqttSettings {
    pub host: String,
    pub port: u16,

    pub client_id: String,

    pub user: String,
    pub password: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            mqtt: MqttSettings::default(),
            influx: InfluxSettings::default(),

            ingest: Some(ingest::Settings::default()),

            publish_messages: vec![publish_message::Settings::default()],
        }
    }
}

impl Default for InfluxSettings {
    fn default() -> Self {
        Self {
            url: "http://influxdb.lan".parse().unwrap(),
            org: "iot".to_owned(),
            token: "".to_owned(),
        }
    }
}

impl Default for MqttSettings {
    fn default() -> Self {
        Self {
            host: "mqtt.lan".to_owned(),
            port: 1883,

            client_id: "telemetry-bridge".to_owned(),

            user: "".to_owned(),
            password: "".to_owned(),
        }
    }
}
