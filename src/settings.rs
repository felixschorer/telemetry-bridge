use crate::{
    ingest_bresser_station, ingest_rtl433_events, ingest_shelly_switch, send_periodic_message,
};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub mqtt: MqttSettings,
    pub influx: InfluxSettings,

    pub ingest_shelly_switch: Option<ingest_shelly_switch::Settings>,
    pub ingest_bresser_station: Option<ingest_bresser_station::Settings>,
    pub ingest_rtl433_events: Option<ingest_rtl433_events::Settings>,

    pub send_periodic_message: Vec<send_periodic_message::Settings>,
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

            ingest_shelly_switch: Some(ingest_shelly_switch::Settings::default()),
            ingest_bresser_station: Some(ingest_bresser_station::Settings::default()),
            ingest_rtl433_events: Some(ingest_rtl433_events::Settings::default()),

            send_periodic_message: vec![send_periodic_message::Settings::default()],
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
