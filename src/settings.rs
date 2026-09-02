use crate::{
    ingest_bresser_station, ingest_rtl433_events, ingest_shelly_switch, send_periodic_message,
};
use serde::Deserialize;
use url::Url;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub mqtt: MqttSettings,
    pub influx: InfluxSettings,

    pub ingest_shelly_switch: Option<ingest_shelly_switch::Settings>,
    pub ingest_bresser_station: Option<ingest_bresser_station::Settings>,
    pub ingest_rtl433_events: Option<ingest_rtl433_events::Settings>,

    pub send_periodic_message: Vec<send_periodic_message::Settings>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct InfluxSettings {
    pub url: Url,
    pub org: String,
    pub token: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct MqttSettings {
    pub host: String,
    pub port: u16,

    #[serde(default = "default_client_id")]
    pub client_id: String,

    pub user: String,
    pub password: String,
}

fn default_client_id() -> String {
    format!("telemetry-bridge-{:4x}", rand::random::<u16>())
}
