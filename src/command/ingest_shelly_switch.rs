use crate::command::CommandContext;
use crate::influx;
use crate::influx::{InfluxOptions, Precision};
use crate::line_protocol::Point;
use anyhow::{Context, Result, anyhow};
use clap::Args;
use rumqttc::v5::mqttbytes::QoS;
use serde::Deserialize;
use std::str::from_utf8;
use tracing::info;

#[derive(Args, Debug)]
pub struct IngestShellySwitch {
    #[clap(flatten)]
    pub influx_options: InfluxOptions,

    #[arg(long, env = "TB_INFLUX_BUCKET", default_value = "devices")]
    pub influx_bucket: String,
}

#[derive(Deserialize, Debug)]
struct MessagePayload {
    output: bool,

    voltage: f64,
    current: f64,

    #[serde(rename = "apower")]
    a_power: f64,

    #[serde(rename = "aenergy")]
    a_energy: AEnergy,
}

#[derive(Deserialize, Debug)]
struct AEnergy {
    total: f64,
}

pub async fn run(command: IngestShellySwitch, mut context: CommandContext) -> Result<()> {
    let influx_client = influx::Client::new(command.influx_options);

    info!("Starting ingestion...");

    context
        .mqtt_client
        .subscribe("shelly/plug-s-gen2/+/status/switch:0", QoS::AtMostOnce)
        .await
        .context("Failed to subscribe to topic.")?;

    loop {
        let Some(message) = context.message_rx.recv().await else {
            return Ok(());
        };

        let mut point = Point::new("switch");

        let device_name = from_utf8(&message.packet.topic)
            .context("Failed to decode topic")?
            .split('/')
            .nth(2)
            .ok_or_else(|| anyhow!("Failed to parse topic."))?;

        point
            .add_tag("deviceType", "shelly/plug-s-gen2")
            .add_tag("deviceName", device_name);

        let payload: MessagePayload = serde_json::from_slice(&message.packet.payload)
            .context("Failed to parse message payload.")?;

        point
            .add_bool_field("switchedOn", payload.output)
            .add_float_field("power_W", payload.a_power)?
            .add_float_field("voltage_V", payload.voltage)?
            .add_float_field("current_A", payload.current)?
            .add_float_field("totalEnergy_Wh", payload.a_energy.total)?;

        point.set_timestamp(message.timestamp.timestamp_millis());

        influx_client
            .write(&command.influx_bucket, point, Precision::Millisecond)
            .await
            .context("Failed to write switch state to Influx.")?;
    }
}
