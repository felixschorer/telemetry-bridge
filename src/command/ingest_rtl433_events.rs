use crate::command::CommandContext;
use crate::influx;
use crate::influx::{InfluxOptions, Precision};
use crate::line_protocol::Point;
use anyhow::{Context, Result};
use clap::Args;
use rumqttc::v5::mqttbytes::QoS;
use serde::Deserialize;
use tracing::info;

#[derive(Args, Debug)]
pub struct IngestRtl433Events {
    #[clap(flatten)]
    pub influx_options: InfluxOptions,

    #[arg(long, env = "TB_INFLUX_BUCKET", default_value = "rtl433")]
    pub influx_bucket: String,
}

#[derive(Deserialize, Debug)]
struct MessagePayload {
    time: String,

    id: u64,
    model: String,

    protocol: u64,

    freq1: f64,
    freq2: f64,

    rssi: f64,
    snr: f64,
    noise: f64,
}

pub async fn run(command: IngestRtl433Events, mut context: CommandContext) -> Result<()> {
    let influx_client = influx::Client::new(command.influx_options);

    info!("Starting ingestion...");

    context
        .mqtt_client
        .subscribe("rtl433/events/+/+", QoS::AtMostOnce)
        .await
        .context("Failed to subscribe to topic.")?;

    loop {
        let Some(message) = context.message_rx.recv().await else {
            return Ok(());
        };

        let payload: MessagePayload = serde_json::from_slice(&message.packet.payload)
            .context("Failed to parse message payload.")?;

        let mut point = Point::new("events");

        point
            .add_tag("id", &payload.id.to_string())
            .add_tag("model", &payload.model)
            .add_tag("protocol", &payload.protocol.to_string());

        point
            .add_float_field("freq1_MHz", payload.freq1)?
            .add_float_field("freq2_MHz", payload.freq2)?
            .add_float_field("rssi_dB", payload.rssi)?
            .add_float_field("snr_dB", payload.snr)?
            .add_float_field("noise_dB", payload.noise)?;

        let epoch_seconds: f64 = payload.time.parse().context("Failed to parse timestamp.")?;
        point.set_timestamp((epoch_seconds * 1000.0) as i64);

        influx_client
            .write(&command.influx_bucket, point, Precision::Millisecond)
            .await
            .context("Failed to write rtl_433 event to Influx.")?;
    }
}
