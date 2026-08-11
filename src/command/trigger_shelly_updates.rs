use crate::command::CommandContext;
use anyhow::{Context, Result};
use clap::Args;
use rumqttc::v5::mqttbytes::QoS;
use std::time::Duration;
use tokio::time::MissedTickBehavior;
use tracing::info;

#[derive(Args, Debug)]
pub struct TriggerShellyUpdates {
    #[arg(long, default_value = "60")]
    pub interval_s: u64,
}

pub async fn run(command: TriggerShellyUpdates, context: CommandContext) -> Result<()> {
    let duration = Duration::from_secs(command.interval_s);

    let mut interval = tokio::time::interval(duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    info!("Triggering updates every {}s...", command.interval_s);

    loop {
        interval.tick().await;

        context
            .mqtt_client
            .publish("shellies/command", QoS::AtMostOnce, false, "status_update")
            .await
            .context("Failed to publish command.")?;
    }
}
