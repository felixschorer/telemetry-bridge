use crate::message::Message;
use anyhow::Result;
use clap::Subcommand;
use rumqttc::v5::AsyncClient;
use tokio::sync::mpsc::Receiver;

mod ingest_bresser_station;
mod ingest_rtl433_events;
mod ingest_shelly_switch;
mod trigger_shelly_updates;

#[derive(Subcommand, Debug)]
pub enum Command {
    TriggerShellyUpdates(trigger_shelly_updates::TriggerShellyUpdates),
    IngestShellySwitch(ingest_shelly_switch::IngestShellySwitch),
    IngestBresserStation(ingest_bresser_station::IngestBresserStation),
    IngestRtl433Events(ingest_rtl433_events::IngestRtl433Events),
}

pub struct CommandContext {
    mqtt_client: AsyncClient,
    message_rx: Receiver<Message>,
}

impl CommandContext {
    pub fn new(mqtt_client: AsyncClient, message_rx: Receiver<Message>) -> Self {
        Self {
            mqtt_client,
            message_rx,
        }
    }
}

pub async fn run(command: Command, context: CommandContext) -> Result<()> {
    match command {
        Command::TriggerShellyUpdates(cmd) => trigger_shelly_updates::run(cmd, context).await,
        Command::IngestShellySwitch(cmd) => ingest_shelly_switch::run(cmd, context).await,
        Command::IngestBresserStation(cmd) => ingest_bresser_station::run(cmd, context).await,
        Command::IngestRtl433Events(cmd) => ingest_rtl433_events::run(cmd, context).await,
    }
}
