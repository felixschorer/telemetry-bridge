use crate::message::TimestampedMessage;
use crate::topic_pattern::TopicPattern;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::SubscribeProperties;
use rumqttc::v5::{AsyncClient, ClientError};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tracing::warn;

#[derive(Clone, Debug)]
pub struct MessageRouterBuilder {
    buffer_size: usize,
    senders: BTreeMap<String, Sender<TimestampedMessage>>,
}

impl MessageRouterBuilder {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            senders: BTreeMap::new(),
        }
    }

    pub fn add_subscriber(
        &mut self,
        topic_pattern: &TopicPattern,
    ) -> impl Stream<Item = TimestampedMessage> + 'static {
        let topic_filter = topic_pattern.to_topic_filter();

        let rx = match self.senders.entry(topic_filter.to_owned()) {
            Entry::Occupied(entry) => entry.get().subscribe(),
            Entry::Vacant(entry) => {
                let (tx, rx) = broadcast::channel(self.buffer_size);
                entry.insert(tx);
                rx
            }
        };

        BroadcastStream::new(rx).filter_map(move |res| match res {
            Ok(message) => Some(message),
            Err(BroadcastStreamRecvError::Lagged(count)) => {
                warn!("Dropping {count} lagged messages on topic '{topic_filter}'.");
                None
            }
        })
    }

    pub fn build(self) -> MessageRouter {
        MessageRouter {
            subscriptions: self.senders.into_iter().collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MessageRouter {
    subscriptions: Vec<(String, Sender<TimestampedMessage>)>,
}

impl MessageRouter {
    pub async fn subscribe(&self, mqtt_client: &AsyncClient) -> Result<(), ClientError> {
        for (index, (topic_filter, _)) in self.subscriptions.iter().enumerate() {
            let properties = SubscribeProperties {
                id: Some(index + 1),
                user_properties: Vec::new(),
            };

            mqtt_client
                .subscribe_with_properties(topic_filter, QoS::AtMostOnce, properties)
                .await?;
        }
        Ok(())
    }

    pub fn handle_message(&self, message: TimestampedMessage) {
        let properties = &message.publish.properties;

        let subscriptions = properties
            .iter()
            .flat_map(|p| p.subscription_identifiers.iter())
            .filter_map(|id| id.checked_sub(1))
            .filter_map(|idx| self.subscriptions.get(idx));

        for (topic_filter, sender) in subscriptions {
            if sender.send(message.clone()).is_err() {
                warn!("Dropping message on topic '{topic_filter}'. All receivers are closed.")
            };
        }
    }
}
