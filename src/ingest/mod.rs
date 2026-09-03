use serde::{Deserialize, Serialize};

pub mod bresser_station;
pub mod rtl433_events;
pub mod shelly_switch;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Settings {
    pub shelly_switch: Option<shelly_switch::Settings>,
    pub bresser_station: Option<bresser_station::Settings>,
    pub rtl433_events: Option<rtl433_events::Settings>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            shelly_switch: Some(shelly_switch::Settings::default()),
            bresser_station: Some(bresser_station::Settings::default()),
            rtl433_events: Some(rtl433_events::Settings::default()),
        }
    }
}
