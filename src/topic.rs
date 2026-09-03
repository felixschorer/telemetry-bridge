use crate::topic::ParseError::{EmptyTopic, Wildcard};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct Topic(String);

#[derive(Error, Debug, Eq, PartialEq)]
pub enum ParseError {
    #[error("The topic cannot be empty.")]
    EmptyTopic,
    #[error("The topic cannot contain '+', or '#'.")]
    Wildcard,
}

impl FromStr for Topic {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_owned().try_into()
    }
}

impl TryFrom<String> for Topic {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(EmptyTopic);
        }
        if value.contains(['+', '#']) {
            return Err(Wildcard);
        }
        Ok(Self(value))
    }
}

impl From<Topic> for String {
    fn from(value: Topic) -> Self {
        value.0
    }
}

impl Display for Topic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
