use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter, Write};
use std::str::FromStr;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
enum TopicLevel {
    Empty,
    Static(String),
    Wildcard,
    NamedWildcard(String),
    MultiLevelWildcard,
}

enum WriteMode {
    PreserveNames,
    StripNames,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct TopicPattern {
    share_name: Option<String>,
    levels: Vec<TopicLevel>,
}

impl TopicPattern {
    pub fn has_named_wildcard(&self, name: &str) -> bool {
        self.levels
            .iter()
            .any(|level| matches!(level, TopicLevel::NamedWildcard(n) if n == name))
    }

    pub fn extract_value<'a>(&self, topic: &'a str, name: &str) -> Option<&'a str> {
        let mut levels = self.levels.iter();
        let mut segments = topic.split('/');

        let mut value = None;

        loop {
            match (levels.next(), segments.next()) {
                (Some(TopicLevel::Empty), Some("")) => (),
                (Some(TopicLevel::Static(s)), Some(segment)) if s == segment => (),
                (Some(TopicLevel::Wildcard), Some(_)) => (),
                (Some(TopicLevel::NamedWildcard(n)), Some(v)) => {
                    if n == name {
                        value = Some(v)
                    }
                }
                (Some(TopicLevel::MultiLevelWildcard), _) => return value,
                (None, None) => return value,
                _ => return None,
            }
        }
    }

    pub fn to_topic_filter(&self) -> String {
        let mut topic = String::with_capacity(64);
        let _ = self.write(&mut topic, WriteMode::StripNames);
        topic
    }

    fn write(&self, writer: &mut impl Write, mode: WriteMode) -> std::fmt::Result {
        if let Some(share_name) = &self.share_name {
            writer.write_str("$share/")?;
            writer.write_str(share_name)?;
            writer.write_char('/')?;
        }

        let mut first = true;

        for level in &self.levels {
            if !first {
                writer.write_char('/')?;
            }

            match level {
                TopicLevel::Empty => {}
                TopicLevel::Static(s) => writer.write_str(s)?,
                TopicLevel::Wildcard => writer.write_char('+')?,
                TopicLevel::NamedWildcard(_) if matches!(mode, WriteMode::StripNames) => {
                    writer.write_char('+')?
                }
                TopicLevel::NamedWildcard(name) => {
                    writer.write_char('+')?;
                    writer.write_str(name)?;
                }
                TopicLevel::MultiLevelWildcard => writer.write_char('#')?,
            }

            first = false;
        }

        Ok(())
    }
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum ParseError {
    #[error("Expected a share name to follow '$share/'.")]
    MissingShareName,
    #[error("The share name must have at least one char.")]
    EmptyShareName,
    #[error("The share name '{0}' cannot contain '+', or '#'.")]
    InvalidShareName(String),
    #[error("The topic cannot be empty.")]
    EmptyTopic,
    #[error("Cannot append another level after a multi-level wildcard.")]
    EndOfPattern,
    #[error("Wildcard name '{0}' cannot contain '+', or '#'.")]
    InvalidWildcardName(String),
    #[error("Duplicate wildcard '{0}'.")]
    DuplicateWildcard(String),
    #[error("Non-wildcard level '{0}' cannot contain '+', or '#'.")]
    InvalidTopicLevel(String),
}

fn contains_wildcard_char(level: &str) -> bool {
    level.contains(['+', '#'])
}

impl FromStr for TopicPattern {
    type Err = ParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut pattern = Self {
            share_name: None,
            levels: Vec::with_capacity(8),
        };

        let mut segments = input.split('/');

        loop {
            match segments.next() {
                Some("$share") if pattern.levels.is_empty() && pattern.share_name.is_none() => {
                    let Some(share_name) = segments.next() else {
                        return Err(ParseError::MissingShareName);
                    };

                    if share_name.is_empty() {
                        return Err(ParseError::EmptyShareName);
                    }

                    if contains_wildcard_char(share_name) {
                        return Err(ParseError::InvalidShareName(share_name.to_owned()));
                    }

                    pattern.share_name = Some(share_name.to_owned())
                }
                Some("") => pattern.levels.push(TopicLevel::Empty),
                Some("+") => pattern.levels.push(TopicLevel::Wildcard),
                Some("#") => {
                    if segments.next().is_some() {
                        return Err(ParseError::EndOfPattern);
                    }

                    pattern.levels.push(TopicLevel::MultiLevelWildcard);
                }
                Some(segment) if segment.starts_with('+') => {
                    let wildcard_name = &segment[1..];

                    if contains_wildcard_char(wildcard_name) {
                        return Err(ParseError::InvalidWildcardName(wildcard_name.to_owned()));
                    }

                    if pattern.has_named_wildcard(wildcard_name) {
                        return Err(ParseError::DuplicateWildcard(wildcard_name.to_owned()));
                    }

                    let level = TopicLevel::NamedWildcard(wildcard_name.to_owned());
                    pattern.levels.push(level)
                }
                Some(segment) => {
                    if contains_wildcard_char(segment) {
                        return Err(ParseError::InvalidTopicLevel(segment.to_owned()));
                    }

                    let level = TopicLevel::Static(segment.to_owned());
                    pattern.levels.push(level)
                }
                None => break,
            }
        }

        if matches!(pattern.levels[..], [] | [TopicLevel::Empty]) {
            return Err(ParseError::EmptyTopic);
        }

        Ok(pattern)
    }
}

impl TryFrom<String> for TopicPattern {
    type Error = ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<TopicPattern> for String {
    fn from(value: TopicPattern) -> Self {
        value.to_string()
    }
}

impl Display for TopicPattern {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        self.write(f, WriteMode::PreserveNames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        let res = TopicPattern::from_str("dev/+/+type/+id");
        assert!(res.is_ok());

        let res = TopicPattern::from_str("$share/name/dev/+/+type/+id/#");
        assert!(res.is_ok());

        let res = TopicPattern::from_str("dev/$share");
        assert!(res.is_ok());

        let res = TopicPattern::from_str("");
        assert_eq!(res, Err(ParseError::EmptyTopic));

        let res = TopicPattern::from_str("$share");
        assert_eq!(res, Err(ParseError::MissingShareName));

        let res = TopicPattern::from_str("$share/name");
        assert_eq!(res, Err(ParseError::EmptyTopic));

        let res = TopicPattern::from_str("$share//");
        assert_eq!(res, Err(ParseError::EmptyShareName));

        let res = TopicPattern::from_str("$share/+/");
        assert_eq!(res, Err(ParseError::InvalidShareName("+".to_owned())));

        let res = TopicPattern::from_str("#/");
        assert_eq!(res, Err(ParseError::EndOfPattern));

        let res = TopicPattern::from_str("++");
        assert_eq!(res, Err(ParseError::InvalidWildcardName("+".to_owned())));

        let res = TopicPattern::from_str("+id/+id");
        assert_eq!(res, Err(ParseError::DuplicateWildcard("id".to_owned())));

        let res = TopicPattern::from_str("dev#");
        assert_eq!(res, Err(ParseError::InvalidTopicLevel("dev#".to_owned())));

        let pattern = TopicPattern::from_str("$share/name/$share/other").unwrap();
        assert_eq!(pattern.to_topic_filter(), "$share/name/$share/other");
    }

    #[test]
    fn test_extract_value() {
        let pattern = TopicPattern::from_str("dev/+type/+id").unwrap();
        assert_eq!(pattern.extract_value("dev/foo/42", "type"), Some("foo"));
        assert_eq!(pattern.extract_value("dev/foo/42", "id"), Some("42"));
        assert_eq!(pattern.extract_value("dev/foo/42", "unknown"), None);
        assert_eq!(pattern.extract_value("dev/foo/42/unexpected", "type"), None);
        assert_eq!(pattern.extract_value("dev/foo", "type"), None);
        assert_eq!(pattern.extract_value("unexpected/foo/42", "type"), None);

        let pattern = TopicPattern::from_str("dev/+type/+").unwrap();
        assert_eq!(pattern.extract_value("dev/foo/42", "type"), Some("foo"));
        assert_eq!(pattern.extract_value("dev/foo", "type"), None);

        let pattern = TopicPattern::from_str("dev/+type/#").unwrap();
        assert_eq!(pattern.extract_value("dev/foo", "type"), Some("foo"));
        assert_eq!(pattern.extract_value("dev/foo/42", "type"), Some("foo"));
        assert_eq!(pattern.extract_value("dev/foo/42/bar", "type"), Some("foo"));

        let pattern = TopicPattern::from_str("dev/+type/+id/").unwrap();
        assert_eq!(pattern.extract_value("dev/foo/42/", "type"), Some("foo"));
        assert_eq!(pattern.extract_value("dev/foo/42", "type"), None);
    }

    #[test]
    fn test_to_topic_filter() {
        let pattern = TopicPattern::from_str("dev/+/+type/+id").unwrap();
        assert_eq!(pattern.to_topic_filter(), "dev/+/+/+");

        let pattern = TopicPattern::from_str("$share/name/dev/+/+type/+id/#").unwrap();
        assert_eq!(pattern.to_topic_filter(), "$share/name/dev/+/+/+/#");
    }

    #[test]
    fn test_to_string() {
        let pattern = TopicPattern::from_str("dev/+/+type/+id").unwrap();
        assert_eq!(pattern.to_string(), "dev/+/+type/+id");

        let pattern = TopicPattern::from_str("$share/name/dev/+/+type/+id/#").unwrap();
        assert_eq!(pattern.to_string(), "$share/name/dev/+/+type/+id/#");
    }
}
