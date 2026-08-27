use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use smallvec::SmallVec;
use std::fmt::{Display, Formatter, Write};
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug, Eq, PartialEq)]
pub enum ParseError {
    #[error("A topic pattern must have at least one char.")]
    EmptyPattern,
    #[error("Expected a share name to follow '$share/'.")]
    MissingShareName,
    #[error("Expected a topic filter to follow the share name.")]
    MissingTopicFilter,
    #[error("The share name must have at least one char.")]
    EmptyShareName,
    #[error("The share name '{0}' cannot contain '/', '+', or '#'.")]
    InvalidShareName(String),
    #[error("Cannot append another level after a multi-level wildcard.")]
    EndOfPattern,
    #[error("Wildcard name '{0}' cannot contain '/', '+', or '#'.")]
    InvalidWildcardName(String),
    #[error("Duplicate wildcard '{0}'.")]
    DuplicateWildcardName(String),
    #[error("Non-wildcard level '{0}' cannot contain '/', '+', or '#'.")]
    InvalidTopicLevel(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WildcardValue<'name, 'val> {
    name: Option<&'name str>,
    value: &'val str,
}

impl<'name, 'val> WildcardValue<'name, 'val> {
    fn new(value: &'val str) -> Self {
        Self { name: None, value }
    }

    fn named(name: &'name str, value: &'val str) -> Self {
        Self {
            name: Some(name),
            value,
        }
    }

    fn has_name(&self, name: &str) -> bool {
        self.name == Some(name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMatch<'n, 'v> {
    values: SmallVec<[WildcardValue<'n, 'v>; 8]>,
}

impl<'name, 'val> TopicMatch<'name, 'val> {
    fn new() -> Self {
        Self {
            values: SmallVec::new(),
        }
    }

    fn push(&mut self, item: WildcardValue<'name, 'val>) {
        self.values.push(item);
    }

    pub fn get(&self, index: usize) -> Option<&'val str> {
        let item = self.values.get(index)?;
        Some(item.value)
    }

    pub fn get_named(&self, name: &str) -> Option<&'val str> {
        let item = self.values.iter().find(|i| i.has_name(name))?;
        Some(item.value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TopicLevel {
    Static(String),
    Wildcard,
    NamedWildcard(String),
}

impl TopicLevel {
    fn is_named_wildcard(&self, name: &str) -> bool {
        matches!(self, TopicLevel::NamedWildcard(n) if n == name)
    }

    fn to_subscription_topic(&self) -> &str {
        match self {
            TopicLevel::Static(s) => s,
            TopicLevel::Wildcard | TopicLevel::NamedWildcard(_) => "+",
        }
    }
}

impl Display for TopicLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TopicLevel::Static(s) => f.write_str(s),
            TopicLevel::Wildcard => f.write_char('+'),
            TopicLevel::NamedWildcard(name) => write!(f, "+{name}"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicPattern {
    share: Option<String>,
    levels: Vec<TopicLevel>,
    ends_with_wildcard: bool,
}

impl TopicPattern {
    pub fn new() -> Self {
        Self {
            share: None,
            levels: Vec::new(),
            ends_with_wildcard: false,
        }
    }

    pub fn parse(input: &str) -> Result<Self, ParseError> {
        if input.is_empty() {
            return Err(ParseError::EmptyPattern);
        }

        let mut pattern = Self::new();

        let mut levels = input.split('/').peekable();

        if levels.next_if_eq(&"$share").is_some() {
            let Some(share_name) = levels.next() else {
                return Err(ParseError::MissingShareName);
            };

            pattern.set_share(share_name)?;

            if levels.peek().is_none() {
                return Err(ParseError::MissingTopicFilter);
            }
        }

        for level in levels {
            pattern.append_level(level)?;
        }

        Ok(pattern)
    }

    pub fn set_share(&mut self, share_name: &str) -> Result<&mut Self, ParseError> {
        if share_name.is_empty() {
            return Err(ParseError::EmptyShareName);
        }

        if contains_separator_or_wildcard_char(share_name) {
            return Err(ParseError::InvalidShareName(share_name.to_owned()));
        }

        self.share = Some(share_name.to_owned());
        Ok(self)
    }

    pub fn append_level(&mut self, level: &str) -> Result<&mut Self, ParseError> {
        if self.ends_with_wildcard {
            return Err(ParseError::EndOfPattern);
        }

        match level.chars().next() {
            Some('#') if level.len() == 1 => {
                self.ends_with_wildcard = true;
            }
            Some('+') if level.len() == 1 => {
                self.levels.push(TopicLevel::Wildcard);
            }
            Some('+') => {
                let name = &level[1..];

                if contains_separator_or_wildcard_char(name) {
                    return Err(ParseError::InvalidWildcardName(name.to_owned()));
                }

                if self.has_named_wildcard(name) {
                    return Err(ParseError::DuplicateWildcardName(name.to_owned()));
                }

                self.levels.push(TopicLevel::NamedWildcard(name.to_owned()));
            }
            _ => {
                if contains_separator_or_wildcard_char(level) {
                    return Err(ParseError::InvalidTopicLevel(level.to_owned()));
                }

                self.levels.push(TopicLevel::Static(level.to_owned()));
            }
        }

        Ok(self)
    }

    pub fn has_named_wildcard(&self, name: &str) -> bool {
        self.levels.iter().any(|l| l.is_named_wildcard(name))
    }

    pub fn to_subscription_topic(&self) -> String {
        join_pattern(
            self.share.as_deref(),
            self.levels.iter().map(TopicLevel::to_subscription_topic),
            self.ends_with_wildcard,
        )
    }

    pub fn match_topic<'a, 'topic>(&'a self, topic: &'topic str) -> Option<TopicMatch<'a, 'topic>> {
        let mut topic_match = TopicMatch::new();

        let mut topic_levels = topic.split('/');
        let mut pattern_levels = self.levels.iter();

        loop {
            match (pattern_levels.next(), topic_levels.next()) {
                (Some(TopicLevel::Static(s)), Some(value)) if s == value => {}
                (Some(TopicLevel::Wildcard), Some(value)) => {
                    topic_match.push(WildcardValue::new(value))
                }
                (Some(TopicLevel::NamedWildcard(name)), Some(value)) => {
                    topic_match.push(WildcardValue::named(name, value))
                }
                (None, Some(_)) if self.ends_with_wildcard => return Some(topic_match),
                (None, None) => return Some(topic_match),
                _ => return None,
            }
        }
    }
}

impl Default for TopicPattern {
    fn default() -> Self {
        TopicPattern::new()
    }
}

impl FromStr for TopicPattern {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TopicPattern::parse(s)
    }
}

impl Display for TopicPattern {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let pattern = join_pattern(
            self.share.as_deref(),
            self.levels.iter().map(TopicLevel::to_string),
            self.ends_with_wildcard,
        );
        f.write_str(&pattern)
    }
}

impl Serialize for TopicPattern {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TopicPattern {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TopicPatternVisitor;

        impl<'de> de::Visitor<'de> for TopicPatternVisitor {
            type Value = TopicPattern;

            fn expecting(&self, f: &mut Formatter) -> std::fmt::Result {
                f.write_str("a valid MQTT topic pattern string")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                TopicPattern::parse(value).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_str(TopicPatternVisitor)
    }
}

fn contains_separator_or_wildcard_char(level: &str) -> bool {
    level.contains(['/', '+', '#'])
}

fn join_pattern<S: AsRef<str>>(
    share: Option<&str>,
    levels: impl IntoIterator<Item = S>,
    ends_with_wildcard: bool,
) -> String {
    let mut pattern = String::with_capacity(64);

    if let Some(share_name) = share {
        pattern.push_str("$share/");
        pattern.push_str(share_name);
        pattern.push('/');
    }

    let mut levels = levels.into_iter().peekable();

    loop {
        let Some(level) = levels.next() else { break };

        pattern.push_str(level.as_ref());

        let is_last = levels.peek().is_none();
        if !is_last || ends_with_wildcard {
            pattern.push('/');
        }
    }

    if ends_with_wildcard {
        pattern.push('#');
    }

    pattern
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn test_parse() {
        let res = TopicPattern::parse("dev/+/+type/+id");
        assert_matches!(res, Ok(_));

        let res = TopicPattern::parse("$share/name/dev/+/+type/+id/#");
        assert_matches!(res, Ok(_));

        let res = TopicPattern::parse("");
        assert_eq!(res, Err(ParseError::EmptyPattern));

        let res = TopicPattern::parse("$share");
        assert_eq!(res, Err(ParseError::MissingShareName));

        let res = TopicPattern::parse("$share/name");
        assert_eq!(res, Err(ParseError::MissingTopicFilter));

        let res = TopicPattern::parse("$share//");
        assert_eq!(res, Err(ParseError::EmptyShareName));

        let res = TopicPattern::parse("$share/+/");
        assert_eq!(res, Err(ParseError::InvalidShareName("+".to_owned())));

        let res = TopicPattern::parse("#/");
        assert_eq!(res, Err(ParseError::EndOfPattern));

        let res = TopicPattern::parse("++");
        assert_eq!(res, Err(ParseError::InvalidWildcardName("+".to_owned())));

        let res = TopicPattern::parse("+id/+id");
        assert_eq!(res, Err(ParseError::DuplicateWildcardName("id".to_owned())));

        let res = TopicPattern::parse("dev#");
        assert_eq!(res, Err(ParseError::InvalidTopicLevel("dev#".to_owned())));
    }

    #[test]
    fn test_to_subscription_topic() {
        let pattern = TopicPattern::parse("dev/+/+type/+id").unwrap();
        assert_eq!(pattern.to_subscription_topic(), "dev/+/+/+");

        let pattern = TopicPattern::parse("$share/name/dev/+/+type/+id/#").unwrap();
        assert_eq!(pattern.to_subscription_topic(), "$share/name/dev/+/+/+/#");
    }

    #[test]
    fn test_to_string() {
        let pattern = TopicPattern::parse("dev/+/+type/+id").unwrap();
        assert_eq!(pattern.to_string(), "dev/+/+type/+id");

        let pattern = TopicPattern::parse("$share/name/dev/+/+type/+id/#").unwrap();
        assert_eq!(pattern.to_string(), "$share/name/dev/+/+type/+id/#");
    }

    #[test]
    fn test_match_topic() {
        let pattern = TopicPattern::parse("dev").unwrap();
        assert_matches!(pattern.match_topic("dev"), Some(_));

        let pattern = TopicPattern::parse("dev").unwrap();
        assert_matches!(pattern.match_topic("dev1"), None);

        let pattern = TopicPattern::parse("dev/+").unwrap();
        assert_matches!(pattern.match_topic("dev"), None);

        let pattern = TopicPattern::parse("dev").unwrap();
        assert_matches!(pattern.match_topic("dev/foo"), None);

        let pattern = TopicPattern::parse("+").unwrap();
        assert_matches!(pattern.match_topic("foo"), Some(_));

        let pattern = TopicPattern::parse("+type").unwrap();
        assert_matches!(pattern.match_topic("bar"), Some(_));

        let pattern = TopicPattern::parse("#").unwrap();
        assert_matches!(pattern.match_topic("dev/foo"), Some(_));
    }

    #[test]
    fn test_matched_values() {
        let pattern = TopicPattern::parse("dev/+/+type/+id").unwrap();

        let m = pattern.match_topic("dev/foo/bar/42").unwrap();
        assert_eq!(m.get(0), Some("foo"));
        assert_eq!(m.get(1), Some("bar"));
        assert_eq!(m.get(2), Some("42"));
        assert_eq!(m.get_named("type"), Some("bar"));
        assert_eq!(m.get_named("id"), Some("42"));
    }
}
