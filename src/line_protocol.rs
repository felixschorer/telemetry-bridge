use itertools::Itertools;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Error, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    #[error("Infinity and NaN are not supported.")]
    UnsupportedFloat,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Point {
    measurement: String,
    tags: BTreeMap<String, String>,
    fields: BTreeMap<String, String>,
    timestamp: Option<i64>,
}

impl Point {
    pub fn new(measurement: &str) -> Self {
        Self {
            measurement: escape_measurement(measurement),
            tags: BTreeMap::new(),
            fields: BTreeMap::new(),
            timestamp: None,
        }
    }

    pub fn add_tag(&mut self, key: &str, value: &str) -> &mut Self {
        self.tags.insert(escape_key(key), escape_key(value));
        self
    }

    pub fn add_str_field(&mut self, key: &str, value: &str) -> &mut Self {
        self.fields.insert(escape_key(key), encode_str_value(value));
        self
    }

    pub fn add_int_field(&mut self, key: &str, value: i64) -> &mut Self {
        self.fields.insert(escape_key(key), format!("{value}i"));
        self
    }

    pub fn add_float_field(&mut self, key: &str, value: f64) -> Result<&mut Self, ProtocolError> {
        if !value.is_finite() {
            return Err(ProtocolError::UnsupportedFloat);
        }
        self.fields.insert(escape_key(key), value.to_string());
        Ok(self)
    }

    pub fn add_bool_field(&mut self, key: &str, value: bool) -> &mut Self {
        self.fields.insert(escape_key(key), value.to_string());
        self
    }

    pub fn set_timestamp(&mut self, timestamp: i64) -> &mut Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn to_line(&self) -> Option<String> {
        let mut line = String::with_capacity(128);

        line.push_str(&self.measurement);

        for (key, value) in &self.tags {
            line.push(',');
            line.push_str(key);
            line.push('=');
            line.push_str(value);
        }

        line.push(' ');

        let mut fields = self.fields.iter();

        let Some((key, value)) = fields.next() else {
            return None;
        };

        line.push_str(key);
        line.push('=');
        line.push_str(value);

        for (key, value) in fields {
            line.push(',');
            line.push_str(key);
            line.push('=');
            line.push_str(value);
        }

        if let Some(timestamp) = self.timestamp {
            line.push(' ');
            line.push_str(&timestamp.to_string());
        }

        Some(line)
    }
}

pub fn to_line_protocol(points: impl IntoIterator<Item = Point>) -> String {
    points.into_iter().filter_map(|p| p.to_line()).join("\n")
}

fn escape_measurement(key: &str) -> String {
    let mut escaped = String::with_capacity(key.len());
    for char in key.chars() {
        match char {
            ',' => escaped.push_str("\\,"),
            ' ' => escaped.push_str("\\ "),
            // New line signals the start of the next measurement.
            // InfluxDB v2 parser does not handle the escape sequence and
            // will just include the literal escape sequence in the measurement.
            '\n' => escaped.push_str("\\n"),
            // For consistency: include escape sequence of common non-printable characters in the measurement.
            '\t' => escaped.push_str("\\t"),
            '\r' => escaped.push_str("\\r"),
            // InfluxDB v2 parser handles backslashes improperly. They cannot be escaped!
            // Unescaped backslashes work unless they are followed by a character which needs escaping,
            // or are the last character in the measurement.
            // Trying to escape a backslash will just include two backslashes in the measurement
            // and all the same gotchas of a single backslash apply.
            // Therefore, we drop them.
            '\\' => (),
            _ => escaped.push(char),
        }
    }
    escaped
}

fn escape_key(key: &str) -> String {
    let mut escaped = String::with_capacity(key.len());
    for char in key.chars() {
        match char {
            ',' => escaped.push_str("\\,"),
            ' ' => escaped.push_str("\\ "),
            '=' => escaped.push_str("\\="),
            // New line signals the start of the next measurement.
            // InfluxDB v2 parser does not handle the escape sequence and
            // will just include the literal escape sequence in the measurement.
            '\n' => escaped.push_str("\\n"),
            // For consistency: include escape sequence of common non-printable characters in the measurement.
            '\t' => escaped.push_str("\\t"),
            '\r' => escaped.push_str("\\r"),
            // InfluxDB v2 parser handles backslashes improperly. They cannot be escaped!
            // Unescaped backslashes work unless they are followed by a character which needs escaping,
            // or are the last character in the measurement.
            // Trying to escape a backslash will just include two backslashes in the measurement
            // and all the same gotchas of a single backslash apply.
            // Therefore, we drop them.
            '\\' => (),
            _ => escaped.push(char),
        }
    }
    escaped
}

fn encode_str_value(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');

    for char in value.chars() {
        match char {
            '\\' => encoded.push_str("\\\\"),
            '"' => encoded.push_str("\\\""),
            _ => encoded.push(char),
        }
    }

    encoded.push('"');
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;

    #[test]
    fn test_to_line() {
        let point = Point::new("devices");
        assert_eq!(point.to_line(), None);

        let mut point = Point::new("devices");

        point.add_tag("type", "foo");

        point.add_str_field("unit", "V");
        point.add_int_field("phases", 3);
        point.add_float_field("value", 230.1).unwrap();
        point.add_bool_field("error", false);

        point.set_timestamp(1_234_567_890);

        assert_eq!(
            point.to_line().unwrap(),
            "devices,type=foo error=false,phases=3i,unit=\"V\",value=230.1 1234567890"
        );
    }

    #[test]
    fn test_add_float_field() {
        let mut point = Point::new("devices");
        assert_matches!(point.add_float_field("value", 230.1), Ok(_));
        assert_eq!(
            point.add_float_field("value", f64::INFINITY),
            Err(ProtocolError::UnsupportedFloat)
        );
        assert_eq!(
            point.add_float_field("value", f64::NAN),
            Err(ProtocolError::UnsupportedFloat)
        );
    }
}
