use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{Score, ScoreDataType};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreConfig {
    pub config_id: String,
    pub timestamp: DateTime<Utc>,
    pub name: String,
    pub data_type: ScoreDataType,
    pub description: Option<String>,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    #[serde(default)]
    pub categories: Vec<String>,
    pub author_id: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    pub record_date: NaiveDate,
}

impl ScoreConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.config_id.trim().is_empty() {
            return Err("config_id cannot be empty");
        }
        if self.name.trim().is_empty() {
            return Err("name cannot be empty");
        }
        if matches!(self.data_type, ScoreDataType::Categorical) && self.categories.is_empty() {
            return Err("categorical configs require at least one category");
        }
        if let (Some(min), Some(max)) = (self.min_value, self.max_value) {
            if min > max {
                return Err("min_value must be <= max_value");
            }
        }
        Ok(())
    }

    /// Hard-validate a score against this config when `config_id` is set on the score.
    pub fn validate_score(&self, score: &Score) -> Result<(), &'static str> {
        if score.name != self.name {
            return Err("score name does not match config");
        }
        if score.data_type != self.data_type {
            return Err("score data_type does not match config");
        }
        if let Some(value) = score.numeric_value {
            if let Some(min) = self.min_value {
                if value < min {
                    return Err("numeric score below config min_value");
                }
            }
            if let Some(max) = self.max_value {
                if value > max {
                    return Err("numeric score above config max_value");
                }
            }
        }
        if matches!(self.data_type, ScoreDataType::Categorical) {
            let Some(value) = score.string_value.as_deref() else {
                return Err("categorical score requires string_value");
            };
            if !self.categories.iter().any(|c| c == value) {
                return Err("categorical score value not in config categories");
            }
        }
        Ok(())
    }

    pub fn seed_defaults(now: DateTime<Utc>) -> Vec<Self> {
        let record_date = now.date_naive();
        vec![
            Self {
                config_id: "cfg-correctness".to_string(),
                timestamp: now,
                name: "correctness".to_string(),
                data_type: ScoreDataType::Boolean,
                description: Some("Whether the turn was correct".to_string()),
                min_value: None,
                max_value: None,
                categories: vec![],
                author_id: Some("system".to_string()),
                metadata: HashMap::from([("seed".to_string(), "default".to_string())]),
                record_date,
            },
            Self {
                config_id: "cfg-quality".to_string(),
                timestamp: now,
                name: "quality".to_string(),
                data_type: ScoreDataType::Categorical,
                description: Some("Coarse quality label".to_string()),
                min_value: None,
                max_value: None,
                categories: vec!["good".to_string(), "ok".to_string(), "bad".to_string()],
                author_id: Some("system".to_string()),
                metadata: HashMap::from([("seed".to_string(), "default".to_string())]),
                record_date,
            },
            Self {
                config_id: "cfg-expected-output".to_string(),
                timestamp: now,
                name: "expected_output".to_string(),
                data_type: ScoreDataType::Text,
                description: Some("Corrected assistant text for eval gold".to_string()),
                min_value: None,
                max_value: None,
                categories: vec![],
                author_id: Some("system".to_string()),
                metadata: HashMap::from([("seed".to_string(), "default".to_string())]),
                record_date,
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ScoreSource;

    fn boolean_config() -> ScoreConfig {
        let timestamp = Utc::now();
        ScoreConfig {
            config_id: "cfg-1".to_string(),
            timestamp,
            name: "correctness".to_string(),
            data_type: ScoreDataType::Boolean,
            description: None,
            min_value: None,
            max_value: None,
            categories: vec![],
            author_id: None,
            metadata: HashMap::new(),
            record_date: timestamp.date_naive(),
        }
    }

    #[test]
    fn rejects_empty_categorical_categories() {
        let mut config = boolean_config();
        config.data_type = ScoreDataType::Categorical;
        assert_eq!(
            config.validate(),
            Err("categorical configs require at least one category")
        );
    }

    #[test]
    fn validates_score_name_and_type() {
        let config = boolean_config();
        let timestamp = Utc::now();
        let score = Score {
            score_id: "s1".to_string(),
            timestamp,
            trace_id: Some("t".to_string()),
            span_id: None,
            session_id: None,
            name: "correctness".to_string(),
            data_type: ScoreDataType::Boolean,
            numeric_value: None,
            string_value: None,
            boolean_value: Some(true),
            source: ScoreSource::Annotation,
            comment: None,
            config_id: Some("cfg-1".to_string()),
            author_id: None,
            metadata: HashMap::new(),
            record_date: timestamp.date_naive(),
        };
        assert_eq!(config.validate_score(&score), Ok(()));
    }
}
