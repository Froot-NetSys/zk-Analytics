use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EpochType {
    CmEpoch,
    HistogramEpoch,
    SamplesEpoch,
}

impl EpochType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EpochType::CmEpoch => "cm_epoch",
            EpochType::HistogramEpoch => "histogram_epoch",
            EpochType::SamplesEpoch => "samples_epoch",
        }
    }

    pub fn is_samples_epoch(self) -> bool {
        matches!(self, EpochType::SamplesEpoch)
    }

    pub fn is_series_shard(self) -> bool {
        // No series shard types anymore - histogram and cm are now per-key by default
        false
    }

    pub fn is_cm_epoch(self) -> bool {
        matches!(self, EpochType::CmEpoch)
    }

    pub fn is_histogram_epoch(self) -> bool {
        matches!(self, EpochType::HistogramEpoch)
    }

    pub fn metric_index(self) -> usize {
        match self {
            EpochType::CmEpoch => 0,
            EpochType::HistogramEpoch => 1,
            EpochType::SamplesEpoch => 2,
        }
    }
}

impl fmt::Display for EpochType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for EpochType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cm_epoch" => Ok(EpochType::CmEpoch),
            "histogram_epoch" => Ok(EpochType::HistogramEpoch),
            "samples_epoch" => Ok(EpochType::SamplesEpoch),
            other => Err(anyhow::anyhow!("unsupported epoch_type {other}")),
        }
    }
}
