use std::fmt::Debug;

use abomonation_derive::Abomonation;
use serde::{Deserialize, Serialize};

use crate::{dataflow::time::Timestamp, communication::MessageMetadata};

/// Trait for valid message data. The data must be clonable, sendable between threads and
/// serializable.
// TODO: somehow add the deserialize requirement.
pub trait Data: 'static + Clone + Send + Sync + Debug + Serialize {}
/// Any type that is clonable, sendable, and can be serialized and dereserialized implements `Data`.
impl<T> Data for T where
    for<'a> T: 'static + Clone + Send + Sync + Debug + Serialize + Deserialize<'a>
{
}

/*
#[derive(Clone, Debug, Serialize, Deserialize, Abomonation)]
pub struct ExtendInfo{
    pub stage: Stage,
    pub expected_deadline: u16,
    pub timestamp_0: u128,//the time this msg is sent from source
    pub timestamp_1: u128,//the time this msg is received in other node
    pub timestamp_2: u128,//the time this msg is sent from the other node
    pub timestamp_3: u128,//the time this msg is received in this node
}

impl ExtendInfo {
    pub fn new(stage: Stage, expected_deadline:u16, timestamp_0: u128, timestamp_1: u128, timestamp_2:u128, timestamp_3:u128 ) -> Self {
        Self { stage, expected_deadline, timestamp_0, timestamp_1, timestamp_2, timestamp_3 }
    }
}*/

/// Operators send messages on streams. A message can be either a `Watermark` or a `TimestampedData`.
#[derive(Clone, Debug, Serialize, Deserialize, Abomonation)]
pub enum Message<D: Data> {
    TimestampedData(TimestampedData<D>),
    Watermark(Timestamp),
    ExtendTimestampedData(ExtendTimestampedData<D>),
}

impl<D: Data> Message<D> {
    /// Creates a new `TimestampedData` message.
    pub fn new_message(timestamp: Timestamp, data: D) -> Message<D> {
        Self::TimestampedData(TimestampedData::new(timestamp, data))
    }

    /// Creates a new `Watermark` message.
    pub fn new_watermark(timestamp: Timestamp) -> Message<D> {
        Self::Watermark(timestamp)
    }

    /// Creates a new `ExtendTimestampedData` message"
    pub fn new_extendmessage(timestamp: Timestamp, metadata: MessageMetadata, data: D) -> Message<D>{
        Self::ExtendTimestampedData(ExtendTimestampedData::new(timestamp, metadata, data))
    }

    pub fn is_top_watermark(&self) -> bool {
        if let Self::Watermark(t) = self {
            t.is_top()
        } else {
            false
        }
    }

    pub fn data(&self) -> Option<&D> {
        match self {
            Self::TimestampedData(d) => Some(&d.data),
            Self::ExtendTimestampedData(d) => Some(&d.data),
            _ => None,
        }
    }
    
    pub fn metadata(&self) -> Option<&MessageMetadata> {
        match self {
            Self::ExtendTimestampedData(d) => Some(&d.metadata),
            _ => None,
        }
    }

    pub fn timestamp(&self) -> &Timestamp {
        match self {
            Self::TimestampedData(d) => &d.timestamp,
            Self::Watermark(t) => t,
            Self::ExtendTimestampedData(d) => &d.timestamp,
        }
    }
}

impl<D: Data + PartialEq> PartialEq for Message<D> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::TimestampedData(d1), Self::TimestampedData(d2)) => d1 == d2,
            (Self::Watermark(w1), Self::Watermark(w2)) => w1 == w2,
            _ => false,
        }
    }
}

/// Data message which operators send along streams.
#[derive(Debug, Clone, Serialize, Deserialize, Abomonation)]
pub struct TimestampedData<D: Data> {
    /// Timestamp of the message.
    pub timestamp: Timestamp,
    /// Data is an option in case one wants to send null messages.
    pub data: D,
}

impl<D: Data> TimestampedData<D> {
    pub fn new(timestamp: Timestamp, data: D) -> Self {
        Self { timestamp, data }
    }
}

impl<D: Data + PartialEq> PartialEq for TimestampedData<D> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.data == other.data
    }
}

/// Data message which operators send along streams with more communication info.
#[derive(Debug, Clone, Serialize, Deserialize, Abomonation)]
pub struct ExtendTimestampedData<D: Data> {
    /// Timestamp of the message.
    pub timestamp: Timestamp,
    pub metadata: MessageMetadata,
    /// Data is an option in case one wants to send null messages.
    pub data: D,
}

impl<D: Data> ExtendTimestampedData<D> {
    pub fn new(timestamp: Timestamp, metadata:MessageMetadata, data: D) -> Self {
        Self { timestamp, metadata, data }
    }

    /*
    pub fn extendinfo(&self) -> &'static ExtendInfo {
        &self.extend_info
    }*/
}

impl<D: Data + PartialEq> PartialEq for ExtendTimestampedData<D> {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp && self.data == other.data
    }
}