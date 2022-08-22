/*
 * @Description: 
 * @Author: Sauron
 * @Date: 2022-06-11 17:17:22
 * @LastEditTime: 2022-08-21 18:10:58
 * @LastEditors: Sauron
 */
//! Functions and structures for building an ERDOS application.

// Public submodules
pub mod connect;
pub mod context;
pub mod deadlines;
pub mod graph;
pub mod message;
pub mod operator;
pub mod operators;
pub mod state;
pub mod stream;
pub mod time;

// Public exports
pub use deadlines::TimestampDeadline;
pub use message::{Data, Message, TimestampedData, ExtendTimestampedData};
pub use operator::OperatorConfig;
pub use state::{AppendableState, State};
pub use stream::{LoopStream, ReadStream, Stream, WriteStream};
pub use time::Timestamp;
