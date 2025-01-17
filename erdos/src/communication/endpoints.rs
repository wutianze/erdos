/*
 * @Description: 
 * @Author: Sauron
 * @Date: 2022-08-19 21:00:40
 * @LastEditTime: 2022-08-21 19:00:12
 * @LastEditors: Sauron
 */
use std::{fmt::Debug, sync::Arc};

use futures::FutureExt;
use tokio::{sync::mpsc, task::unconstrained};

use crate::{
    communication::{CommunicationError, InterProcessMessage,  TryRecvError},
    dataflow::stream::StreamId,
    dataflow::{Data,Message},
};

use super::{MessageMetadata};

/// Endpoint to be used to send messages between operators.
#[derive(Clone)]
pub enum SendEndpoint<D: Clone + Send + Debug> {
    /// Send messages to an operator running in the same process.
    InterThread(mpsc::UnboundedSender<D>),
    /// Send messages to operators running on a different node.
    /// Data is first sent to [`DataSender`](crate::communication::senders::DataSender)
    /// which encodes and sends the message on a TCP stream.
    InterProcess(StreamId, mpsc::UnboundedSender<InterProcessMessage>),
}

/// Zero-copy implementation of the endpoint.
/// Because we [`Arc`], the message isn't copied when sent between endpoints within the node.
impl<D: Data> SendEndpoint<Arc<Message<D>>> {
    pub fn send(&mut self, msg: Arc<Message<D>>) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(sender) => sender.send(msg).map_err(CommunicationError::from),
            Self::InterProcess(stream_id, sender) => {
                sender
                .send(InterProcessMessage::new_deserialized(msg, *stream_id))
                .map_err(CommunicationError::from)
            },
        }
    }

    pub fn send_dual(&mut self, msg: Arc<Message<D>>, mut metadata:MessageMetadata) -> Result<(), CommunicationError> {
        match self {
            Self::InterThread(sender) => sender.send(msg).map_err(CommunicationError::from),
            Self::InterProcess(stream_id, sender) => {
                metadata.stream_id = *stream_id;
                sender
                    .send(InterProcessMessage::new_deserialized_dual(msg, metadata))//stream_id
                    .map_err(CommunicationError::from)
            },
        }
    }
}

/// Endpoint to be used to receive messages.
pub enum RecvEndpoint<D: Clone + Send + Debug> {
    InterThread(mpsc::UnboundedReceiver<D>),
}

impl<D: Clone + Send + Debug> RecvEndpoint<D> {
    /// Async read of a new message.
    pub async fn read(&mut self) -> Result<D, CommunicationError> {
        match self {
            Self::InterThread(receiver) => receiver
                .recv()
                .await
                .ok_or(CommunicationError::Disconnected),
        }
    }

    /// Non-blocking read of a new message. Returns `TryRecvError::Empty` if no message is available.
    pub fn try_read(&mut self) -> Result<D, TryRecvError> {
        match self {
            // See https://github.com/tokio-rs/tokio/issues/3350.
            Self::InterThread(rx) => match unconstrained(rx.recv()).now_or_never() {
                Some(Some(msg)) => Ok(msg),
                Some(None) => Err(TryRecvError::Disconnected),
                None => Err(TryRecvError::Empty),
            },
        }
    }
}
