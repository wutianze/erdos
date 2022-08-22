/*
 * @Description: 
 * @Author: Sauron
 * @Date: 2022-08-19 21:00:40
 * @LastEditTime: 2022-08-21 19:28:32
 * @LastEditors: Sauron
 */
use std::{
    any::Any,
    fmt::{self, Debug},
    sync::Arc,
};

use bytes::BytesMut;

use crate::{
    communication::{
        serializable::{Deserializable, DeserializedMessage},
        CommunicationError, SendEndpoint,
    },
    dataflow::{Data,Message},
};

use super::MessageMetadata;

/// Trait used to deserialize a message and send it on a collection of [`SendEndpoint`]s
/// without exposing the message's type to owner of the [`PusherT`] trait object.
pub trait PusherT: Send {
    fn as_any(&mut self) -> &mut dyn Any;
    /// To be used to clone a boxed pusher.
    fn box_clone(&self) -> Box<dyn PusherT>;
    /// Creates message from bytes and sends it to endpoints.
    fn send_from_bytes(&mut self, buf: BytesMut, metadata: MessageMetadata) -> Result<(), CommunicationError>;
    //fn msg_from_bytes(&mut self, mut buf: BytesMut) -> Option<Message<D>>;
}

/// Internal structure used to send data on a collection of [`SendEndpoint`]s.
#[derive(Clone)]
pub struct Pusher<D: Debug + Clone + Send> {
    // TODO: We might want to order the endpoints by the priority of their tasks.
    endpoints: Vec<SendEndpoint<D>>,
}

/// Zero-copy implementation of the pusher.
impl<D: Data> Pusher<Arc<Message<D>>> {
    pub fn new() -> Self {
        Self {
            endpoints: Vec::new(),
        }
    }

    pub fn add_endpoint(&mut self, endpoint: SendEndpoint<Arc<Message<D>>>) {
        self.endpoints.push(endpoint);
    }

    pub fn send(&mut self, msg: Arc<Message<D>>) -> Result<(), CommunicationError> {
        for endpoint in self.endpoints.iter_mut() {
            endpoint.send(Arc::clone(&msg))?;
        }
        Ok(())
    }
    
    pub fn send_dual(&mut self, msg: Arc<Message<D>>, metadata:MessageMetadata) -> Result<(), CommunicationError> {
        for endpoint in self.endpoints.iter_mut() {
            endpoint.send_dual(Arc::clone(&msg), metadata.clone())?;
        }
        Ok(())
    }

}

impl Clone for Box<dyn PusherT> {
    /// Clones a boxed pusher.
    fn clone(&self) -> Box<dyn PusherT> {
        self.box_clone()
    }
}

/// The [`PusherT`] trait is implemented only for the [`Data`] pushers.
impl<D:Data> PusherT for Pusher<Arc<Message<D>>>
    where 
    for<'de> Message<D>: Deserializable<'de>,
{
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn box_clone(&self) -> Box<dyn PusherT> {
        Box::new((*self).clone())
    }
    
    /*
    fn msg_from_bytes(&mut self, mut buf: BytesMut) -> Option<Message<D>> {
        if !self.endpoints.is_empty() {
            let msg = match Deserializable::decode(&mut buf)? {//TODO,maybe wrong
                DeserializedMessage::<Message<D>>::Owned(msg) => msg,
                DeserializedMessage::<Message<D>>::Ref(msg) => msg.clone(),
            };
            Option::Some(msg)
        }
        None
    }*/
    
    fn send_from_bytes(&mut self, mut buf: BytesMut,metadata:MessageMetadata) -> Result<(), CommunicationError> {
        if !self.endpoints.is_empty() {
            let msg = match Deserializable::decode(&mut buf)? {//TODO,maybe wrong
                DeserializedMessage::<Message<D>>::Owned(msg) => msg,
                DeserializedMessage::<Message<D>>::Ref(msg) => msg.clone(),
            };
            let msg_arc = match metadata.stage{
                super::Stage::IGNORE => {
                    Arc::new(msg)
                },
                _ =>{//ExtendTimestampData but as TimestampData
                    match msg{
                        Message::TimestampedData(d) => {
                        Arc::new(Message::new_extendmessage(d.timestamp, metadata, d.data))
                        },
                        Message::Watermark(_) => {
                            Arc::new(msg)
                        }
                        _ => {
                            print!("msg is {:?}",msg);
                            panic!("received ExtendTimestampData with wrong Message Enum")
                        },
                    }
                },
            };
            self.send(msg_arc)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Box<dyn PusherT> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Box<dyn PusheT> {{ }}")
    }
}
