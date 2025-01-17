use futures::{future, stream::SplitSink};
use futures_util::sink::SinkExt;
use std::{sync::Arc, time::{Duration,SystemTime,UNIX_EPOCH}};
use tokio::{
    self,
    net::TcpStream,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};
use tokio_util::codec::Framed;

use crate::communication::{
    CommunicationError, ControlMessage, ControlMessageCodec, ControlMessageHandler,
    InterProcessMessage, MessageCodec, CommunicationDeadline, Stage,
};
use crate::node::NodeId;
use crate::scheduler::endpoints_manager::ChannelsToSenders;

use futures_delay_queue::DelayQueue;

#[allow(dead_code)]
/// The [`DataSender`] pulls messages from a FIFO inter-thread channel.
/// The [`DataSender`] services all operators sending messages to a particular
/// node which may result in congestion.
pub(crate) struct DataSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink0: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    sink1: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<InterProcessMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
    deadline_queue: DelayQueue<CommunicationDeadline, futures_intrusive::buffer::GrowingHeapBuf<CommunicationDeadline>>,
}

impl DataSender {
    pub(crate) async fn new(
        node_id: NodeId,
        sink0: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        sink1: SplitSink<Framed<TcpStream, MessageCodec>, InterProcessMessage>,
        channels_to_senders: Arc<Mutex<ChannelsToSenders>>,
        control_handler: &mut ControlMessageHandler,
        deadline_queue: DelayQueue<CommunicationDeadline, futures_intrusive::buffer::GrowingHeapBuf<CommunicationDeadline>>
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::unbounded_channel();
        // Add entry in the shared state map.
        channels_to_senders.lock().await.add_sender(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_data_sender(node_id, control_tx);
        Self {
            node_id,
            sink0,
            sink1,
            rx,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
            deadline_queue,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify [`ControlMessageHandler`] that sender is initialized.
        self.control_tx
            .send(ControlMessage::DataSenderInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        // TODO: listen on control_rx?
        loop {
            match self.rx.recv().await {
                Some(mut msg) => {
                    //println!("send sth");
                    match msg{
                        InterProcessMessage::Serialized {metadata:_,bytes:_ } => unreachable!(),
                        InterProcessMessage::Deserialized { ref mut metadata, data:_ } => {
                            match metadata.stage{// stage is given by the application
                                Stage::Request =>{
                                    //metadata.timestamp_0 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                                    //metadata.timestamp_0 = 0;
                                    self.deadline_queue.insert(CommunicationDeadline::new(metadata.stream_id.clone(), metadata.timestamp_0.clone()), Duration::from_millis(metadata.expected_deadline));
                                    if let Err(e) = self.sink0.send(msg.clone()).await.map_err(CommunicationError::from) {
                                        return Err(e);
                                    }//this msg is device0
                                    if let Err(e) = self.sink1.send(msg).await.map_err(CommunicationError::from) {
                                        return Err(e);
                                    }
                                },
                                Stage::Response =>{
                                    //metadata.timestamp_2 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                                    //metadata.timestamp_2 = 0;
                                    if let Err(e) = self.sink0.send(msg.clone()).await.map_err(CommunicationError::from) {
                                        return Err(e);
                                    }//this msg is device0
                                    if let Err(e) = self.sink1.send(msg).await.map_err(CommunicationError::from) {
                                        return Err(e);
                                    }
                                }
                                Stage::IGNORE =>{
                                    if let Err(e) = self.sink0.send(msg.clone()).await.map_err(CommunicationError::from) {
                                        return Err(e);
                                    }//this msg is device0
                                    if let Err(e) = self.sink1.send(msg).await.map_err(CommunicationError::from) {
                                        return Err(e);
                                    }
                                }
                                _ =>{
                                    println!("wrong Stage code in msg");
                                    unreachable!();
                                }
                            }
                            //may need to use join if sink.send doesnt return instantly,TODO
                        },
                    }
                }
                None => return Err(CommunicationError::Disconnected),
            }
        }
    }
}

/// Sends messages received from operator executors to other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `InterProcessMessages` messages, which it
/// forwards on the TCP stream.
pub(crate) async fn run_senders(senders: Vec<DataSender>) -> Result<(), CommunicationError> {
    println!("run_senders");
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(
        senders
            .into_iter()
            .map(|mut sender| tokio::spawn(async move { sender.run().await })),
    )
    .await;
    Ok(())
}

#[allow(dead_code)]
/// Listens for control messages on a `tokio::sync::mpsc` channel, and sends received messages on the network.
pub(crate) struct ControlSender {
    /// The id of the node the sink is sending data to.
    node_id: NodeId,
    /// Framed TCP write sink.
    sink0: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
    sink1: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
    /// Tokio channel receiver on which to receive data from worker threads.
    rx: UnboundedReceiver<ControlMessage>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Channel receiver for control messages intended for this `ControlSender`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

impl ControlSender {
    pub(crate) fn new(
        node_id: NodeId,
        sink0: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
        sink1: SplitSink<Framed<TcpStream, ControlMessageCodec>, ControlMessage>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up channel to other node.
        let (tx, rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_node(node_id, tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_control_sender(node_id, control_tx);
        Self {
            node_id,
            sink0,
            sink1,
            rx,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // Notify `ControlMessageHandler` that sender is initialized.
        self.control_tx
            .send(ControlMessage::ControlSenderInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        // TODO: listen on control_rx
        loop {
            match self.rx.recv().await {
                Some(msg) => {//TODO, we dont change control part now
                    if let Err(e) = self.sink0.send(msg).await.map_err(CommunicationError::from) {
                        return Err(e);
                    }
                    /*if let Err(e) = self.sink1.send(msg).await.map_err(CommunicationError::from) {
                        return Err(e);
                    }*/
                }
                None => {
                    return Err(CommunicationError::Disconnected);
                }
            }
        }
    }
}

/// Sends messages received from the control handler other nodes.
/// The function launches a task for each TCP sink. Each task listens
/// on a mpsc channel for new `ControlMessage`s, which it
/// forwards on the TCP stream.
pub(crate) async fn run_control_senders(
    mut senders: Vec<ControlSender>,
) -> Result<(), CommunicationError> {
    // Waits until all futures complete. This code will only be reached
    // when all the mpsc channels are closed.
    future::join_all(senders.iter_mut().map(|sender| sender.run())).await;
    Ok(())
}
