use std::{collections::HashMap, sync::Arc,time::{Duration,SystemTime,UNIX_EPOCH}};

use futures::{future, stream::SplitStream, FutureExt};
use futures_util::stream::StreamExt;
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
};


use tokio_util::codec::Framed;

use crate::{
    communication::{
        CommunicationError, ControlMessage, ControlMessageCodec, ControlMessageHandler,
        InterProcessMessage, MessageCodec, PusherT,
    },
    dataflow::stream::StreamId,
    node::NodeId,
    scheduler::endpoints_manager::ChannelsToReceivers,
};

use futures_delay_queue::Receiver;
use super::{Stage, communication_deadline::CommunicationDeadline};

pub struct ServerTimeInfo{
    start_time: u128,
    transfer_time_request: u128,
    transfer_time_response: u128,
}

impl ServerTimeInfo{
    pub fn new(
        start_time: u128,
        transfer_time_request: u128,
        transfer_time_response: u128,
    ) -> Self{
        Self{start_time,transfer_time_request,transfer_time_response}
    }
}

/// Listens on a TCP stream, and pushes messages it receives to operator executors.
#[allow(dead_code)]
pub(crate) struct DataReceiver {
    /// The id of the node the TCP stream is receiving data from.
    node_id: NodeId,
    /// Framed TCP read stream.
    stream0: SplitStream<Framed<TcpStream, MessageCodec>>,
    stream1: SplitStream<Framed<TcpStream, MessageCodec>>,
    /// Channel receiver on which new pusher updates are received.
    rx: UnboundedReceiver<(StreamId, Box<dyn PusherT>)>,
    /// Mapping between stream id to [`PusherT`] trait objects.
    /// [`PusherT`] trait objects are used to deserialize and send
    /// messages to operators.
    stream_id_to_pusher: HashMap<StreamId, Box<dyn PusherT>>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,

    deadline_queue_rx: Receiver<CommunicationDeadline>,
}

impl DataReceiver {
    pub(crate) async fn new(
        node_id: NodeId,
        stream0: SplitStream<Framed<TcpStream, MessageCodec>>,
        stream1: SplitStream<Framed<TcpStream, MessageCodec>>,
        channels_to_receivers: Arc<Mutex<ChannelsToReceivers>>,
        control_handler: &mut ControlMessageHandler,
        deadline_queue_rx: Receiver<CommunicationDeadline>,
    ) -> Self {
        // Create a channel for this stream.
        let (tx, rx) = mpsc::unbounded_channel();
        // Add entry in the shared state vector.
        channels_to_receivers.lock().await.add_sender(tx);
        // Set up control channel.
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        control_handler.add_channel_to_data_receiver(node_id, control_tx);
        Self {
            node_id,
            stream0,
            stream1,
            rx,
            stream_id_to_pusher: HashMap::new(),
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
            deadline_queue_rx,
        }
    }

    pub(crate) async fn run(&mut self, policy: u8) -> Result<(), CommunicationError> {
        // Notify `ControlMessageHandler` that receiver is initialized.
        self.control_tx
            .send(ControlMessage::DataReceiverInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        let stream0 = &mut self.stream0;
        let stream1 = &mut self.stream1;

        let (tx, mut mrx) = mpsc::channel(32);
        let tx2 = tx.clone();


        let handle0 = async move{
            while let Some(res) = stream0.next().await{
                    println!("receive from stream0");
            match res {
                // Push the message to the listening operator executors.
                Ok(msg) => {
                    if let Err(_e) = tx.send(msg).await {
                        panic!("stream0 send msg fail")
                    }
                    }
                Err(e) => panic!("DataReceiver receives an Error and panic:{:?}",e),
                }
                    }
    };
        let handle1 = async move{
            while let Some(res) = stream1.next().await{
                    println!("receive from stream1");
            match res {
                // Push the message to the listening operator executors.
                Ok(msg) => {
                    if let Err(_e) = tx2.send(msg).await {
                        panic!("stream0 send msg fail")
                    }
                    }
                Err(_e) => panic!("DataReceiver receives an Error and panic"),
                }
                    }
    };

    let rx = &mut self.rx;
    let stream_id_to_pusher = &mut self.stream_id_to_pusher;
    let deadline_queue_rx = &mut self.deadline_queue_rx;
    let handle2 = async move{
        let mut server_info = HashMap::new();// server_info only stores info of device 0
        loop{
            tokio::select! {
                Some(communication_deadline) = deadline_queue_rx.receive() =>{// stage 1 should never run this
                    let piece_info = server_info.entry(communication_deadline.stream_id).or_insert(ServerTimeInfo::new(communication_deadline.start_timestamp,0,0));// 0 means not received
                    //should only happen in Stage::ResponseReceived, every msg will cause this
                    
                    if communication_deadline.start_timestamp > piece_info.start_time{// the response of this deadline is received
                        continue;
                    }else{
                        tracing::warn!("deadline alarmed for stream:{} began at time:{}",communication_deadline.stream_id,communication_deadline.start_timestamp);
                        piece_info.start_time = communication_deadline.start_timestamp;
                        //run the handler
                    }
                    continue;
                },
                Some(msg) = mrx.recv() =>{
                    while let Some(Some((stream_id, pusher))) = rx.recv().now_or_never() {
                        stream_id_to_pusher.insert(stream_id, pusher);
                    }
                    let (mut metadata, bytes) = match msg{
                        InterProcessMessage::Serialized { metadata, bytes } => (metadata, bytes),
                        InterProcessMessage::Deserialized { metadata:_, data:_, } => unreachable!(),
                    };
                    
                    //tracing::info!("receive msg metadata:{},",metadata.stream_id);

                    let piece_info = server_info.entry(metadata.stream_id).or_insert(ServerTimeInfo::new(metadata.timestamp_0,metadata.timestamp_1-metadata.timestamp_0, metadata.timestamp_3-metadata.timestamp_2));
                    match metadata.stage{
                        Stage::Request =>{
                            metadata.timestamp_1 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                            piece_info.start_time = metadata.timestamp_0;
                            if metadata.device == 0{//we get main msg first, use it directly
                                piece_info.transfer_time_request = (metadata.timestamp_1 - metadata.timestamp_0 + piece_info.transfer_time_request)/2;
                            }
                        },
                        Stage::Response =>{
                            metadata.timestamp_3 = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
                            if metadata.device == 0{//we get main msg first, use it directly
                                piece_info.transfer_time_response = (metadata.timestamp_3 - metadata.timestamp_2+ piece_info.transfer_time_response)/2;
                            }
                        },
                        Stage::IGNORE =>{
                            tracing::info!("receive non extend data");
                        },
                    }
                    if metadata.timestamp_0 < piece_info.start_time{
                        //todo: device 0 and device is different
                        continue;//out of date msg
                    }

                    //new msg
                    
                    match stream_id_to_pusher.get_mut(&metadata.stream_id) {
                        Some(pusher) => {
                            if let Err(e) = pusher.send_from_bytes(bytes,metadata) {
                                    panic!("pusher send_from_bytes error");
                                }
                            }
                            None => {
                                println!("Receiver does not have any pushers.");
                            },
                        }
                },
                else => break,
            }
        }
    };
            let tuple = future::join3(handle0, handle1, handle2);
            tuple.await;
            Ok(())
    }

    /*
    async fn update_pushers(&mut self) {
        // Execute while we still have pusher updates.
        while let Some(Some((stream_id, pusher))) = self.rx.recv().now_or_never() {
            self.stream_id_to_pusher.insert(stream_id, pusher);
        }
    }*/
}

/// Receives TCP messages, and pushes them to operators endpoints.
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
pub(crate) async fn run_receivers(
    mut receivers: Vec<DataReceiver>,
) -> Result<(), CommunicationError> {
    println!("run_receivers");
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run(0))).await;
    Ok(())
}

/// Listens on a TCP stream, and pushes control messages it receives to the node.
#[allow(dead_code)]
pub(crate) struct ControlReceiver {
    /// The id of the node the stream is receiving data from.
    node_id: NodeId,
    /// Framed TCP read stream.
    stream0: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
    stream1: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
    /// Tokio channel sender to `ControlMessageHandler`.
    control_tx: UnboundedSender<ControlMessage>,
    /// Tokio channel receiver from `ControlMessageHandler`.
    control_rx: UnboundedReceiver<ControlMessage>,
}

impl ControlReceiver {
    pub(crate) fn new(
        node_id: NodeId,
        stream0: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
        stream1: SplitStream<Framed<TcpStream, ControlMessageCodec>>,
        control_handler: &mut ControlMessageHandler,
    ) -> Self {
        // Set up control channel.
        let (tx, control_rx) = tokio::sync::mpsc::unbounded_channel();
        control_handler.add_channel_to_control_receiver(node_id, tx);
        Self {
            node_id,
            stream0,
            stream1,
            control_tx: control_handler.get_channel_to_handler(),
            control_rx,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<(), CommunicationError> {
        // TODO: update `self.channel_to_handler` for up-to-date mappings.
        // between channels and handlers (e.g. for fault-tolerance).
        // Notify `ControlMessageHandler` that sender is initialized.
        self.control_tx
            .send(ControlMessage::ControlReceiverInitialized(self.node_id))
            .map_err(CommunicationError::from)?;
        while let Some(res) = self.stream0.next().await {
            print!("receive control message");
            match res {
                Ok(msg) => {
                    self.control_tx
                        .send(msg)
                        .map_err(CommunicationError::from)?;
                }
                Err(e) => return Err(CommunicationError::from(e)),
            }
        }
        Ok(())
    }
}

/// Receives TCP messages, and pushes them to the ControlHandler
/// The function receives a vector of framed TCP receiver halves.
/// It launches a task that listens for new messages for each TCP connection.
pub(crate) async fn run_control_receivers(
    mut receivers: Vec<ControlReceiver>,
) -> Result<(), CommunicationError> {
    // Wait for all futures to finish. It will happen only when all streams are closed.
    future::join_all(receivers.iter_mut().map(|receiver| receiver.run())).await;
    Ok(())
}
