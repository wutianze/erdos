use std::{
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use byteorder::{ByteOrder, NetworkEndian, WriteBytesExt};
use bytes::BytesMut;
use futures::future;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, InterfaceNature},
    time::sleep,
};

use crate::{dataflow::stream::StreamId, node::NodeId, OperatorId};
use abomonation_derive::Abomonation;
// Private submodules
mod control_message_codec;
mod control_message_handler;
mod endpoints;
mod errors;
mod message_codec;
mod serializable;
mod communication_deadline;

// Crate-wide visible submodules
pub(crate) mod pusher;
pub(crate) mod receivers;
pub(crate) mod senders;

// Private imports
use serializable::Serializable;
use communication_deadline::CommunicationDeadline;

// Module-wide exports
pub(crate) use control_message_codec::ControlMessageCodec;
pub(crate) use control_message_handler::ControlMessageHandler;
pub(crate) use errors::{CodecError, CommunicationError, TryRecvError};
pub(crate) use message_codec::MessageCodec;
pub(crate) use pusher::{Pusher, PusherT};

// Crate-wide exports
pub(crate) use endpoints::{RecvEndpoint, SendEndpoint};

/// Message sent between nodes in order to coordinate node and operator initialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlMessage {
    AllOperatorsInitializedOnNode(NodeId),
    OperatorInitialized(OperatorId),
    RunOperator(OperatorId),
    DataSenderInitialized(NodeId),
    DataReceiverInitialized(NodeId),
    ControlSenderInitialized(NodeId),
    ControlReceiverInitialized(NodeId),
}

#[derive(Debug, Clone, Copy,Serialize, Deserialize,Abomonation)]
pub enum Stage{
    IGNORE,
    Request,
    Response,
}

#[derive(Clone, Debug, Copy, Serialize, Deserialize, Abomonation)]
pub struct MessageMetadata {
    pub stream_id: StreamId,
    pub stage: Stage,
    pub device: u8,// 0 or 1
    pub expected_deadline: u64,
    pub timestamp_0: u128,//the time this msg is sent from source
    pub timestamp_1: u128,//the time this msg is received in other node
    pub timestamp_2: u128,//the time this msg is sent from the other node
    pub timestamp_3: u128,//the time this msg is received in this node
}

impl MessageMetadata {
    pub fn app_default(stage: Stage, device: u8, expected_deadline:u64) -> Self{
        Self { stream_id:StreamId::nil(), stage, device, expected_deadline, timestamp_0: 0, timestamp_1:0, timestamp_2: 0, timestamp_3: 0 }
    }
    pub fn rtt_test(stage: Stage, device: u8, start_time:u128) -> Self{
        Self { stream_id:StreamId::nil(), stage, device, expected_deadline:0, timestamp_0:start_time, timestamp_1:0, timestamp_2: 0, timestamp_3: 0 }
    }
    
}

#[derive(Clone)]
pub enum InterProcessMessage {
    Serialized {
        metadata: MessageMetadata,
        bytes: BytesMut,
    },
    Deserialized {
        metadata: MessageMetadata,
        data: Arc<dyn Serializable + Send + Sync>,
    },
}

impl InterProcessMessage {
    pub fn new_serialized(bytes: BytesMut, metadata: MessageMetadata) -> Self {
        Self::Serialized { metadata, bytes }
    }

    pub fn new_deserialized(//used by TimestampData and Watermark, the Stage is IGNORE
        data: Arc<dyn Serializable + Send + Sync>,
        stream_id: StreamId,
    ) -> Self {
        Self::Deserialized {
            metadata: MessageMetadata { stream_id, stage:Stage::IGNORE, device:0, expected_deadline:0, timestamp_0:0, timestamp_1: 0, timestamp_2: 0, timestamp_3: 0 },//we will fill device_index in senders.rs
            data,
        }
    }

    pub fn new_deserialized_dual(
        data: Arc<dyn Serializable + Send + Sync>,
        metadata: MessageMetadata, 
    ) -> Self {
        Self::Deserialized {
            metadata,
            data,
        }
    }
}

/// Returns a vec of TCPStreams; one for each node pair.
///
/// The function creates a TCPStream to each node address. The node address vector stores
/// the network address of each node, and is indexed by node id.
pub async fn create_tcp_streams_dual(
    node_addrs: Vec<(SocketAddr,SocketAddr)>,
    node_id: NodeId,
    node_devices: (String,String),//only the devices of this node is needed, they are used to connect to other nodes' two addresses
    natures: (InterfaceNature,InterfaceNature),
) -> Vec<(NodeId, TcpStream, TcpStream)> {
    let node_addr = node_addrs[node_id];
    // Connect to the nodes that have a lower id than the node.
    let connect_streams_fut = connect_to_nodes_dual(node_addrs[..node_id].to_vec(), node_id,(node_devices.0.as_bytes(),node_devices.1.as_bytes()),natures);
    // Wait for connections from the nodes that have a higher id than the node.
    let stream_fut = await_node_connections_dual(node_addr, node_addrs.len() - node_id - 1);
    // Wait until all connections are established.
    match future::try_join(connect_streams_fut, stream_fut).await {
        Ok((mut streams, await_streams)) => {
            // Streams contains a TCP stream for each other node.
            streams.extend(await_streams);
            streams
        }
        Err(e) => {
            tracing::error!(
                "Node {}: creating TCP streams errored with {:?}",
                node_id,
                e
            );
            panic!(
                "Node {}: creating TCP streams errored with {:?}",
                node_id, e
            )
        }
    }
}

/// Connects to all addresses and sends node id.
///
/// The function returns a vector of `(NodeId, TcpStream)` for each connection.
async fn connect_to_nodes_dual(
    addrs: Vec<(SocketAddr,SocketAddr)>,
    node_id: NodeId,
    node_devices: (&[u8],&[u8]),
    natures: (InterfaceNature, InterfaceNature),
) -> Result<Vec<(NodeId, TcpStream,TcpStream)>, std::io::Error> {
    let mut connect_futures = Vec::new();
    // For each node address, launch a task that tries to create a TCP stream to the node.
    // Now, each node tries to create two TCP streams using different NICs. Now, two addresses are needed. (test feature)
    for addr in addrs.iter() {
        connect_futures.push(connect_to_node_dual(&addr.0, node_id, node_devices.0, natures.0));
        connect_futures.push(connect_to_node_dual(&addr.1, node_id, node_devices.1, natures.1));
    }
    // Wait for all tasks to complete successfully.
    let mut tcp_results = future::try_join_all(connect_futures).await?;
    let mut streams = Vec::new();
    tcp_results.reverse();
    for i in 0..tcp_results.len()/2{
        let tcp0 = tcp_results.pop().unwrap();
        let tcp1 = tcp_results.pop().unwrap();
        streams.push((i,tcp0,tcp1));
    }
    Ok(streams)
}

/// Creates TCP stream connection to an address and writes the node id on the TCP stream.
///
/// The function keeps on retrying until it connects successfully.
async fn connect_to_node_dual(
    dst_addr: &SocketAddr,
    node_id: NodeId,
    node_device: &[u8],
    nature: InterfaceNature,
) -> Result<TcpStream, std::io::Error> {
    tracing::info!("in connect_to_node_dual");
    // Keeps on reatying to connect to `dst_addr` until it succeeds.
    let mut last_err_msg_time = Instant::now();
    loop {
        match TcpStream::connect(dst_addr,Some(node_device), nature).await {
            Ok(mut stream) => {
                stream.set_nodelay(true).expect("couldn't disable Nagle");
                tracing::info!("start bind device");
                let mut buffer: Vec<u8> = Vec::new();
                WriteBytesExt::write_u32::<NetworkEndian>(&mut buffer, node_id as u32)?;
                loop {
                    match stream.write(&buffer[..]).await {
                        Ok(_) => return Ok(stream),
                        Err(e) => {
                            tracing::error!(
                                "Node {}: could not send node id to {}; error {}; retrying in 100 ms",
                                node_id,
                                dst_addr,
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                // Only print connection errors every 1s.
                let now = Instant::now();
                if now.duration_since(last_err_msg_time) >= Duration::from_secs(1) {
                    tracing::error!(
                        "Node {}: could not connect to {}; error {}; retrying",
                        node_id,
                        dst_addr,
                        e
                    );
                    last_err_msg_time = now;
                }
                // Wait a bit until it tries to connect again.
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

/// Awaiting for connections from `expected_conns` other nodes.
///
/// Upon a new connection, the function reads from the stream the id of the node that initiated
/// the connection.
async fn await_node_connections_dual(
    addr: (SocketAddr, SocketAddr),
    expected_conns: usize,
) -> Result<Vec<(NodeId, TcpStream,TcpStream)>, std::io::Error> {
    let mut await_futures = Vec::new();
    let listener0 = TcpListener::bind(&addr.0).await?;
    let listener1 = TcpListener::bind(&addr.1).await?;
    // Awaiting for `expected_conns` conections.
    for _ in 0..expected_conns {//because we have two connections for each node
        let (stream0, _) = listener0.accept().await?;
        stream0.set_nodelay(true).expect("couldn't disable Nagle");
        // Launch a task that reads the node id from the TCP stream.
        let (stream1, _) = listener1.accept().await?;
        stream1.set_nodelay(true).expect("couldn't disable Nagle");
        // Launch a task that reads the node id from the TCP stream.
        await_futures.push(read_node_id_dual(stream0, stream1));
    }
    // Await until we've received `expected_conns` node ids.
    future::try_join_all(await_futures).await
}

/// Reads a node id from a TCP stream.
///
/// The method is used to discover the id of the node that initiated the connection.
async fn read_node_id_dual(mut stream0: TcpStream, mut stream1: TcpStream) -> Result<(NodeId, TcpStream, TcpStream), std::io::Error> {
    let mut buffer0 = [0u8; 4];
    match stream0.read_exact(&mut buffer0).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!("failed to read from socket; err = {:?}", e);
            return Err(e);
        }
    };
    let node_id0: u32 = NetworkEndian::read_u32(&buffer0);
    let mut buffer1 = [0u8; 4];
    match stream1.read_exact(&mut buffer1).await {
        Ok(n) => n,
        Err(e) => {
            tracing::error!("failed to read from socket; err = {:?}", e);
            return Err(e);
        }
    };
    let node_id1: u32 = NetworkEndian::read_u32(&buffer1);
    if node_id0 != node_id1{
        panic!("received different NodeId from the same Node;");
    }
    tracing::info!("read_node_id_dual get stream_id:{}",node_id0);
    Ok((node_id0 as NodeId, stream0,stream1))
}
