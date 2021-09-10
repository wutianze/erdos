use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use serde::Deserialize;

use crate::{
    dataflow::{
        graph::{default_graph, StreamSetupHook},
        Data, Message,
    },
    scheduler::channel_manager::ChannelManager,
};

use super::{errors::SendError, StreamId, WriteStream, WriteStreamT};

/// An [`IngestStream`] enables drivers to inject data into a running ERDOS application.
///
/// Similar to a [`WriteStream`], an [`IngestStream`] exposes a [`send`](IngestStream::send)
/// function to allow drivers to send data to the operators of the constructed graph.
///
/// # Example
/// The below example shows how to use a [`MapOperator`](crate::dataflow::operators::MapOperator)
/// to double an incoming stream of [`u32`] messages, and return them as [`u64`] messages.
/// ```
/// # use erdos::dataflow::{
/// #    stream::IngestStream,
/// #    operators::MapOperator,
/// #    OperatorConfig, Message, Timestamp
/// # };
/// # use erdos::*;
/// #
/// # let map_config = OperatorConfig::new()
//  #     .name("MapOperator")
/// #     .arg(|data: &u32| -> u64 { (data * 2) as u64 });
/// #
/// // Create an IngestStream. The driver is assigned an ID of 0.
/// let mut ingest_stream = IngestStream::new(); // or IngestStream::new_with_name("driver")
/// let output_read_stream = connect_1_write!(MapOperator<u32, u64>, map_config, ingest_stream);
///
/// // Send data on the IngestStream.
/// for i in 1..10 {
///     // We expect an error because we have not started the dataflow graph yet.
///     match ingest_stream.send(Message::new_message(Timestamp::new(vec![i as u64]), i)) {
///         Err(e) => (),
///         _ => (),
///     };
/// }
/// ```
pub struct IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// The unique ID of the stream (automatically generated by the constructor)
    id: StreamId,
    // Use a std mutex because the driver doesn't run on the tokio runtime.
    write_stream_option: Arc<Mutex<Option<WriteStream<D>>>>,
}

impl<D> IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Returns a new instance of the [`IngestStream`].
    pub fn new() -> Self {
        slog::debug!(crate::TERMINAL_LOGGER, "Initializing an IngestStream");
        let id = StreamId::new_deterministic();
        IngestStream::new_internal(id, &format!("ingest_stream_{}", id.to_string()))
    }

    /// Returns a new instance of the [`IngestStream`].
    ///
    /// # Arguments
    /// * `name` - The name to be given to the stream.
    pub fn new_with_name(name: &str) -> Self {
        slog::debug!(
            crate::TERMINAL_LOGGER,
            "Initializing an IngestStream with the name {}",
            name
        );
        let id = StreamId::new_deterministic();
        IngestStream::new_internal(id, name)
    }

    /// Creates the [`WriteStream`] to be used to send messages to the dataflow, and adds it to
    /// the dataflow graph.
    /// Panics if the stream could not be created.
    fn new_internal(id: StreamId, name: &str) -> Self {
        let ingest_stream = Self {
            id,
            write_stream_option: Arc::new(Mutex::new(None)),
        };
        let write_stream_option_copy = Arc::clone(&ingest_stream.write_stream_option);

        // Sets up self.write_stream_option using channel_manager
        let setup_hook = ingest_stream.get_setup_hook();

        default_graph::add_ingest_stream(&ingest_stream, setup_hook);
        ingest_stream
    }

    /// Returns `true` if a top watermark message was received or the [`IngestStream`] failed to
    /// set up.
    pub fn is_closed(&self) -> bool {
        self.write_stream_option
            .lock()
            .unwrap()
            .as_ref()
            .map(WriteStream::is_closed)
            .unwrap_or(true)
    }

    /// Sends data on the stream.
    ///
    /// # Arguments
    /// * `msg` - The message to be sent on the stream.
    pub fn send(&mut self, msg: Message<D>) -> Result<(), SendError> {
        if !self.is_closed() {
            loop {
                {
                    if let Some(write_stream) = self.write_stream_option.lock().unwrap().as_mut() {
                        let res = write_stream.send(msg);
                        return res;
                    }
                }
                thread::sleep(Duration::from_millis(100));
            }
        } else {
            slog::warn!(
                crate::TERMINAL_LOGGER,
                "Trying to send messages on a closed IngestStream {} (ID: {})",
                default_graph::get_stream_name(&self.id),
                self.id(),
            );
            return Err(SendError::Closed);
        }
    }

    pub fn id(&self) -> StreamId {
        self.id
    }

    pub fn name(&self) -> String {
        default_graph::get_stream_name(&self.id)
    }

    pub fn set_name(&self, name: &str) {
        default_graph::set_stream_name(&self.id, name);
    }

    /// Returns a function which sets up self.write_stream_option using the channel_manager.
    pub(crate) fn get_setup_hook(&self) -> impl StreamSetupHook {
        let id = self.id();
        let write_stream_option_copy = Arc::clone(&self.write_stream_option);

        move |channel_manager: Arc<Mutex<ChannelManager>>| match channel_manager
            .lock()
            .unwrap()
            .get_send_endpoints(id)
        {
            Ok(send_endpoints) => {
                let write_stream = WriteStream::from_endpoints(send_endpoints, id);
                write_stream_option_copy
                    .lock()
                    .unwrap()
                    .replace(write_stream);
            }
            Err(msg) => panic!("Unable to set up IngestStream {}: {}", id, msg),
        }
    }
}

impl<D> WriteStreamT<D> for IngestStream<D>
where
    for<'a> D: Data + Deserialize<'a>,
{
    /// Blocks until write stream is available
    fn send(&mut self, msg: Message<D>) -> Result<(), SendError> {
        self.send(msg)
    }
}
