use std::{thread, time::Duration};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source},
        //operators::{Filter, Join, Map, Split},
        state::TimeVersionedState,
        stream::{WriteStream, WriteStreamT},
        Message, OperatorConfig, Timestamp,
    },
    node::Node,
    Configuration, communication::MessageMetadata,
};

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<usize> for SourceOperator {
    fn run(&mut self, _operator_config: &OperatorConfig, write_stream: &mut WriteStream<usize>) {
        tracing::info!("Running Source Operator");
        for t in 0..3 {
            let timestamp = Timestamp::Time(vec![t as u64]);
            write_stream
                .send(Message::new_extendmessage(timestamp.clone(), MessageMetadata::app_default(erdos::communication::Stage::Request, 0, 100), t))
                .unwrap();
            write_stream
                .send(Message::new_watermark(timestamp))
                .unwrap();
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn destroy(&mut self) {
        tracing::info!("Destroying Source Operator");
    }
}

struct MiddleOperator {}

impl MiddleOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInOneOut<(), usize, usize> for MiddleOperator {

    fn on_extenddata(&mut self, ctx: &mut OneInOneOutContext<(), usize>, metadata:&MessageMetadata, data: &usize) {
        tracing::info!("MiddleOperator @ {:?}: received {}", ctx.timestamp(), data);
        let timestamp = ctx.timestamp().clone();
        ctx.write_stream()
            .send(Message::new_extendmessage(timestamp, metadata, data * data))
            .unwrap();
        tracing::info!(
            "MiddleOperator @ {:?}: sent {}",
            ctx.timestamp(),
            data * data
        );
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), usize>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<TimeVersionedState<usize>, usize> for SinkOperator {
    fn on_extenddata(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, metadata:&MessageMetadata,data: &usize) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received on_extenddata {}, metadata {}",
            ctx.operator_config().get_name(),
            timestamp,
            data,
            metadata.timestamp_0,
        );

        // Increment the message count.
        *ctx.current_state().unwrap() += 1;
    }

    fn on_data(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, data: &usize) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received on_data {}",
            ctx.operator_config().get_name(),
            timestamp,
            data,
        );

        // Increment the message count.
        *ctx.current_state().unwrap() += 1;
    }

    fn on_watermark(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {} data messages.",
            ctx.operator_config().get_name(),
            timestamp,
            ctx.current_state().unwrap(),
        );
    }
}

fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .init();
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator").node(0);
    // Streams data 0, 1, 2, ..., 9 with timestamps 0, 1, 2, ..., 9.
    let source_stream = erdos::connect_source(SourceOperator::new, source_config);

    let middle_config = OperatorConfig::new().name("MiddleOperator").node(1);
    let middle_stream = erdos::connect_one_in_one_out(MiddleOperator::new, || {}, square_config, &source_stream);
    let odds_sink_config = OperatorConfig::new().name("OddsSinkOperator").node(0);
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        odds_sink_config,
        &middle_stream,
    );

    node.run();
}