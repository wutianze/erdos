use std::{thread, time::Duration};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use erdos::{
    dataflow::{
        context::SinkContext,
        operator::{Sink, Source, ExtendInfo},
        //operators::{Filter, Join, Map, Split},
        state::TimeVersionedState,
        stream::{WriteStream, WriteStreamT},
        Message, OperatorConfig, Timestamp,
    },
    node::Node,
    Configuration,
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
                .send(Message::new_extendmessage(timestamp.clone(), ExtendInfo::new(10, 0, 0, 0, 0), t))
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

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<TimeVersionedState<usize>, usize> for SinkOperator {
    fn on_extenddata(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, extend_info:&mut ExtendInfo,data: &usize) {
        let timestamp = ctx.timestamp().clone();
        tracing::info!(
            "{} @ {:?}: Received {}, extend_info {}",
            ctx.operator_config().get_name(),
            timestamp,
            data,
            extend_info,
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

    let source_config = OperatorConfig::new().name("SourceOperator").node(1);
    // Streams data 0, 1, 2, ..., 9 with timestamps 0, 1, 2, ..., 9.
    let source_stream = erdos::connect_source(SourceOperator::new, source_config);
    let odds_sink_config = OperatorConfig::new().name("OddsSinkOperator").node(0);
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        odds_sink_config,
        &source_stream,
    );

    node.run();
}