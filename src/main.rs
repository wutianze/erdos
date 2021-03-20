#[macro_use]
extern crate erdos;

use std::usize;

use erdos::dataflow::stream::WriteStreamT;
use erdos::dataflow::*;
use erdos::node::Node;
use erdos::Configuration;
use erdos::{dataflow::operator::*, get_terminal_logger};

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<(), usize> for SourceOperator {
    fn run(&mut self, write_stream: &mut WriteStream<usize>) {
        let logger = erdos::get_terminal_logger();
        slog::info!(logger, "Running Source Operator");
        for t in 0..10 {
            let timestamp = Timestamp::new(vec![t as u64]);
            write_stream
                .send(Message::new_message(timestamp.clone(), t))
                .unwrap();
            write_stream
                .send(Message::new_watermark(timestamp))
                .unwrap();
        }
    }

    fn destroy(&mut self) {
        slog::info!(erdos::get_terminal_logger(), "Destroying Source Operator");
    }
}

struct SquareOperator {}

impl SquareOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInOneOut<(), usize, usize> for SquareOperator {
    fn on_data(ctx: &mut OneInOneOutContext<usize>, data: &usize) {
        let logger = erdos::get_terminal_logger();
        slog::info!(
            logger,
            "SquareOperator @ {:?}: received {}",
            ctx.timestamp,
            data
        );
        ctx.write_stream
            .send(Message::new_message(ctx.timestamp.clone(), data * data))
            .unwrap();
        slog::info!(
            logger,
            "SquareOperator @ {:?}: sent {}",
            ctx.timestamp,
            data * data
        );
    }

    fn on_data_stateful(ctx: &mut StatefulOneInOneOutContext<(), usize>, data: &usize) {}

    fn on_watermark(ctx: &mut StatefulOneInOneOutContext<(), usize>) {}
}

struct SumOperator {}

impl SumOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl OneInOneOut<usize, usize, usize> for SumOperator {
    fn on_data(ctx: &mut OneInOneOutContext<usize>, data: &usize) {}

    // Proof of concept that state "works", although it is mis-implemented.
    // Also need to fix the lattice.
    fn on_data_stateful(ctx: &mut StatefulOneInOneOutContext<usize, usize>, data: &usize) {
        let logger = erdos::get_terminal_logger();
        slog::info!(
            logger,
            "SumOperator @ {:?}: received {}",
            ctx.timestamp,
            data
        );
        let mut state = ctx.state.try_lock().unwrap();
        *state += data;
        ctx.write_stream
            .send(Message::new_message(ctx.timestamp.clone(), *state))
            .unwrap();
        slog::info!(logger, "SumOperator @ {:?}: sent {}", ctx.timestamp, state);
    }

    fn on_watermark(ctx: &mut StatefulOneInOneOutContext<usize, usize>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<(usize, usize), usize> for SinkOperator {
    fn on_data(ctx: &mut SinkContext, data: &usize) {
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: received {}",
            ctx.timestamp,
            data
        )
    }

    fn on_data_stateful(ctx: &mut StatefulSinkContext<(usize, usize)>, _data: &usize) {
        // State := number of messages and watermarks received.
        let mut state = ctx.state.try_lock().unwrap();
        state.0 += 1;
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: received {} data messages, {} watermarks",
            ctx.timestamp,
            state.0,
            state.1,
        )
    }

    fn on_watermark(ctx: &mut StatefulSinkContext<(usize, usize)>) {
        // State := number of messages and watermarks received.
        let mut state = ctx.state.try_lock().unwrap();
        state.1 += 1;
        slog::info!(
            erdos::get_terminal_logger(),
            "SinkOperator @ {:?}: received {} data messages, {} watermarks",
            ctx.timestamp,
            state.0,
            state.1,
        )
    }
}

fn main() {
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator");
    let source_stream = erdos::connect_source(SourceOperator::new, || {}, source_config);
    // TODO: clean up API weirdness.
    // * Add a new abstract stream type.
    // * Look into removing connect from trait.
    let source_stream = ReadStream::from(&source_stream);

    let square_config = OperatorConfig::new().name("SquareOperator");
    let square_stream =
        erdos::connect_one_in_one_out(SquareOperator::new, || {}, square_config, &source_stream);
    let square_stream = ReadStream::from(&square_stream);

    let sum_config = OperatorConfig::new().name("SumOperator");
    let sum_stream =
        erdos::connect_one_in_one_out(SumOperator::new, || 0, sum_config, &square_stream);

    let sum_stream = ReadStream::from(&sum_stream);
    let sink_config = OperatorConfig::new().name("SinkOperator");
    erdos::connect_sink(SinkOperator::new, || (0, 0), sink_config, &sum_stream);

    node.run();
}
