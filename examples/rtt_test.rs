use std::{thread, time::{Duration,SystemTime,UNIX_EPOCH}};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use erdos::{
    dataflow::{
        context::{SinkContext,OneInOneOutContext},
        operator::{Sink, Source,OneInOneOut},
        //operators::{Filter, Join, Map, Split},
        state::TimeVersionedState,
        stream::{WriteStream, WriteStreamT},
        Message, OperatorConfig, Timestamp,
    },
    node::Node,
    Configuration, communication::MessageMetadata,
};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

struct SourceOperator {}

impl SourceOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Source<String> for SourceOperator {
    fn run(&mut self, _operator_config: &OperatorConfig, write_stream: &mut WriteStream<String>) {
        tracing::info!("Running Source Operator");
	/*let mut to_send = String::new();
	for i in 0..262144{
		to_send.push('a');
	}*/
    let to_send: String = rand::thread_rng()
    .sample_iter(&rand::distributions::Alphanumeric)
    .take(1024*1024)
    .map(char::from)
    .collect::<String>();

	print!("to_send size:{}",to_send.len());
        for t in 0..60 {
		let data_copy = to_send.clone();
            let timestamp = Timestamp::Time(vec![t as u64]);
            write_stream
                .send(Message::new_extendmessage(timestamp.clone(), MessageMetadata::rtt_test(erdos::communication::Stage::Request, 0, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()), data_copy))
                .unwrap();
            write_stream
                .send(Message::new_watermark(timestamp))
                .unwrap();
            thread::sleep(Duration::from_millis(500));
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

impl OneInOneOut<(), String, String> for MiddleOperator {

	fn on_data(&mut self, ctx: &mut OneInOneOutContext<(), String>, data: &String) {}

    fn on_extenddata(&mut self, ctx: &mut OneInOneOutContext<(), String>, metadata:&MessageMetadata, data: &String) {
        //tracing::info!("MiddleOperator @ {:?}: received {}", ctx.timestamp(), data);
        let timestamp = ctx.timestamp().clone();
        ctx.write_stream()
            .send(Message::new_extendmessage(timestamp, *metadata, String::from(data)))
            .unwrap();
	    /*
        tracing::info!(
            "MiddleOperator @ {:?}: sent {}",
            ctx.timestamp(),
            data
        );*/
    }

    fn on_watermark(&mut self, _ctx: &mut OneInOneOutContext<(), String>) {}
}

struct SinkOperator {}

impl SinkOperator {
    pub fn new() -> Self {
        Self {}
    }
}

impl Sink<TimeVersionedState<usize>, String> for SinkOperator {
    fn on_extenddata(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, metadata:&MessageMetadata,data: &String) {
        //let timestamp = ctx.timestamp().clone();

        println!("{},{}",metadata.device,SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() - metadata.timestamp_0);
        //println!("received size:{}",data.len());
        /*tracing::info!(
            "{} @ {:?}: Received on_extenddata, start at: {}, cost:{}",
            ctx.operator_config().get_name(),
            timestamp,
	    metadata.timestamp_0,
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() - metadata.timestamp_0,
        );*/

        // Increment the message count.
        *ctx.current_state().unwrap() += 1;
    }

    fn on_data(&mut self, ctx: &mut SinkContext<TimeVersionedState<usize>>, data: &String) {
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
        //println!("watermark:{:?}",timestamp);
        /*tracing::info!(
            "{} @ {:?}: Received {} data messages.",
            ctx.operator_config().get_name(),
            timestamp,
            ctx.current_state().unwrap(),
        );*/
    }
}

fn main() {
    /*
    tracing_subscriber::registry()
        .with(fmt::layer())
        .init();*/
    let args = erdos::new_app("ERDOS").get_matches();
    let mut node = Node::new(Configuration::from_args(&args));

    let source_config = OperatorConfig::new().name("SourceOperator").node(0);
    // Streams data 0, 1, 2, ..., 9 with timestamps 0, 1, 2, ..., 9.
    let source_stream = erdos::connect_source(SourceOperator::new, source_config);

    //let middle_config = OperatorConfig::new().name("MiddleOperator").node(1);
    //let middle_stream = erdos::connect_one_in_one_out(MiddleOperator::new, || {}, middle_config, &source_stream);
    let odds_sink_config = OperatorConfig::new().name("OddsSinkOperator").node(1);
    erdos::connect_sink(
        SinkOperator::new,
        TimeVersionedState::new,
        odds_sink_config,
        &source_stream,
    );

    node.run();
}