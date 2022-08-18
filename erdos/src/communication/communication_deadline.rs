use crate::dataflow::stream::StreamId;
pub struct CommunicationDeadline{
	stream_id: StreamId,
	start_timestamp: u128,
}

impl CommunicationDeadline{
	pub fn new(
		stream_id: StreamId,
		start_timestamp: u128,
	) -> Self{
		Self{
			stream_id,
			start_timestamp,
		}
	}
}