use std::net::SocketAddr;

use tracing::Level;

use tokio::net::InterfaceNature;

use crate::node::NodeId;

/// Stores the configuration parameters of a [`node`](crate::node::Node).
#[derive(Clone)]
pub struct Configuration {
    /// The index of the node.
    pub index: NodeId,
    /// The number of OS threads the node will use.
    pub num_threads: usize,
    /// Mapping between node indices and data socket addresses.
    pub data_addresses: Vec<(SocketAddr,SocketAddr)>,
    /// Mapping between node indices and control socket addresses.
    pub control_addresses: Vec<(SocketAddr,SocketAddr)>,
    /// NIC devices of this node
    pub devices: ([u8],[u8]),
    /// Natures of devices
    pub natures: (InterfaceNature,InterfaceNature),
    /// DOT file to export dataflow graph.
    pub graph_filename: Option<String>,
    /// The logging level of the logger initialized by ERDOS.
    /// If `None`, ERDOS will not initialize a logger.
    /// Defaults to [`Level::DEBUG`] when compiling in debug mode,
    /// [`Level::INFO`] when compiling in release mode.
    ///
    /// While [`tracing`] provides extensions for connecting additional
    /// subscribers, note that these may impact performance.
    pub logging_level: Option<Level>,
}

impl Configuration {
    /// Creates a new node configuration.
    pub fn new(
        node_index: NodeId,
        data_addresses: Vec<(SocketAddr,SocketAddr)>,
        control_addresses: Vec<(SocketAddr,SocketAddr)>,
        devices: (&[u8],&[u8]),
        natures: (InterfaceNature,InterfaceNature),
        num_threads: usize,
    ) -> Self {
        let log_level = if cfg!(debug_assertions) {
            Some(Level::DEBUG)
        } else {
            Some(Level::INFO)
        };
        Self {
            index: node_index,
            num_threads,
            data_addresses,
            control_addresses,
            devices,
            natures,
            graph_filename: None,
            logging_level: log_level,
        }
    }

    /// Creates a node configuration from command line arguments.
    pub fn from_args(args: &clap::ArgMatches) -> Self {
        let num_threads = args
            .value_of("threads")
            .unwrap()
            .parse()
            .expect("Unable to parse number of worker threads");

        let data_addrs = args.value_of("data-addresses").unwrap();
        let mut data_addresses: Vec<SocketAddr> = Vec::new();
        for addrs in data_addrs.split(';') {
            let (addr0,addr1) = addrs.split(',');
            data_addresses.push((addr0.parse().expect("Unable to parse socket address"),addr1.parse().expect("Unable to parse socket address")));
        }
        let control_addrs = args.value_of("control-addresses").unwrap();
        let mut control_addresses: Vec<SocketAddr> = Vec::new();
        for addrs in control_addrs.split(';') {
            let (addr0,addr1) = addrs.split(',');
            control_addresses.push((addr0.parse().expect("Unable to parse socket address"),addr1.parse().expect("Unable to parse socket address")));
        }
        assert_eq!(
            data_addresses.len(),
            control_addresses.len(),
            "Each node must have 1 data address and 1 control address"
        );
        let (device0,device1) = args.value_of("device").unwrap().split(',');
        let devices = (device0,device1);
        let natures_ini = args.value_of("natures").unwrap();
        let mut nature0:InterfaceNature = InterfaceNature(0,0,0,0);
        let mut nature1:InterfaceNature = InterfaceNature(0,0,0,0);
        let (n0,n1) = natures_ini.split(';');
        (nature0.0,nature0.1,nature0.2,nature0.3) = n0.split(',');
        (nature1.0,nature1.1,nature1.2,nature1.3) = n1.split(',');
        let natures = (nature0,nature1);
        let node_index = args
            .value_of("index")
            .unwrap()
            .parse()
            .expect("Unable to parse node index");
        assert!(
            node_index < data_addresses.len(),
            "Node index is larger than number of available nodes"
        );
        let graph_filename_arg = args.value_of("graph-filename").unwrap();
        let graph_filename = if graph_filename_arg.is_empty() {
            None
        } else {
            Some(graph_filename_arg.to_string())
        };
        let log_level = match args.occurrences_of("verbose") {
            0 => None,
            1 => Some(Level::WARN),
            2 => Some(Level::INFO),
            3 => Some(Level::DEBUG),
            _ => Some(Level::TRACE),
        };

        Self {
            index: node_index,
            num_threads,
            data_addresses,
            control_addresses,
            devices,
            natures,
            graph_filename,
            logging_level: log_level,
        }
    }

    /// Upon executing, exports the dataflow graph as a
    /// [DOT file](https://en.wikipedia.org/wiki/DOT_(graph_description_language)).
    pub fn export_dataflow_graph(mut self, filename: &str) -> Self {
        self.graph_filename = Some(filename.to_string());
        self
    }

    /// Sets the logging level.
    pub fn with_logging_level(mut self, level: Level) -> Self {
        self.logging_level = Some(level);
        self
    }

    /// ERDOS will not initialize a logger if this method is called.
    pub fn disable_logger(mut self) -> Self {
        self.logging_level = None;
        self
    }
}
