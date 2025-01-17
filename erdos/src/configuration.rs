use std::net::SocketAddr;

use tracing::Level;

use tokio::net::InterfaceNature;

use crate::node::NodeId;

/// Stores the configuration parameters of a [`node`](crate::node::Node).
#[derive(Clone, Debug)]
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
    pub devices: (String,String),
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
        devices: (String,String),
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
        let mut data_addresses: Vec<(SocketAddr,SocketAddr)> = Vec::new();
        for addrs in data_addrs.split(';') {
            let mut tmp_addrs = addrs.split(',');
            data_addresses.push((tmp_addrs.next().unwrap().parse().expect("Unable to parse socket address"),tmp_addrs.next().unwrap().parse().expect("Unable to parse socket address")));
        }
        let control_addrs = args.value_of("control-addresses").unwrap();
        let mut control_addresses: Vec<(SocketAddr,SocketAddr)> = Vec::new();
        for addrs in control_addrs.split(';') {
            let mut tmp_addrs = addrs.split(',');
            control_addresses.push((tmp_addrs.next().unwrap().parse().expect("Unable to parse socket address"),tmp_addrs.next().unwrap().parse().expect("Unable to parse socket address")));
        }
        assert_eq!(
            data_addresses.len(),
            control_addresses.len(),
            "Each node must have 1 data address and 1 control address"
        );
        let mut tmp_devices = args.value_of("devices").unwrap().split(',');
        let devices = (tmp_devices.next().unwrap().to_string(),tmp_devices.next().unwrap().to_string());
        let natures_ini = args.value_of("natures").unwrap();
        let mut tmp_nature = natures_ini.split(';');
        let (mut n0,mut n1) = (tmp_nature.next().unwrap().split(','),tmp_nature.next().unwrap().split(','));
        let nature0:InterfaceNature = InterfaceNature{delay:n0.next().unwrap().parse().expect("Unable to parse number of nature"),bandwidth:n0.next().unwrap().parse().expect("Unable to parse number of nature"),reliability:n0.next().unwrap().parse().expect("Unable to parse number of nature"),security:n0.next().unwrap().parse().expect("Unable to parse number of nature")};
        let nature1:InterfaceNature = InterfaceNature{delay:n1.next().unwrap().parse().expect("Unable to parse number of nature"),bandwidth:n1.next().unwrap().parse().expect("Unable to parse number of nature"),reliability:n1.next().unwrap().parse().expect("Unable to parse number of nature"),security:n1.next().unwrap().parse().expect("Unable to parse number of nature")};
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
