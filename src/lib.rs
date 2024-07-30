pub mod network;
pub mod server;
pub mod storage;

/// Helper function to parse the IP address delimited by ':' and return tuple of (ip, port)
fn parse_ip_address(addr: &str) -> (&str, &str) {
    let tokens = addr.split(':').collect::<Vec<&str>>();
    (tokens[0], tokens[1])
}
