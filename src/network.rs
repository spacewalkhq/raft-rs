// organization : SpacewalkHq
// License : MIT License

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::join_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::error::NetworkError::ConnectionClosedError;
use crate::error::Result;
use crate::error::{Error, NetworkError};

#[async_trait]
pub trait NetworkLayer: Send + Sync {
    async fn send(&self, address: &SocketAddr, data: &[u8]) -> Result<()>;
    async fn receive(&self) -> Result<Vec<u8>>;
    async fn broadcast(&self, data: &[u8], addresses: &[SocketAddr]) -> Result<()>;
    async fn open(&self) -> Result<()>;
    async fn close(self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct TCPManager {
    address: SocketAddr,
    listener: Arc<Mutex<Option<TcpListener>>>,
    is_open: Arc<Mutex<bool>>,
}

impl TCPManager {
    pub fn new(address: SocketAddr) -> Self {
        TCPManager {
            address,
            listener: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
        }
    }

    async fn async_send(data: &[u8], address: &SocketAddr) -> Result<()> {
        let mut stream = TcpStream::connect(address).await.map_err(Error::Io)?;
        stream.write_all(data).await.map_err(Error::Io)?;
        Ok(())
    }

    async fn handle_receive(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        let listener = self.listener.lock().await;
        if let Some(listener) = &*listener {
            let (mut stream, _) = listener.accept().await.map_err(Error::Io)?;
            let mut buffer = Vec::new();
            let mut reader = tokio::io::BufReader::new(&mut stream);
            reader.read_to_end(&mut buffer).await.map_err(Error::Io)?;
            data = buffer;
        }
        Ok(data)
    }
}

#[async_trait]
impl NetworkLayer for TCPManager {
    async fn send(&self, address: &SocketAddr, data: &[u8]) -> Result<()> {
        Self::async_send(data, address).await?;
        Ok(())
    }

    async fn receive(&self) -> Result<Vec<u8>> {
        self.handle_receive().await
    }

    async fn broadcast(&self, data: &[u8], addresses: &[SocketAddr]) -> Result<()> {
        let futures = addresses
            .iter()
            .map(|address| Self::async_send(data, address));
        join_all(futures)
            .await
            .into_iter()
            .collect::<std::result::Result<_, _>>()
            // FIXME: We should let client decide what to do with the errors
            .map_err(|e| NetworkError::BroadcastError(e.to_string()))?;
        Ok(())
    }

    async fn open(&self) -> Result<()> {
        let mut is_open = self.is_open.lock().await;
        if *is_open {
            return Err(Error::Unknown("Listener is already open".into()));
        }
        let addr: SocketAddr = self.address;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|_e| NetworkError::BindError(addr))?;
        *self.listener.lock().await = Some(listener);
        *is_open = true;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        let mut is_open = self.is_open.lock().await;
        if !*is_open {
            return Err(Error::Network(ConnectionClosedError));
        }
        *self.listener.lock().await = None;
        *is_open = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use tokio::task::JoinSet;

    use crate::network::{NetworkLayer, TCPManager};

    const LOCALHOST: &str = "127.0.0.1";

    fn sock_addr(host: &str, port: u32) -> SocketAddr {
        let addr = format!("{}:{}", host, port);
        addr.parse::<SocketAddr>().unwrap()
    }

    #[tokio::test]
    async fn test_send() {
        let network = TCPManager::new(sock_addr(LOCALHOST, 8082));
        let data = vec![1, 2, 3];
        network.open().await.unwrap();
        let network_clone = network.clone();
        let handler = tokio::spawn(async move {
            let _ = network_clone.receive().await.unwrap();
        });

        let send_result = network.send(&sock_addr(LOCALHOST, 8082), &data).await;
        assert!(send_result.is_ok());

        handler.await.unwrap();
    }

    #[tokio::test]
    async fn test_send_closed_connection() {
        let network = TCPManager::new(sock_addr(LOCALHOST, 8020));
        let data = vec![1, 2, 3];
        network.open().await.unwrap();
        let network_clone = network.clone();
        tokio::spawn(async move {
            let _ = network_clone.receive().await.unwrap();
        });

        let send_result = network.send(&sock_addr(LOCALHOST, 8021), &data).await;
        assert!(send_result.is_err());
    }

    #[tokio::test]
    async fn test_receive_happy_case() {
        let network = TCPManager::new(sock_addr(LOCALHOST, 8030));
        let data = vec![1, 2, 3];
        network.open().await.unwrap();
        let network_clone = network.clone();
        let handler = tokio::spawn(async move { network_clone.receive().await.unwrap() });

        network
            .send(&sock_addr(LOCALHOST, 8030), &data)
            .await
            .unwrap();
        let rx_data = handler.await.unwrap();
        assert_eq!(rx_data, data)
    }

    #[tokio::test]
    async fn test_open() {
        let network = TCPManager::new(sock_addr(LOCALHOST, 8040));
        let status = network.open().await;
        assert!(status.is_ok());
        assert!(*network.is_open.lock().await);
    }

    #[tokio::test]
    async fn test_reopen_opened_port() {
        let network = TCPManager::new(sock_addr(LOCALHOST, 8042));
        let status = network.open().await;
        assert!(status.is_ok());
        let another_network = network.clone();
        let status = another_network.open().await;
        assert!(status.is_err());
    }

    #[tokio::test]
    async fn test_close() {
        let network = TCPManager::new(sock_addr(LOCALHOST, 8046));
        let _ = network.open().await;

        let close_status = network.close().await;
        assert!(close_status.is_ok());
    }

    #[tokio::test]
    async fn test_broadcast_happy_case() {
        let data = vec![1, 2, 3, 4];
        // server which is about to broadcast data
        let broadcasting_node = TCPManager::new(sock_addr(LOCALHOST, 8050));
        broadcasting_node.open().await.unwrap();
        assert!(*broadcasting_node.is_open.lock().await);

        // vec to keep track of all other server which should be receiving data
        let mut receivers = vec![];
        // vec to keep track of the address of servers
        let mut receiver_addresses = vec![];

        for p in 8051..8060 {
            // create receiver server
            let rx = TCPManager::new(sock_addr(LOCALHOST, p));
            receiver_addresses.push(
                format!("{}:{}", LOCALHOST, p)
                    .parse::<SocketAddr>()
                    .unwrap(),
            );

            rx.open().await.unwrap();
            assert!(*rx.is_open.lock().await);
            receivers.push(rx)
        }

        let mut s = JoinSet::new();
        for rx in receivers {
            s.spawn(async move {
                let rx_data = rx.receive().await;
                assert!(rx_data.is_ok());
                // return the received data
                rx_data.unwrap()
            });
        }

        // broadcast the message
        let broadcast_result = broadcasting_node
            .broadcast(&data, &receiver_addresses)
            .await;
        assert!(broadcast_result.is_ok());

        // assert the data received on servers
        while let Some(res) = s.join_next().await {
            let rx_data = res.unwrap();
            assert_eq!(data, rx_data)
        }
    }

    #[tokio::test]
    async fn test_broadcast_some_nodes_down() {
        let data = vec![1, 2, 3, 4];
        // server which is about to broadcast data
        let broadcasting_node = TCPManager::new(sock_addr(LOCALHOST, 8061));
        broadcasting_node.open().await.unwrap();
        assert!(*broadcasting_node.is_open.lock().await);

        // vec to keep track of all servers which should be receiving data
        let mut receivers = vec![];
        // vec to keep track of the address
        let mut receiver_addresses = vec![];
        for p in 8062..8070 {
            // Create a receiver node
            let rx = TCPManager::new(sock_addr(LOCALHOST, p));
            receiver_addresses.push(
                format!("{}:{}", LOCALHOST, p)
                    .parse::<SocketAddr>()
                    .unwrap(),
            );
            // open connection for half server
            // mocking rest half to be down
            if p & 1 == 1 {
                rx.open().await.unwrap();
                assert!(*rx.is_open.lock().await);
            }
            receivers.push(rx)
        }

        // broadcast the data
        let broadcast_result = broadcasting_node
            .broadcast(&data, &receiver_addresses)
            .await;
        assert!(broadcast_result.is_err());
    }
}
