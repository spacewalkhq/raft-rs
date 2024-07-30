// author : Vipul Vaibhaw
// organization : SpacewalkHq
// License : MIT License

use crate::parse_ip_address;
use async_trait::async_trait;
use futures::future::join_all;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

#[async_trait]
pub trait NetworkLayer: Send + Sync {
    async fn send(
        &self,
        address: &str,
        port: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn receive(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>>;
    async fn broadcast(
        &self,
        data: &[u8],
        addresses: Vec<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn open(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn close(&self) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Clone)]
pub struct TCPManager {
    address: String,
    port: u16,
    listener: Arc<Mutex<Option<TcpListener>>>,
    is_open: Arc<Mutex<bool>>,
}

impl TCPManager {
    pub fn new(address: String, port: u16) -> Self {
        TCPManager {
            address,
            port,
            listener: Arc::new(Mutex::new(None)),
            is_open: Arc::new(Mutex::new(false)),
        }
    }

    async fn async_send(
        data: &[u8],
        address: SocketAddr,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(data).await?;
        Ok(())
    }

    async fn handle_receive(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let mut data = Vec::new();
        let listener = self.listener.lock().await;
        if let Some(listener) = &*listener {
            let (mut stream, _) = listener.accept().await?;
            let mut buffer = Vec::new();
            let mut reader = tokio::io::BufReader::new(&mut stream);
            reader.read_to_end(&mut buffer).await?;
            data = buffer;
        }
        Ok(data)
    }
}

#[async_trait]
impl NetworkLayer for TCPManager {
    async fn send(
        &self,
        address: &str,
        port: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let addr: SocketAddr = format!("{}:{}", address, port).parse()?;
        Self::async_send(data, addr).await?;
        Ok(())
    }

    async fn receive(&self) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        self.handle_receive().await
    }

    async fn broadcast(
        &self,
        data: &[u8],
        addresses: Vec<String>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let futures = addresses.into_iter().map(|address| {
            let (ip, port) = parse_ip_address(&address);
            let addr: SocketAddr = format!("{}:{}", ip, port).parse().unwrap();
            Self::async_send(data, addr)
        });
        join_all(futures)
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut is_open = self.is_open.lock().await;
        if *is_open {
            return Err("Listener is already open".into());
        }
        let addr: SocketAddr = format!("{}:{}", self.address, self.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        *self.listener.lock().await = Some(listener);
        *is_open = true;
        println!("Listening on {}", addr);
        Ok(())
    }

    async fn close(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut is_open = self.is_open.lock().await;
        if !*is_open {
            return Err("Listener is not open".into());
        }
        *self.listener.lock().await = None;
        *is_open = false;
        println!("Listener closed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::network::{NetworkLayer, TCPManager};
    use tokio::task::JoinSet;

    const LOCALHOST: &str = "127.0.0.1";

    #[tokio::test]
    async fn test_send() {
        let network = TCPManager::new(LOCALHOST.to_string(), 8082);
        let data = vec![1, 2, 3];
        network.open().await.unwrap();
        let network_clone = network.clone();
        let handler = tokio::spawn(async move {
            loop {
                let data = network_clone.receive().await.unwrap();
                if data.is_empty() {
                    continue;
                } else {
                    assert_eq!(data, vec![1, 2, 3]);
                    break;
                }
            }
        });
        // Sending to a valid address
        let send_result = network.send(LOCALHOST, "8082", &data).await;
        assert!(send_result.is_ok());

        // Sending to a valid address
        let send_result = network.send(LOCALHOST, "8083", &data).await;
        assert!(send_result.is_err());

        handler.await.unwrap();
    }

    #[tokio::test]
    async fn test_receive() {
        let network = TCPManager::new(LOCALHOST.to_string(), 8082);
        let data = vec![1, 2, 3];
        network.open().await.unwrap();
        let network_clone = network.clone();
        let handler = tokio::spawn(async move { network_clone.receive().await.unwrap() });

        network.send(LOCALHOST, "8082", &data).await.unwrap();
        let rx_data = handler.await.unwrap();
        assert_eq!(rx_data, data)
    }

    #[tokio::test]
    async fn test_open() {
        let network = TCPManager::new(LOCALHOST.to_string(), 8081);
        let status = network.open().await;
        assert!(status.is_ok());
        assert!(*network.is_open.lock().await);
    }

    #[tokio::test]
    async fn test_close() {
        let network = TCPManager::new(LOCALHOST.to_string(), 8081);
        // Open the connection
        let _ = network.open().await;

        let close_status = network.close().await;
        assert!(close_status.is_ok());
        assert!(!*network.is_open.lock().await);
    }

    #[tokio::test]
    async fn test_broadcast() {
        let data = vec![1, 2, 3, 4];
        // Node to broadcast data
        let tx = TCPManager::new(LOCALHOST.to_string(), 8081);
        tx.open().await.unwrap();
        assert!(*tx.is_open.lock().await);

        // vec to keep track of all receiver nodes
        let mut receivers = vec![];
        // vec to keep track of address of receiver nodes
        let mut addresses = vec![];

        for p in 8082..8090 {
            // Create a receiver node
            let rx = TCPManager::new(LOCALHOST.to_string(), p);
            addresses.push(format!("{}:{}", LOCALHOST, p));
            // Open the connect
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

        // send the data to all receiver nodes
        let broadcast_result = tx.broadcast(&data, addresses).await;
        assert!(broadcast_result.is_ok());

        // asser the data received
        while let Some(res) = s.join_next().await {
            let rx_data = res.unwrap();
            assert_eq!(data, rx_data)
        }
    }
}
