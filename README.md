# Lesson 0: The Raw Event Loop

In this first lesson, we strip away the HTTP layer to understand the heart of Pingora: the **Event Loop**.

Pingora is not just an HTTP proxy; it is a generic network server framework. At its lowest level, it manages the lifecycle of a server process, handles configuration, daemonization, and graceful shutdowns. It then delegates the actual handling of traffic to **Services**.

We will build a raw TCP Echo Server. This requires implementing the `ServerApp` trait, which gives us direct access to the underlying TCP stream before any protocol parsing occurs.

### Key Concepts

1. **`Server`**: The process manager. It owns the main thread, handles signals (like SIGTERM), and manages the worker threads.
2. **`Service`**: A background worker or a listening endpoint. A `Server` can run multiple `Services`.
3. **`ServerApp`**: The logic trait. You implement this to define *what* happens when a new connection is established.
4. **`Stream`**: A wrapper around the raw socket (TCP or Unix Domain Socket). It implements `AsyncRead` and `AsyncWrite`.

### The Code (`examples/00_basic_server.rs`)

We will implement a struct `EchoApp`. When a client connects, `EchoApp` will read bytes from the stream and immediately write them back until the client disconnects or the server shuts down.

Notice the strict error handling. We avoid `unwrap()`. If the server fails to initialize or a socket read fails, we log the error and exit the scope gracefully.

We also import `GetSocketDigest` to access metadata about the connection, such as the peer's IP address.

```rust
// examples/00_basic_server.rs

use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::protocols::Stream;
// We need this trait to access connection metadata (IPs, etc.) from the Stream
use pingora::protocols::GetSocketDigest;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::listening::Service;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A custom application logic that implements `ServerApp`.
/// This is the lowest level of logic in Pingora, dealing with raw streams.
#[derive(Clone)]
pub struct EchoApp;

#[async_trait]
impl pingora::apps::ServerApp for EchoApp {
   /// This method is called whenever a new TCP connection is established.
   ///
   /// # Arguments
   /// * `stream` - The raw TCP/Unix stream (Box<dyn IO>).
   /// * `shutdown` - A watcher to check if the server is requested to stop.
   ///
   /// # Returns
   /// * `Some(stream)`: The connection is reusable and should be kept alive.
   /// * `None`: The connection is finished or errored and should be closed.
   async fn process_new(
      self: &Arc<Self>,
      mut stream: Stream,
      shutdown: &ShutdownWatch,
   ) -> Option<Stream> {
      // Access the socket digest to get the peer address.
      // We safely check if the digest exists and if the peer address is available.
      if let Some(digest) = stream.get_socket_digest() {
         if let Some(peer_addr) = digest.peer_addr() {
            info!("New connection from: {:?}", peer_addr);
         }
      }

      let mut buf = [0; 1024];

      loop {
         // 1. Graceful Shutdown Check
         // We check this every loop to ensure we don't hold connections hostage
         // during a server restart or shutdown.
         if *shutdown.borrow() {
            info!("Server shutting down, closing connection");
            return None;
         }

         // 2. Read data from the stream safely
         let read_result = stream.read(&mut buf).await;

         match read_result {
            Ok(0) => {
               // 0 bytes read indicates the client closed the connection cleanly.
               info!("Client closed connection");
               return None;
            }
            Ok(n) => {
               // We successfully read n bytes. Now echo them back.
               // write_all ensures every byte in the buffer is transmitted.
               if let Err(e) = stream.write_all(&buf[0..n]).await {
                  error!("Failed to write to stream: {}", e);
                  return None;
               }

               // Flush ensures the data is actually sent over the wire immediately.
               if let Err(e) = stream.flush().await {
                  error!("Failed to flush stream: {}", e);
                  return None;
               }
            }
            Err(e) => {
               // IO errors (broken pipe, reset, etc.) happen here.
               error!("Stream read error: {}", e);
               return None;
            }
         }
      }
   }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
   // 1. Initialize logging. Pingora uses `log`, so we need an implementation like `env_logger`.
   env_logger::init();

   // 2. Parse command line options safely.
   // Pingora provides a built-in Clap parser for standard options (-c, -d, --upgrade).
   let opt = Opt::parse_args();

   // 3. Create the Server instance.
   // This handles process lifecycle, PID files, and configuration loading.
   // We propagate the error up instead of unwrapping.
   let mut my_server = Server::new(Some(opt))?;

   // 4. Bootstrap initializes the environment (e.g., file descriptor inheritance for upgrades).
   my_server.bootstrap();

   // 5. Initialize our custom logic.
   let echo_logic = EchoApp;

   // 6. Create a Listening Service.
   // We wrap our logic in a Service which binds it to a specific protocol/port.
   let mut service = Service::new("Echo Service".to_string(), echo_logic);

   // 7. Add a TCP listening endpoint.
   // This tells the service to listen on 0.0.0.0:6142.
   service.add_tcp("0.0.0.0:6142");

   // 8. Register the service with the server.
   my_server.add_service(service);

   // 9. Run the server.
   // This enters the event loop and will not return until the process exits.
   info!("Starting server on 0.0.0.0:6142...");
   my_server.run_forever();
}
```

### Verification

To verify that your raw TCP server is working correctly, we will use `telnet` (since `nc` is unavailable).

1. **Run the Server**:
   Open your terminal in the project root and run the example. We enable info logs to see the connection events.
   ```bash
   RUST_LOG=info cargo run --example 00_basic_server
   ```
2. **Connect with Telnet**:
   Open a **separate** terminal window and connect to the server.
   ```bash
   telnet localhost 6142
   ```
3. **Test Echo**:
   Type `Hello Pingora` and press Enter. You should see the text echo back immediately.
   ```text
   Trying 127.0.0.1...
   Connected to localhost.
   Escape character is '^]'.
   Hello Pingora
   Hello Pingora
   ```
4. **Disconnect**:
   To exit `telnet`, press `Ctrl` + `]` (Control and right bracket), then type `close` and press Enter.
   ```text
   ^]
   telnet> close
   Connection closed.
   ```
5. **Check Logs**:
   In your first terminal, you should see logs indicating a new connection was established, and a closure log when you exited `telnet`.
