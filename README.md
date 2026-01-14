# Quick Start: The "Pingora City" Lab

To ensure a consistent environment for these tutorials, we have created a deterministic network topology using Docker Compose. This "Pingora City" simulates a real-world infrastructure with multiple clients, distinct upstreams (HTTP, HTTPS, gRPC, H2C), and a dedicated development station.

## 1. Setup & Installation

**Prerequisites:** Docker and Docker Compose.

First, generate the Certificate Authority and service certificates. This populates `conf/keys/` with the TLS assets required for the advanced lessons.

```bash
# 1. Generate Certificates (Root CA, Server, Client, Upstream)
chmod +x scripts/00-setup-certs.sh
./scripts/00-setup-certs.sh

# 2. Build and Launch the City
docker compose up -d --build

```

## 2. The Developer Environment

You do not need Rust installed on your host machine. We have a dedicated `dev` container (Debian Bookworm) with a pre-configured Rust toolchain, OpenSSL, and network utilities.

**Enter the Dev Container:**

```bash
docker exec -it pingora_dev bash

```

**Verify Compilation (Run Example 05):**
Once inside, run the "Background Services" example. This acts as a smoke test to ensure `cargo` can compile the project and bind to the network.

```bash
# Inside pingora_dev
RUST_LOG=info cargo run --example 05_background_services

```

*Wait for the "Server started" log message, then press `Ctrl+C` to exit the process.*

**Verify Internal Network Reachability:**
We have provided a script to verify that the Dev station can reach all upstream services via both Static IP and DNS.

```bash
# Inside pingora_dev
./scripts/validate-dev.sh

```

*You should see green `OK` statuses for Blue, Green, Advanced (Nginx), and gRPC upstreams.*

Type `exit` to return to your host terminal.

## 3. Verification & Connectivity Test

From your **host machine**, run the `validate-others.sh` script. This automation script will:

1. Start `example 05_background_services` in the background on the Dev container.
2. Instruct **Client 1** and **Client 2** to connect to the server.
3. Verify that both clients (with distinct IPs) successfully received a response.
4. Verify the server logs to confirm traffic handling.
5. Send a `SIGTERM` to the server to test graceful shutdown.

```bash
# On Host
chmod +x scripts/validate-others.sh
./scripts/validate-others.sh

```

## 4. Network Topology & Services

The lab runs on a fixed subnet `172.28.0.0/24`. All containers mount the `conf/keys` directory to trust the local Root CA.

| Service | Hostname | Static IP | Role & Features |
| --- | --- | --- | --- |
| **Dev Station** | `dev.pingora.local` | `172.28.0.10` | **Your Workstation.** Rust toolchain, code bind-mount. Runs your Proxy. |
| **Upstream Blue** | `blue.pingora.local` | `172.28.0.20` | **Basic HTTP.** Returns "Response from BLUE" on port 8080. |
| **Upstream Green** | `green.pingora.local` | `172.28.0.21` | **Basic HTTP.** Returns "Response from GREEN" on port 8080. |
| **Advanced Upstream** | `advanced.pingora.local` | `172.28.0.22` | **Nginx.** Supports: <br><br>• Port 80: HTTP (Caching headers)<br><br>• Port 443: HTTPS<br><br>• Port 8443: Mutual TLS (mTLS)<br><br>• Port 8081: HTTP/2 Cleartext (H2C) |
| **gRPC Upstream** | `grpc.pingora.local` | `172.28.0.23` | **gRPC.** `grpcbin` server listening on TCP 9000. |
| **Client 1** | `client1.pingora.local` | `172.28.0.30` | **Traffic Generator.** Simulates User A. |
| **Client 2** | `client2.pingora.local` | `172.28.0.31` | **Traffic Generator.** Simulates User B (Useful for IP Rate Limiting). |


# Lesson 0: The Raw Event Loop

In this first lesson, we strip away the HTTP layer to understand the heart of Pingora: the **Event Loop**.

Pingora is not just an HTTP proxy; it is a generic network server framework. At its lowest level, it manages the lifecycle of a server process, handles configuration, daemonization, and graceful shutdowns. It then delegates the actual handling of traffic to **Services**.

We will build a raw TCP Echo Server. This requires implementing the `ServerApp` trait, which gives us direct access to the underlying TCP stream before any protocol parsing occurs.

## Key Concepts

1. **`Server`**: The process manager. It owns the main thread, handles signals (like SIGTERM), and manages the worker threads.
2. **`Service`**: A background worker or a listening endpoint. A `Server` can run multiple `Services`.
3. **`ServerApp`**: The logic trait. You implement this to define *what* happens when a new connection is established.
4. **`Stream`**: A wrapper around the raw socket (TCP or Unix Domain Socket). It implements `AsyncRead` and `AsyncWrite`.

## The Code (`examples/00_basic_server.rs`)

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

## Verification

To verify that your raw TCP server is working correctly, we will use `telnet`.

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

# Lesson 1: Configuration & Lifecycle

In Lesson 0, we built a server that ran with default settings. However, production services rarely run on defaults. They need to define how many worker threads to use, where to write error logs, where to store PID files, and how to handle process upgrades.

Pingora handles this "infrastructure" configuration separately from your traffic handling logic. This separation allows the framework to manage the process lifecycle (daemonization, restarts, upgrades) standardly across all Pingora applications.

## Key Concepts

1. **`Opt`**: This struct represents command-line arguments. Pingora provides a standard parser (via `clap`) that handles flags like `-c` (config file), `-d` (daemon mode), and `-u` (upgrade).
2. **`ServerConf`**: This struct holds the runtime configuration for the server process. It includes settings for:
   * **Threading**: `threads` and `work_stealing`.
   * **Process Management**: `pid_file`, `upgrade_sock`, `user`, `group`.
   * **Logging**: `error_log`.
   * **SSL/Network**: `ca_file`, `upstream_keepalive_pool_size`.
3. **`Server::new`**: This constructor is the bridge. It takes the command-line options (`Opt`), attempts to load the configuration file specified by `-c`, merges it with defaults, and returns a fully initialized `Server` instance.

## The Code (`examples/01_configuration.rs`)

In this example, we build a "dummy" server. Its only purpose is to load a configuration file and print the resulting settings to the console so we can verify that Pingora is correctly parsing our input.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::listening::Service;
use std::sync::Arc;
use pingora::protocols::Stream;

#[derive(Clone)]
pub struct ConfigDemoApp;

#[async_trait]
impl pingora::apps::ServerApp for ConfigDemoApp {
    async fn process_new(
        self: &Arc<Self>,
        _stream: Stream,
        _shutdown: &ShutdownWatch
    ) -> Option<Stream> {
        // For this lesson, we don't process traffic.
        // We return None to close the connection immediately.
        None
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize logging
    env_logger::init();

    // 2. Parse Command Line Arguments
    // This allows us to pass `-c conf/01_config.yaml` or `-d` (daemon mode)
    let opt = Opt::parse_args();

    // 3. Initialize Server with Options
    // Server::new will attempt to load the config file specified in `opt.conf`.
    // If the file is missing or invalid, this will return an Error.
    let mut my_server = Server::new(Some(opt))?;
    let conf = &my_server.configuration;

    // 4. Inspect the Loaded Configuration
    info!("--- Configuration Loaded ---");
    info!("  version: {}", conf.version);
    info!("  daemon: {}", conf.daemon);
    info!("  error_log: {:?}", conf.error_log);
    info!("  pid_file: {}", conf.pid_file);
    info!("  upgrade_sock: {}", conf.upgrade_sock);
    info!("  user: {:?}", conf.user);
    info!("  group: {:?}", conf.group);
    info!("  threads: {}", conf.threads);
    info!("  listener_tasks_per_fd: {}", conf.listener_tasks_per_fd);
    info!("  work_stealing: {}", conf.work_stealing);
    info!("  ca_file: {:?}", conf.ca_file);
    info!("  grace_period_seconds: {:?}", conf.grace_period_seconds);
    info!("  graceful_shutdown_timeout_seconds: {:?}", conf.graceful_shutdown_timeout_seconds);

    info!("  client_bind_to_ipv4: {:?}", conf.client_bind_to_ipv4);
    info!("  client_bind_to_ipv6: {:?}", conf.client_bind_to_ipv6);
    info!("  upstream_keepalive_pool_size: {}", conf.upstream_keepalive_pool_size);
    info!("  upstream_connect_offload_threadpools: {:?}", conf.upstream_connect_offload_threadpools);
    info!("  upstream_connect_offload_thread_per_pool: {:?}", conf.upstream_connect_offload_thread_per_pool);
    info!("  upstream_debug_ssl_keylog: {}", conf.upstream_debug_ssl_keylog);
    info!("  max_retries: {}", conf.max_retries);
    info!("----------------------------");

    // 5. Bootstrap the server
    my_server.bootstrap();

    // 6. Setup a dummy service (required to run the server)
    let mut service = Service::new("ConfigDemo".to_string(), ConfigDemoApp);
    service.add_tcp("0.0.0.0:6143");
    my_server.add_service(service);

    info!("Starting server. Verify the thread count in the logs above matches your YAML.");
    my_server.run_forever();
}

```

## Running the Lesson

### 1. Define a Configuration File

Create a file at `conf/01_config.yaml` with the following content. We specifically set `threads` to 2 to differentiate it from the default (which is usually 1 or the number of cores depending on environment).

```yaml
---
version: 1
threads: 2
pid_file: "/tmp/pingora_lesson_01.pid"
upgrade_sock: "/tmp/pingora_upgrade_01.sock"
error_log: "/tmp/pingora_error.log"

```

### 2. Run with Defaults

First, run without arguments. Pingora will use its internal defaults.

```bash
RUST_LOG=info cargo run --example 01_configuration

```

You should see `threads: 1` and `pid_file: /tmp/pingora.pid`.

### 3. Run with Configuration

Now, pass the configuration file.

```bash
RUST_LOG=info cargo run --example 01_configuration -- -c conf/01_config.yaml

```

You should see the values change to match your YAML file:

* `threads: 2`
* `pid_file: /tmp/pingora_lesson_01.pid`
* `error_log: Some("/tmp/pingora_error.log")`

This confirms that the `Server` has successfully bootstrapped itself using your external configuration.

# Lesson 2: Daemon Mode & Background Services

In production environments, servers are rarely run in the foreground attached to a terminal session. They run as **daemons**—background processes that survive user logouts and system restarts.

Pingora has built-in support for daemonization. It handles the low-level Unix operations required to detach from the terminal (forking, setsid), manages process ID (PID) files, and redirects standard output/error streams to log files.

This lesson also introduces the **`BackgroundService`**. Unlike the `ListeningService` from Lesson 0 (which accepts network connections), a `BackgroundService` runs an arbitrary task loop. This is useful for sidecar processes, metric exporters, or health check runners that need to live alongside your proxy logic.

## Key Concepts

1. **Daemonization Configuration**:
   * **`daemon: true`**: Tells Pingora to fork into the background.
   * **`pid_file`**: The path where the server writes its Process ID. External tools (like `systemd` or `monit`) use this to track and stop the server.
   * **`error_log`**: In daemon mode, `stdout` and `stderr` are closed. This setting redirects logs to a file so they aren't lost.
2. **`BackgroundService`**: A trait for tasks that run continuously until the server shuts down. It receives a `ShutdownWatch` to know when to exit gracefully.
3. **`background_service` Helper**: A utility function in the prelude that wraps your custom logic into a generic service container, saving you from implementing boilerplate.

## The Code (`examples/02_daemon_mode.rs`)

This example defines a `HeartbeatService` that logs a message every second. We check the `shutdown` signal in the loop to ensure we stop immediately when the server receives a `SIGTERM`.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;
use std::time::Duration;
use tokio::time::interval;

pub struct HeartbeatService;

#[async_trait]
impl BackgroundService for HeartbeatService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let mut period = interval(Duration::from_secs(1));
        info!("Heartbeat service started. PID: {}", std::process::id());

        loop {
            tokio::select! {
                // Wait for shutdown signal
                _ = shutdown.changed() => {
                    info!("Shutdown signal received. Stopping heartbeat.");
                    break;
                }
                // Or wait for the next tick
                _ = period.tick() => {
                    info!("Beep... (PID: {})", std::process::id());
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Inform the user if we are about to detach
    if my_server.configuration.daemon {
        println!("Preparing to daemonize. Logs will be redirected to: {:?}", my_server.configuration.error_log);
        println!("Check the PID file at: {}", my_server.configuration.pid_file);
    } else {
        println!("Running in foreground mode. Pass '-d' or use config file to daemonize.");
    }

    let heartbeat = HeartbeatService;
    // Helper to wrap our logic in a Service container
    let service = background_service("Heartbeat", heartbeat);

    my_server.add_service(service);
    my_server.run_forever();
}

```

## Running the Lesson

To test daemonization, we must use a configuration file, as the behavior changes significantly from the default foreground mode.

### 1. Define the Daemon Configuration

Create `conf/02_daemon.yaml`. We set `daemon: true` and define paths for logs and the PID file.

```yaml
---
version: 1
daemon: true
pid_file: "/tmp/pingora_02.pid"
error_log: "/tmp/pingora_02.log"
upgrade_sock: "/tmp/pingora_upgrade_02.sock"

```

### 2. Start the Daemon

Run the server with the configuration.

```bash
RUST_LOG=info cargo run --example 02_daemon_mode -- -c conf/02_daemon.yaml

```

The program will print "Preparing to daemonize..." and then **exit immediately**. This is expected; the parent process exits while the child process continues in the background.

### 3. Verify Background Execution

The server is now running silently. You can verify this by checking the PID file or listing processes.

```bash
# Read the PID
cat /tmp/pingora_02.pid

# Check if the process exists
ps -p $(cat /tmp/pingora_02.pid)

```

### 4. Check the Logs

Since the process is detached, you won't see "Beep..." in your terminal. Tail the log file to see the output.

```bash
tail -f /tmp/pingora_02.log

```

You should see the heartbeat messages appearing every second.

### 5. Stop the Daemon

To stop the server gracefully, send a `SIGTERM` to the process ID stored in the PID file.

```bash
kill $(cat /tmp/pingora_02.pid)

```

If you check the log file again, you should see the "Shutdown signal received" message, confirming the `ShutdownWatch` logic worked correctly.

# Lesson 3: Graceful Shutdown

In the previous lessons, stopping the server meant killing the process immediately. In a development environment, this is fine. In production, however, a hard stop is dangerous. You might interrupt a database write, corrupt a file, or drop a client connection in the middle of a request.

Pingora provides a built-in **Graceful Shutdown** mechanism to handle this. When the server receives a specific signal (usually `SIGTERM`), it doesn't exit immediately. Instead:

1. It broadcasts a shutdown event to all services.
2. It stops accepting *new* connections (if using listeners).
3. It waits for a configurable period (the `grace_period_seconds`) for services to finish their current work.
4. If the grace period expires and services are still running, it forces a shutdown.

## Key Concepts

* **`ShutdownWatch`**: This is a Tokio `watch` channel provided to every service's `start()` method. Services must monitor this to know when to stop accepting new work and begin their cleanup.
* **`grace_period_seconds`**: A setting in `ServerConf`. It defines the maximum time the server will wait for services to finish after a shutdown signal is received.
* **Signal Handling**: Pingora distinguishes between two types of shutdown:
* **Fast Shutdown (`SIGINT` / Ctrl+C)**: The server exits immediately. Use this during development or emergencies.
* **Graceful Shutdown (`SIGTERM`)**: The server enters the graceful shutdown phase described above. This is the standard signal used by deployment tools like Kubernetes or systemd.

## The Code (`examples/03_graceful_shutdown.rs`)

We will build a "Batch Job" service. It simulates processing long-running tasks that take 20 seconds to complete.

* If we used **Fast Shutdown** (Ctrl+C), the job would be cut off immediately.
* By using **Graceful Shutdown** (SIGTERM) and checking `ShutdownWatch`, the service detects the signal, stops starting *new* jobs, but finishes the *current* job before exiting.

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use std::sync::Arc;
use std::time::Duration;
use pingora::services::background::BackgroundService;
use tokio::time::sleep;


pub struct BatchJobService;

#[async_trait]
impl BackgroundService for BatchJobService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        info!("BatchJob Service started. Waiting for jobs");
        let mut job_id = 0;

        loop {
            // 1. Check before starting work
            // If shutdown is requested, we break the loop immediately so no new jobs start.
            if *shutdown.borrow() {
                info!("Shutdown requested. No new jobs will be started.");
                break;
            }

            job_id += 1;
            info!("Starting Job #{} (simulated 20s duration)...", job_id);

            // 2. Run the job with cancellation awareness
            // We use tokio::select! to listen for the shutdown signal WHILE the job is running.
            let job_duration = Duration::from_secs(20);
            tokio::select! {
                // The "Happy Path": The job finishes normally
                _ = sleep(job_duration) => {
                    info!("Job #{} completed successfully.", job_id);
                }

                // The "Shutdown Path": Signal received mid-job
                _ = shutdown.changed() => {
                    warn!("Shutdown signal received while Job #{} is running!", job_id);
                    warn!("Finishing Job #{} before exiting...", job_id);
                    
                    // 3. Simulate wrapping up critical work (e.g., flushing buffers)
                    // In a real app, this ensures we don't leave data in a corrupt state.
                    sleep(Duration::from_secs(10)).await;
                    info!("Job #{} completed gracefully during shutdown.", job_id);
                    
                    // Now we break the loop to allow the service to exit
                    break;
                }
            }

            // Brief pause between jobs
            sleep(Duration::from_secs(1)).await;
        }
        info!("BatchJob Service has stopped cleanly.");
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;

    // 4. Configure the Grace Period
    // We set this to 10 seconds. Pingora will wait up to this long for
    // BatchJobService to exit. If we didn't set this, the server might exit
    // before our cleanup logic finishes.
    if let Some(conf) = Arc::get_mut(&mut my_server.configuration) {
        conf.grace_period_seconds = Some(10);
    }
    my_server.bootstrap();

    let service = background_service("BatchJobService", BatchJobService);
    my_server.add_service(service);

    info!("Server running. Send SIGTERM to trigger graceful shutdown (e.g. 'pkill -TERM -f 03_graceful_shutdown').");
    my_server.run_forever();
}

```

## Running the Lesson

To verify this lesson, we need to send specific signals to the process. We will test both the graceful path and the fast path.

### 1. Test Graceful Shutdown (The Happy Path)

We want to confirm that if we stop the server while a job is running, it finishes that job.

1. **Start the Server**:
   ```bash
   RUST_LOG=info cargo run --example 03_graceful_shutdown
   ```
2. **Wait for a Job to Start**:
   Watch the logs until you see `Starting Job #1...`.
3. **Send SIGTERM**:
   Open a **second terminal** window and run:
   ```bash
   pkill -TERM -f 03_graceful_shutdown
   ```
4. **Observe the Logs**:
   Back in the first terminal, you should see the shutdown sequence.
   * Pingora logs `SIGTERM received, gracefully exiting`.
   * Our service logs `Shutdown signal received... Finishing Job #1`.
   * **Crucially**, the server *waits* for the job to finish (`Job #1 completed gracefully`) before the process actually exits.

### 2. Test Fast Shutdown (The Emergency Path)

We want to confirm that we can still force-kill the server if needed.

1. **Start the Server**:
   ```bash
   RUST_LOG=info cargo run --example 03_graceful_shutdown
   ```
2. **Wait for a Job to Start**.
3. **Press** `Ctrl+C`: This sends `SIGINT`.
4. **Observe the Logs**:
   The server should exit **immediately**. You will see `SIGINT received, exiting`, but you will *not* see the "Finishing Job" or "Job completed" messages. The work was abandoned instantly.

# Lesson 4: Threading Models

Pingora offers two distinct threading models (runtimes) to execute your services. Choosing the right one is critical for performance tuning, as it dictates how your CPU cores are utilized and how tasks are scheduled.

## The Two Flavors

1. **Work Stealing (`Steal`)**:
   * **What it is:** This is the standard Tokio multi-threaded runtime behavior. All worker threads share a global queue of tasks. If one thread finishes its work early, it "steals" tasks from other busy threads.
   * **Pros:** Excellent handling of uneven workloads. If one request takes 500ms and others take 1ms, the idle threads pick up the slack, preventing the system from stalling.
   * **Cons:** Higher overhead due to synchronization (locking) between threads. This "chatter" can become a bottleneck at very high throughputs (e.g., 100k+ RPS).
   * **Default:** `true` in `ServerConf`.
2. **Shared-Nothing (`NoSteal`)**:
   * **What it is:** This is Pingora's specialized optimization. Instead of one large runtime, Pingora spawns a separate, single-threaded Tokio runtime for *each* CPU core/thread configured. Incoming connections are sharded (distributed) to these threads. Once a connection belongs to a thread, it stays there.
   * **Pros:** Zero contention. Thread A never locks Thread B. This mimics the architecture of Nginx (one worker per core) and maximizes CPU cache locality.
   * **Cons:** Susceptible to "head-of-line blocking." If Thread A gets a heavy CPU-bound job, it cannot offload pending tasks to Thread B, even if Thread B is idle.
   * **Use Case:** High-throughput proxies where request latency is uniform (IO-bound).

## The Code (`examples/04_threading_model.rs`)

In this lesson, we programmatically toggle the threading model to `NoSteal` (disabling work stealing) and set the thread count to 2.

We then spawn two background services. Because we are in `NoSteal` mode, Pingora will distribute these services across the available independent runtimes. We print the `ThreadId` to verify that they are indeed running on different OS threads without moving between them.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::time::interval;

/// A service that simply announces which thread it is running on.
pub struct ThreadReporterService;

#[async_trait]
impl BackgroundService for ThreadReporterService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        // We set the interval to 1 second so logs don't flood the console
        let mut period = interval(Duration::from_secs(1));
        
        info!("ThreadReporter started.");

        loop {
            if *shutdown.borrow() {
                break;
            }
            
            // This print will show us WHICH OS thread is executing this task.
            // In a 'Steal' runtime, this ID might change if the task moves (rare but possible).
            // In a 'NoSteal' runtime, this task is pinned to one specific thread forever.
            let thread_id = thread::current().id();
            let thread_name = thread::current().name().unwrap_or("unnamed").to_string();
            
            info!("I am running on thread: {:?} ({})", thread_id, thread_name);

            period.tick().await;
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;

    // 1. Configure the Threading Model
    // Access the configuration via Arc::get_mut to modify it before bootstrapping.
    if let Some(conf) = Arc::get_mut(&mut my_server.configuration) {
        // Set the number of worker threads to 2 so we can see concurrency.
        conf.threads = 2;
        
        // Disable work stealing. This switches Pingora to the "Shared-Nothing" model.
        // Each of the 2 threads will run its own independent single-threaded runtime.
        conf.work_stealing = false;
    }

    my_server.bootstrap();

    // 2. Add multiple services
    // We add TWO instances of the same service. 
    // In a NoSteal runtime with 2 threads, Pingora will attempt to distribute 
    // these services across the available runtimes.
    let reporter_a = background_service("Reporter-A", ThreadReporterService);
    let reporter_b = background_service("Reporter-B", ThreadReporterService);

    my_server.add_service(reporter_a);
    my_server.add_service(reporter_b);

    info!("Server starting with work_stealing = False.");
    my_server.run_forever();
}

```

## Verification

Run the server and observe the logs. You are looking for proof that two different Operating System threads are active.

1. **Run the Example**:
   ```bash
   RUST_LOG=info cargo run --example 04_threading_model
   ```
2. **Analyze the Output**:
   You should see two different `ThreadId` values appearing in the logs.
   ```text
   INFO  04_threading_model > Server starting with work_stealing = False.
   INFO  04_threading_model > ThreadReporter started.
   INFO  04_threading_model > ThreadReporter started.
   INFO  04_threading_model > I am running on thread: ThreadId(2) (BG Reporter-B)
   INFO  04_threading_model > I am running on thread: ThreadId(3) (BG Reporter-A)
   ```

If `work_stealing` were enabled (default), you might see the same ThreadId for both, or the IDs swapping, depending on how Tokio schedules the tasks. With `work_stealing = false`, these tasks are rigidly pinned to their respective threads.

# Lesson 5: Background Services & Shared State

Real-world proxies rarely run in isolation. They need to report metrics, fetch dynamic configurations, or perform health checks on upstream servers. These tasks must run continuously but independently of the request-handling logic.

Pingora provides the **`BackgroundService`** trait for these scenarios. Unlike a `ListeningService` (which waits for incoming network connections), a background service runs an arbitrary loop.

In this lesson, we build a **Traffic Monitor**. It consists of two parts running in parallel:

1. **Traffic Service**: Accepts TCP connections and increments a shared counter.
2. **Metric Exporter**: A background service that wakes up every 2 seconds to read and log the current connection count.

## Key Concepts

### 1. Shared State with `Arc`

To share data between the Traffic Service (which writes) and the Exporter (which reads), we wrap our state struct in an `Arc` (Atomic Reference Counted smart pointer). This allows multiple threads to own a reference to the same memory location safely.

### 2. Atomic Operations & Memory Ordering

Since multiple threads access the `connection_count` simultaneously, we cannot use a simple `usize`. We must use `AtomicUsize`.

When reading or writing atomic variables, we must specify a **Memory Ordering**. This tells the CPU and compiler how strictly they must synchronize this operation with other memory operations. In our example, we used `Ordering::Relaxed`.

* **`Ordering::Relaxed`**: "I only care that this specific variable is updated atomically. I don't care about the order of *other* memory operations around it."
  * *Why use it here?* We are just counting numbers. If the "Exporter" sees the count update 5 nanoseconds later than it actually happened, or if it sees the updates out of perfect chronological order with unrelated variables, it doesn't matter. It is the fastest option.
* **`Ordering::SeqCst` (Sequential Consistency)**: "Every thread must see all operations in the exact same global order."
  * *Why avoid it here?* It forces heavy synchronization barriers on the CPU, slowing down performance unnecessarily for a simple counter.
* **`Ordering::Acquire` / `Release**`: Used for locks. "If I see this flag set (Acquire), I am guaranteed to see all the data you wrote before you set the flag (Release)."

### 3. `BackgroundService` Lifecycle

A background service receives a `ShutdownWatch` in its `start()` method. It is critical to check this watcher (usually via `tokio::select!`). If you ignore it, your background loop will keep running forever, preventing the server from shutting down gracefully.

## The Code (`examples/05_background_services.rs`)

We define a shared `AppState` and pass clones of it to both services. The `Traffic` service simulates handling requests by incrementing the counter and writing a response. The `MetricExporter` wakes up periodically to read that counter.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::{Server, ShutdownWatch};
use pingora::services::background::BackgroundService;
use pingora::services::listening::Service;
use pingora::protocols::Stream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::time::interval;

/// Shared state between the Traffic Service and the Background Service.
struct AppState {
    connection_count: AtomicUsize,
}

// --- 1. Traffic Handling Service ---

#[derive(Clone)]
pub struct CounterApp {
    state: Arc<AppState>,
}

#[async_trait]
impl pingora::apps::ServerApp for CounterApp {
    async fn process_new(
        self: &Arc<Self>,
        mut stream: Stream,
        _shutdown: &ShutdownWatch,
    ) -> Option<Stream> {
        // Increment the shared counter.
        // We use Relaxed because we don't rely on this value to synchronize other data.
        let count = self.state.connection_count.fetch_add(1, Ordering::Relaxed) + 1;
        
        info!("Traffic: New connection handled. Count is now {}", count);

        let response = format!("Hello! You are visitor #{}\n", count);
        let _ = stream.write_all(response.as_bytes()).await;
        
        // Return None to close the connection immediately
        None
    }
}

// --- 2. Background Metric Exporter ---

pub struct MetricExporter {
    state: Arc<AppState>,
}

#[async_trait]
impl BackgroundService for MetricExporter {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        // Run every 2 seconds
        let mut period = interval(Duration::from_secs(2));
        info!("Exporter: Service started.");

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("Exporter: Shutdown requested.");
                    break;
                }
                _ = period.tick() => {
                    // Read the shared state
                    let count = self.state.connection_count.load(Ordering::Relaxed);
                    info!("Exporter: Current Total Connections: {}", count);
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Initialize shared state
    let state = Arc::new(AppState { 
        connection_count: AtomicUsize::new(0) 
    });

    // 1. Setup the Traffic Service (Port 6145)
    let traffic_logic = CounterApp { state: state.clone() };
    let mut traffic_service = Service::new("Traffic".to_string(), traffic_logic);
    traffic_service.add_tcp("0.0.0.0:6145");

    // 2. Setup the Background Service
    let exporter_logic = MetricExporter { state: state.clone() };
    // 'background_service' is a helper that wraps our struct into a Pingora Service
    let background_service = background_service("MetricExporter", exporter_logic);

    // 3. Add both to the server
    my_server.add_service(traffic_service);
    my_server.add_service(background_service);

    info!("Server started. Traffic on port 6145. Metrics in logs.");
    my_server.run_forever();
}
```

## Verification

We will verify that both services are running and successfully communicating via the shared state. Since `nc` (Netcat) is useful for testing network services, you may need to install it if you haven't already and are operating outside the dev container:

```bash
sudo apt install netcat-traditional
```

**1. Run the Server**
Start your server with info-level logging enabled.

```bash
RUST_LOG=info cargo run --example 05_background_services
```

**2. Observe Initial Logs**
You should see the "Exporter" logging `Current Total Connections: 0` every 2 seconds. This confirms the background service is running.

```text
INFO  05_background_services > Exporter: Service started.
INFO  05_background_services > Exporter: Current Total Connections: 0
```

**3. Generate Traffic**
Open a **second terminal** and connect to the traffic port a few times. Each connection will trigger the Traffic Service.

```bash
echo "hi" | nc localhost 6145
echo "hi" | nc localhost 6145
```

**4. Observe State Update**
Back in your first terminal, you should see the "Traffic" service log the new connections. Shortly after, the "Exporter" log should automatically reflect the new count, proving that the `Arc<AppState>` is successfully sharing data between the two services.

```text
INFO  05_background_services > Traffic: New connection handled. Count is now 1
INFO  05_background_services > Traffic: New connection handled. Count is now 2
INFO  05_background_services > Exporter: Current Total Connections: 2
```

**5. Graceful Stop**
Use `pkill -TERM -f 05_background_services` (or `Ctrl+C` if you don't mind the fast shutdown) to confirm the background service exits cleanly.

```bash
pkill -TERM -f 05_background_services
```

# Module 2: The Proxy Logic

We have established the foundation of running a Pingora server. Now, we move to the core utility of the framework: **HTTP Proxying**.

**Important: The Lab Environment**
From this module onwards, all examples must be run inside the `pingora_dev` Docker container. The examples rely on the deterministic network topology of "Pingora City" to connect to upstream services (like `blue.pingora.local`) and receive traffic from clients.

If you are not inside the container yet, enter it now:

```bash
docker exec -it pingora_dev bash

```

---

# Lesson 6: The Simple Forwarder

A "Simple Forwarder" or "Dumb Proxy" is the most basic proxy implementation. It accepts a request from a downstream client and forwards it to a single, hardcoded upstream server. It does not perform load balancing, authentication, or complex routing.

This lesson introduces the **`ProxyHttp`** trait, which is the primary interface for building HTTP proxies in Pingora.

## Key Concepts

1. **`ProxyHttp` Trait**: This is the heart of any HTTP proxy service. It provides hooks into the request lifecycle (request arrival, upstream selection, response filtering, etc.).
2. **`upstream_peer()`**: This is the only *mandatory* hook you must implement (besides `new_ctx`). It tells Pingora *where* to send the current request.
3. **`HttpPeer`**: A struct defining the destination. It includes the IP/Port, whether to use TLS, and the SNI (Server Name Indication).
4. **`upstream_request_filter()`**: An optional hook that runs *after* the peer is selected but *before* the request is sent. This is the place to modify headers (e.g., setting the `Host` header).

## The Code (`examples/06_simple_forward.rs`)

We will build a proxy that listens on port `6146`. It will forward every request it receives to our lab's **Upstream Blue** (IP `172.28.0.20`, port `8080`).

We also implement `upstream_request_filter` to rewrite the `Host` header. This is a best practice; many web servers (like Nginx) will reject requests if the `Host` header does not match their configuration, even if the IP is correct.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

// 1. Define the Proxy Logic
pub struct SimpleProxy;

#[async_trait]
impl ProxyHttp for SimpleProxy {
    // Context (CTX) is per-request state. We don't need it for a simple forwarder.
    type CTX = ();

    fn new_ctx(&self) -> Self::CTX {}

    // 2. Define the Upstream Peer
    // This hook is called for EVERY request.
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        // In our lab, Upstream Blue is at this fixed IP.
        let addr = ("172.28.0.20", 8080);
        
        info!("Forwarding request to Upstream Blue ({:?})", addr);
        
        // Construct the peer. 
        // - addr: The destination IP and Port.
        // - false: Do not use TLS (Blue is a plaintext HTTP server).
        // - "blue.pingora.local": The SNI (ignored for HTTP, but required by struct).
        let peer = Box::new(HttpPeer::new(
            addr,
            false,
            "blue.pingora.local".to_string()
        ));
        Ok(peer)
    }

    // 3. Modify headers before forwarding
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<()> {
        // Rewrite the Host header to match the destination.
        // Without this, the upstream sees the Host header sent by the client 
        // (e.g., "172.28.0.10"), which might cause it to reject the request.
        let _ = upstream_request.insert_header("Host", "blue.pingora.local");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 4. Create the Service
    // `http_proxy_service` is a helper that wraps our logic in a ready-to-use Service.
    let mut my_proxy = http_proxy_service(
        &my_server.configuration,
        SimpleProxy
    );

    // 5. Configure the Listener
    my_proxy.add_tcp("0.0.0.0:6146");

    info!("Simple Proxy running on 0.0.0.0:6146 -> Forwarding to Upstream Blue");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verified this code by running the proxy in the `dev` container and generating traffic from `client_1` in a separate container.

1. **Start the Proxy (in `pingora_dev`)**:
   ```bash
   RUST_LOG=info cargo run --example 06_simple_forward
   ```
   *Output:* `Simple Proxy running on 0.0.0.0:6146 -> Forwarding to Upstream Blue`
2. **Generate Traffic (from Host)**:
   We instructed `client_1` to curl our proxy's IP (`172.28.0.10`):
   ```bash
   docker exec -it pingora_client_1 curl -v http://172.28.0.10:6146
   ```
3. **Result**:
   * **Client**: Received `200 OK` and the body `'Response from BLUE'`, confirming the traffic successfully traversed the proxy and reached the correct upstream.
   * **Proxy Logs**: Showed `Forwarding request to Upstream Blue`, confirming the `upstream_peer` hook was executed.

# Lesson 7: TLS Termination

In the previous lesson, we built a proxy that communicated over plain text (HTTP). In the modern web, this is insufficient for public-facing services. You need encryption (HTTPS) to protect data in transit.

**TLS Termination** (also known as SSL Offloading) is a pattern where the proxy handles the encrypted connection from the client, decrypts the traffic, and forwards it to the upstream service. Often, the connection to the upstream is kept as plain HTTP to save CPU cycles on the application servers, provided the internal network is secure (like our Docker bridge network).

## Key Concepts

1. **`TlsSettings`**: This struct configures the SSL/TLS stack (OpenSSL or BoringSSL). Pingora provides an `intermediate()` helper that configures a secure set of ciphers and protocols based on Mozilla's security guidelines, striking a balance between compatibility and security.
2. **`enable_h2()`**: Modern TLS listeners often negotiate the application protocol using ALPN (Application-Layer Protocol Negotiation). This setting enables **HTTP/2**, which allows multiplexing multiple requests over a single TCP connection, significantly improving performance.
3. **`add_tls_with_settings`**: Instead of `add_tcp`, we use this method to bind the service to a port. It requires the certificate and private key.
4. **End-to-End vs. Termination**: In this lesson, we terminate TLS at the proxy. The client speaks HTTPS to us, but we speak HTTP to `blue.pingora.local`.

## The Code (`examples/07_tls_termination.rs`)

We load the self-signed certificates generated by our lab environment (`server.crt` and `server.key`). We then configure the proxy to listen on port `6147`.

Notice that inside `upstream_peer`, we still pass `false` to `HttpPeer::new`. This confirms that while the *downstream* (user) connection is secure, the *upstream* (backend) connection remains plain text.

```rust
use async_trait::async_trait;
use log::{error, info};
use pingora::listeners::tls::TlsSettings;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use std::path::Path;

pub struct TlsProxy;

#[async_trait]
impl ProxyHttp for TlsProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        // We forward to Upstream Blue.
        let addr = ("172.28.0.20", 8080);
        info!("Forwarding HTTPS request to Upstream Blue ({:?})", addr);
        
        // CRITICAL: We pass 'false' here. 
        // This effectively "terminates" the TLS. We decrypted the traffic,
        // and now we are sending it as plain HTTP to the internal backend.
        let peer = Box::new(HttpPeer::new(
            addr,
            false,
            "blue.pingora.local".to_string()
        ));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(
        &my_server.configuration,
        TlsProxy,
    );

    // 1. Locate Certificates
    // These are mounted into the dev container at /keys/
    let cert_path = "/keys/server.crt";
    let key_path = "/keys/server.key";

    if !Path::new(cert_path).exists() || !Path::new(key_path).exists() {
        error!("Certificates not found! Make sure you ran scripts/00-setup-certs.sh");
        return Err(format!("Missing keys at {}", cert_path).into());
    }

    // 2. Configure TLS
    // We use the 'intermediate' profile for best-practice security defaults.
    let mut tls_settings = TlsSettings::intermediate(cert_path, key_path)?;
    
    // Enable HTTP/2 support (ALPN)
    tls_settings.enable_h2();

    // 3. Bind the TLS Listener
    // We use port 6147 for this lesson.
    my_proxy.add_tls_with_settings("0.0.0.0:6147", None, tls_settings);

    info!("HTTPS Proxy running on 0.0.0.0:6147 -> Forwarding to Upstream Blue");
    my_server.add_service(my_proxy);;
    my_server.run_forever();
}

```

## Verification

To verify TLS termination, we need a client that trusts our lab's self-signed Certificate Authority (CA).

1. **Start the Proxy (in `pingora_dev`)**:
   ```bash
   RUST_LOG=info cargo run --example 07_tls_termination
   ```
   *Output:* `HTTPS Proxy running on 0.0.0.0:6147`
2. **Test from Client (from Host Machine)**:
   We use `curl` with the CA certificate. We also map the hostname `dev.pingora.local` to the container's IP to ensure the SSL certificate matches the domain name.
   ```bash
   docker exec -it pingora_client_1 curl -v \
     --cacert /keys/ca.crt \
     --resolve dev.pingora.local:6147:172.28.0.10 \
     https://dev.pingora.local:6147
   ```
3. **Result Analysis**:
   * **TLS Handshake**: You will see the handshake occur: `SSL connection using TLSv1.3`.
   * **ALPN**: If verified, you may see `ALPN: offers h2,http/1.1`.
   * **Response**: `Response from BLUE`.
   * **Proxy Logs**: `Forwarding HTTPS request to Upstream Blue`.

# Lesson 8: Header Manipulation

One of the most common tasks for an API Gateway is to modify traffic as it passes through. You might need to sanitize requests (removing sensitive headers like internal tokens), tag traffic (adding request IDs for tracing), or modify responses (adding security headers like CORS or `Strict-Transport-Security`).

Pingora provides specific hooks in the `ProxyHttp` trait to inspect and mutate headers at different stages of the request lifecycle.

## Key Concepts

1. **`upstream_request_filter`**: This hook runs *after* the upstream peer has been selected but *before* the request is sent to the backend. It allows you to modify the `RequestHeader`.
   * *Use cases:* Adding authentication tokens, removing client-identifying info (scrubbing), or rewriting the `Host` header.
2. **`response_filter`**: This hook runs *after* the response receives the headers from the backend but *before* the body is streamed to the client. It allows you to modify the `ResponseHeader`.
   * *Use cases:* Hiding backend server versions (`Server: nginx/1.18`), adding custom watermarks, or fixing caching headers.

## The Code (`examples/08_header_manipulation.rs`)

In this lesson, we proxy traffic to **Upstream Green** (172.28.0.21). We perform the following manipulations:

* **Request Phase**: We inject `X-Pingora-Proxy: true` so the backend knows the request came via our gateway. We also remove the `User-Agent` header to anonymize the client.
* **Response Phase**: We inject `X-Edited-By: Pingora` into the response headers so the client can verify the gateway handled the traffic.

```rust
use async_trait::async_trait;
use log::info;
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct HeaderModProxy;

#[async_trait]
impl ProxyHttp for HeaderModProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<Box<HttpPeer>> {
        let addr = ("172.28.0.21", 8080);
        let peer = Box::new(HttpPeer::new(
            addr,
            false,
            "green.pingora.local".to_string()
        ));
        Ok(peer)
    }

    // 1. Modify the REQUEST (Client -> Proxy -> Upstream)
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> pingora::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // Set the Host header to match the upstream
        upstream_request.insert_header("Host", "green.pingora.local")?;
        
        // Add a custom header for the backend to see
        upstream_request.insert_header("X-Pingora-Proxy", "true")?;
        
        // Remove the User-Agent header for privacy
        let _ = upstream_request.remove_header("User-Agent");
        
        info!("Request headers modified: Added X-Pingora-Proxy, Removed User-Agent.");
        Ok(())
    }

    // 2. Modify the RESPONSE (Upstream -> Proxy -> Client)
    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // Add a custom header so the client knows who handled this
        upstream_response.insert_header("X-Edited-By", "Pingora")?;
        
        info!("Response headers modified: Added X-Edited-By");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, HeaderModProxy);
    
    // Bind to port 6148
    my_proxy.add_tcp("0.0.0.0:6148");

    info!("Header Manipulation Proxy running on 0.0.0.0:6148 -> Forwarding to Upstream Green");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

To verify that the headers are being modified, we inspect the traffic from the client side.

1. **Start the Proxy (in `pingora_dev`)**:
   ```bash
   RUST_LOG=info cargo run --example 08_header_manipulation
   ```
   *Output:* `Header Manipulation Proxy running on 0.0.0.0:6148`
2. **Test from Client (from Host)**:
   Run `curl -v` against port `6148`. We use verbose mode to see the response headers.
   ```bash
   docker exec -it pingora_client_1 curl -v http://172.28.0.10:6148
   ```
3. **Result Analysis**:
   * **Body**: You should see `Response from GREEN`.
   * **Headers**: In the response section (lines starting with `<`), you should see our custom header:
   ```text
   < X-Edited-By: Pingora
   ```
* **Proxy Logs**: The console running the proxy will confirm the hooks executed:
   ```text
   INFO  Request headers modified: Added X-Pingora-Proxy, Removed User-Agent.
   INFO  Response headers modified: Added X-Edited-By
   ```

# Lesson 9: Path Routing

In previous lessons, we blindly forwarded every request to a single destination. In reality, proxies act as traffic routers, dispatching requests to different microservices based on the URL path, HTTP method, or headers.

This lesson introduces two critical architectural concepts in Pingora:

1. **The Request Filter**: A hook that runs *early* in the lifecycle to validate requests or make routing decisions.
2. **The Context (`CTX`)**: A mechanism to share state between different phases of a request (e.g., passing the routing decision from the "Filter" phase to the "Peer Selection" phase).

## Key Concepts

1. **`request_filter`**: This hook runs immediately after the proxy receives the request headers from the client. It returns a `Result<bool>`.
   * If it returns `Ok(false)`: Pingora continues to the next phase (upstream peer selection).
   * If it returns `Ok(true)`: Pingora assumes the request has been fully handled (e.g., you sent a 404 error response manually) and stops processing.
2. **Context (`CTX`)**: The `ProxyHttp` trait has an associated type `CTX`. This is your custom state object created via `new_ctx()` for every new request.
   * In simple proxies, this is `()`.
   * In routing proxies, we use it to store decisions (like `Option<Target>`) so subsequent hooks (like `upstream_peer` or `upstream_request_filter`) know what to do.

## The Code (`examples/09_path_routing.rs`)

We define a simple `Target` enum to represent our microservices.

* **`/blue`** -> Routes to Upstream Blue.
* **`/green`** -> Routes to Upstream Green.
* **Other** -> Returns a 404 error immediately.

```rust
use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

// 1. Define an Enum to track our routing decision
// This will be stored in the Request Context (CTX)
#[derive(Debug, Clone, Copy)]
pub enum Target {
    Blue,
    Green,
}

pub struct PathRouter;

#[async_trait]
impl ProxyHttp for PathRouter {
    // 2. Define the Context Type
    // Instead of (), we now use Option<Target> to store our decision.
    type CTX = Option<Target>;

    fn new_ctx(&self) -> Self::CTX {
        None
    }

    // 3. Request Filter: The Gatekeeper
    // We check the path *before* picking a peer.
    async fn request_filter(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> pingora::Result<bool> {
        let path = session.req_header().uri.path();

        if path.starts_with("/blue") {
            *ctx = Some(Target::Blue);
        } else if path.starts_with("/green") {
            *ctx = Some(Target::Green);
        } else {
            // Unknown path: Return 404 immediately
            let _ = session.respond_error(404).await;
            // Return true to tell Pingora "we handled this, stop processing".
            return Ok(true)
        }
        
        // Return false to continue to the next phase (upstream_peer)
        Ok(false)
    }

    // 4. Upstream Peer: The Router
    // We read the decision made in request_filter to pick the IP.
    async fn upstream_peer(
        &self, session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let target = ctx.expect("Context should be set by request_filter");

        let (addr, sni) = match target {
            Target::Blue => (("172.28.0.20", 8080), "blue.pingora.local"),
            Target::Green => (("172.28.0.21", 8080), "green.pingora.local"),
        };

        info!("Routing request to {:?} based on path", target);
        let peer = Box::new(HttpPeer::new(addr, false, sni.to_string()));
        Ok(peer)
    }

    // 5. Upstream Request Filter: The Modifier
    // We rewrite the Host header to match the chosen upstream.
    async fn upstream_request_filter(
        &self, _session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        let target = ctx.expect("Context should be set");
        let host = match target {
            Target::Blue => "blue.pingora.local",
            Target::Green => "green.pingora.local",
        };

        upstream_request.insert_header("Host", host)?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, PathRouter);
    my_proxy.add_tcp("0.0.0.0:6149");

    info!("Path Router running on 0.0.0.0:6149");
    info!("Try: curl http://127.0.0.1:6149/blue or /green");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verified this routing logic by sending requests to different paths and observing the responses.

1. **Start the Proxy (in `pingora_dev`)**:
   ```bash
   RUST_LOG=info cargo run --example 09_path_routing
   ```
   *Output:* `Path Router running on 0.0.0.0:6149`
2. **Test Blue Route**:
   ```bash
   docker exec -it pingora_client_1 curl -v http://172.28.0.10:6149/blue
   ```
   *Result:* `200 OK` and `'Response from BLUE'`.
3. **Test Green Route**:
   ```bash
   docker exec -it pingora_client_1 curl -v http://172.28.0.10:6149/green
   ```
   *Result:* `200 OK` and `'Response from GREEN'`.
4. **Test Invalid Route (404)**:
   ```bash
   docker exec -it pingora_client_1 curl -v http://172.28.0.10:6149/invalid
   ```
   *Result:* `404 Not Found` (Pingora Default Error Page).

# Lesson 10: Query Params

Modifying the request URI—specifically the query string—is a frequent requirement for edge proxies. Common use cases include:

1. **Cache Normalization**: Reordering parameters or removing volatile ones (like `utm_source` or `fbclid`) so that requests map to the same cache key.
2. **Security**: Stripping internal debug flags or administrative parameters before they reach the backend.
3. **Analytics Tagging**: Injecting a source identifier (e.g., `ref=gateway`) so the upstream knows the request passed through the proxy.

## Key Concepts

* **`upstream_request.uri`**: Accessing the URI within the `upstream_request_filter` hook allows us to inspect the path and query string.
* **URI Immutability**: The `http::Uri` type is immutable. To modify it, we typically extract the string components, manipulate them, parse a new `Uri` object, and then use `upstream_request.set_uri()`.
* **Robust Parsing**: In production code, it is recommended to use the `url` crate for complex parsing (decoding percent-encoding, handling edge cases). For simple string replacement, standard string manipulation works fine.

## The Code (`examples/10_query_params.rs`)

In this lesson, we manipulate the URI string directly in `upstream_request_filter`. We perform two actions:

1. **Security**: Remove any parameter starting with `debug=` (e.g., preventing `debug=true` from triggering verbose backend logs).
2. **Tagging**: Append `ref=pingora` to every request.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use http::uri::Uri;

pub struct QueryModeProxy;

#[async_trait]
impl ProxyHttp for QueryModeProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(
            ("172.28.0.20", 8080),
            false,
            "blue.pingora.local".to_string(),
        ));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self, _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // 1. Access the URI parts
        let uri = &upstream_request.uri;
        let path = uri.path();
        let query = uri.query().unwrap_or("");

        info!("Original Query: '{}'", query);

        // 2. Manipulate the Query String
        // We filter OUT "debug=..." and append "ref=pingora"
        let mut params: Vec<&str> = query.split("&")
            .filter(|part| !part.is_empty() && !part.starts_with("debug="))
            .collect();
        
        params.push("ref=pingora");
        let new_query = params.join("&");

        // 3. Construct and parse the new URI
        let new_uri_string = format!("{}?{}", path, new_query);
        let new_uri: Uri = new_uri_string.parse().expect("Failed to parse new URI");

        info!("Rewritten URI: {}", new_uri);

        // 4. Update the request
        upstream_request.set_uri(new_uri);
        upstream_request.insert_header("Host", "blue.pingora.local")?;

        Ok(())
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, QueryModeProxy);
    my_proxy.add_tcp("0.0.0.0:6150");

    info!("Query Param Proxy running on 0.0.0.0:6150");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify this by sending a request containing the forbidden `debug` parameter and observing the logs to confirm it was removed and replaced.

1. **Start the Proxy (in `pingora_dev`)**:
   ```bash
   RUST_LOG=info cargo run --example 10_query_params
   ```
   *Output:* `Query Param Proxy running on 0.0.0.0:6150`
2. **Send a Request (from Host)**:
   We construct a URL with a mix of parameters:
   ```bash
   docker exec -it pingora_client_1 curl -v "http://172.28.0.10:6150/search?q=rust&debug=true&sort=asc"
   ```
3. **Result Analysis**:
   * **Proxy Logs**: You should see the transformation happening. The `debug=true` segment is dropped, and `ref=pingora` is appended.
   ```text
   INFO  Original Query: 'q=rust&debug=true&sort=asc'
   INFO  Rewritten URI: /search?q=rust&sort=asc&ref=pingora
   ```
   * **Upstream Behavior**: If you inspect the `blue` container logs (or if the echo server reflected the query string), you would see it received the sanitized version.

# Lesson 11: Response Modification

Just as we can modify requests before they reach the upstream, we can intercept and modify the response coming back from the backend before it reaches the client.

This is critical for:

1. **Security**: Adding headers like `HSTS`, `X-Frame-Options`, or `Content-Security-Policy` (CSP) centrally, rather than configuring them on every backend service.
2. **Privacy**: Stripping headers that leak internal implementation details (e.g., removing `X-Powered-By` or internal version numbers).
3. **Legacy Compatibility**: Renaming or duplicating headers to satisfy old client applications.

## Key Concepts

* **`response_filter`**: The `ProxyHttp` hook that runs after the upstream has responded with headers, but before the body is streamed.
* **`ResponseHeader`**: The struct representing the response. It behaves similarly to `RequestHeader`, allowing you to `insert`, `remove`, or `get` headers.
* **Header Handling**: Headers in HTTP are technically multi-valued. When using `.get()`, you receive the first value. If you need to handle multiple values (like multiple `Set-Cookie` headers), you would iterate over them, though simple insertion/removal is the most common use case.

## The Code (`examples/11_response_modification.rs`)

We configure the proxy to forward traffic to **Upstream Blue**. On the return trip, we perform three operations:

1. **Strip** the `X-App-Version` header to hide the backend version.
2. **Inject** `X-Content-Type-Options: nosniff` to prevent browsers from MIME-sniffing the response.
3. **Duplicate** the `Date` header into `X-Legacy-Date` to simulate supporting a legacy client that expects this specific header name.

```rust
use async_trait::async_trait;
use log::info;
use pingora::http::ResponseHeader;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct ResponseModProxy;

#[async_trait]
impl ProxyHttp for ResponseModProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(
            ("172.28.0.20", 8080),
            false,
            "blue.pingora.local".to_string(),
        ));
        Ok(peer)
    }

    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // 1. Remove a header to hide backend details
        let _ = upstream_response.remove_header("X-App-Version");

        // 2. Add a security header
        upstream_response.insert_header("X-Content-Type-Options", "nosniff")?;

        // 3. Copy/Rename a header
        // We retrieve the 'Date' header and insert it as 'X-Legacy-Date'.
        if let Some(date_val) = upstream_response.headers.get("Date") {
            // We clone the bytes because insert_header takes ownership
            let val_bytes = date_val.as_bytes().to_vec();
            upstream_response.insert_header("X-Legacy-Date", val_bytes)?;
        }

        info!("Response filtered. Stripped Version. Added Security Headers.");
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, ResponseModProxy);
    my_proxy.add_tcp("0.0.0.0:6151");

    info!("Response Mod Proxy running on 0.0.0.0:6151");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}

```

## Verification

We verified the logic by inspecting the HTTP headers using `curl`.

1. **Start the Proxy (in `pingora_dev`)**:
   ```bash
   RUST_LOG=info cargo run --example 11_response_modification
   ```
2. **Test from Client (from Host)**:
   ```bash
   docker exec -it pingora_client_1 curl -v http://172.28.0.10:6151
   ```
3. **Result Analysis**:
   * **Removed**: The output showed `< X-App-Name: http-echo`, but `X-App-Version` was successfully absent (it is normally present in the echo server response).
   * **Added**: The line `< X-Content-Type-Options: nosniff` appeared.
   * **Copied**: The line `< X-Legacy-Date: ...` appeared with the exact timestamp as the standard `Date` header.

# Lesson 12: Body Inspection

Validating the request *body* is a powerful capability for an edge proxy. While standard Load Balancers often just route based on headers, a sophisticated proxy can act as a **WAF** (Web Application Firewall), inspecting payloads for SQL injection, malware signatures, or prohibited keywords.

However, inspecting bodies in a proxy is challenging because proxies are typically **streaming** by default to maintain high performance and low memory usage. To inspect the body, we often need to buffer it (hold it in memory), which creates trade-offs between security and resource consumption.

## Key Concepts

* **`request_body_filter`**: This hook is called iteratively for every chunk of data the client uploads. It provides a `&mut Option<Bytes>`, which allows you to inspect, modify, or reject the chunk before it is passed to the upstream.
* **Streaming vs. Buffering**:
* **Streaming**: Data flows `Client -> Proxy -> Upstream` immediately. Good for speed, bad for inspection (you might send half a malicious payload before detecting it).
* **Buffering**: Data is held in the Proxy's `CTX` until a condition is met. In this lesson, we buffer chunks into a `Vec<u8>` to perform a string check.
* **Safety**: When inspecting bodies, you **must** enforce size limits (e.g., stopping after 1MB). Otherwise, a client could exhaust your proxy's RAM by sending an infinite stream of data.

## The Code (`examples/12_body_inspection.rs`)

We define a `BodyCtx` struct to hold our inspection buffer. We then implement `request_body_filter` to accumulate incoming bytes and scan for the forbidden keyword `"rogue"`. If detected, we return a custom error, which immediately aborts the connection.

```rust
use async_trait::async_trait;
use bytes::Bytes;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct BodyInspector;

// 1. Define the Context
// We use a simple buffer (Vec<u8>) to accumulate the body for inspection.
pub struct BodyCtx {
    buffer: Vec<u8>,
}

#[async_trait]
impl ProxyHttp for BodyInspector {
    type CTX = BodyCtx;

    // Initialize the empty buffer for each request
    fn new_ctx(&self) -> Self::CTX {
        BodyCtx { buffer: Vec::new() }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(
            ("172.28.0.20", 8080),
            false,
            "blue.pingora.local".to_string(),
        ));
        Ok(peer)
    }

    // 2. Request Body Filter
    // This runs for EVERY chunk of the request body.
    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // If there is data in this chunk...
        if let Some(bytes) = body {
            // ...append it to our inspection buffer.
            // Note: In production, enforce a size limit (e.g. 1MB) to prevent memory DoS.
            ctx.buffer.extend_from_slice(bytes);

            // Check for the forbidden pattern
            // We use String::from_utf8_lossy to handle potential binary data safely.
            let content = String::from_utf8_lossy(&ctx.buffer);
            
            if content.contains("rogue") {
                warn!("Security Alert: Forbidden content 'rogue' detected in body!");
                // Returning an error here immediately aborts the proxy session.
                return Err(pingora::Error::new(ErrorType::Custom("SecurityPolicyViolation")));
            }
        }
        
        // If we didn't find the keyword, we allow the chunk to proceed.
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, BodyInspector);
    my_proxy.add_tcp("0.0.0.0:6152");

    info!("Body Inspector running on 0.0.0.0:6152");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}

```

## Verification

To verify the "WAF" functionality, we send both compliant and non-compliant POST requests using `curl`.

### 1. Start the Proxy

Run the example inside the `pingora_dev` container:

```bash
RUST_LOG=info cargo run --example 12_body_inspection
```

### 2. Test Clean Request

From the host machine, send a harmless payload:

```bash
docker exec -it pingora_client_1 curl -v -X POST -d "Hello World" http://172.28.0.10:6152
```

*Result:* `200 OK` with body `'Response from BLUE'`.

### 3. Test Forbidden Request

From the host machine, send a payload containing the trigger word:

```bash
docker exec -it pingora_client_1 curl -v -X POST -d "I am a rogue agent" http://172.28.0.10:6152
```

*Result:* `500 Internal Server Error` (or Connection Closed).

*Proxy Logs:*

```text
WARN  Security Alert: Forbidden content 'rogue' detected in body!
ERROR Fail to proxy: SecurityPolicyViolation ...
```

# Lesson 13: Custom Errors

In production environments, returning generic error pages (like `502 Bad Gateway` or `500 Internal Server Error`) provides a poor user experience. Modern APIs and web applications expect structured error responses—typically JSON for APIs (`{"error": "message"}`) or branded HTML pages for browsers.

Pingora provides a dedicated hook, **`fail_to_proxy`**, which acts as a global catch-all for any error that occurs during the request lifecycle. This allows you to inspect the error cause and generate a custom response before closing the connection.

## Key Concepts

* **`fail_to_proxy`**: This hook is triggered if any previous phase (e.g., `request_filter`, `upstream_peer`, `upstream_request_filter`) returns an `Err`. It replaces the default error handling logic.
* **`ErrorType::Custom`**: You can generate your own errors using `pingora::Error::new(ErrorType::Custom("MyReason"))`. This allows you to "throw" specific exceptions (like "BlockedByWAF" or "MaintenanceMode") and "catch" them in the error handler to serve specific status codes.
* **`FailToProxy` Struct**: The return type of this hook. It instructs the server on two things:
  1. `error_code`: The HTTP status code to log internally.
  2. `can_reuse_downstream`: Whether the TCP connection to the client is safe to reuse for another request (Keep-Alive). Usually, this is `false` for fatal errors.

## The Code (`examples/13_custom_errors.rs`)

We implement a proxy that normally forwards to **Upstream Blue**. However, if the user requests the path `/oops`, we intentionally raise a custom error. We then catch this error in `fail_to_proxy` and return a structured JSON response instead of a default error page.

```rust
use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;
use pingora::proxy::FailToProxy;

pub struct CustomErrorProxy;

#[async_trait]
impl ProxyHttp for CustomErrorProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let peer = Box::new(HttpPeer::new(
            ("172.28.0.20", 8080),
            false,
            "blue.pingora.local".to_string(),
        ));
        Ok(peer)
    }

    // 1. Trigger an error intentionally
    async fn request_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        if session.req_header().uri.path() == "/oops" {
            // Raise a custom error. This immediately jumps to fail_to_proxy.
            return Err(pingora::Error::new(ErrorType::Custom("SimulatedFailure")));
        }
        Ok(false)
    }

    // 2. Handle the error (The "Catch" block)
    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &pingora::Error,
        _ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        error!("Entered fail_to_proxy with error: {:?}", e);

        // Map the internal error to an HTTP Status Code
        let code = if let ErrorType::Custom("SimulatedFailure") = e.etype {
            400 // Bad Request
        } else {
            500 // Internal Server Error
        };

        // Construct the Custom JSON Response
        let body = format!(
            r#"{{"status": "error", "code": {}, "message": "We caught a custom error!"}}"#,
            code
        );
        let content_length = body.len();

        let mut header = ResponseHeader::build(code, Some(3)).unwrap();
        header.insert_header("Content-Type", "application/json").unwrap();
        header.insert_header("Content-Length", content_length.to_string()).unwrap();

        // Write the response manually
        // - false: end_of_stream is false because body follows
        // - true: end_of_stream is true because this is the end
        let _ = session.write_response_header(Box::new(header), false).await;
        let _ = session.write_response_body(Some(body.into()), true).await;

        // Return instruction to Pingora core
        FailToProxy {
            error_code: code,
            can_reuse_downstream: false, // Close connection for safety
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, CustomErrorProxy);
    my_proxy.add_tcp("0.0.0.0:6153");

    info!("Custom Error Proxy running on 0.0.0.0:6153");
    info!("Try: curl http://127.0.0.1:6153/oops");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify both the standard success path and the custom error path.

### 1. Start the Proxy

Run the example inside the `pingora_dev` container:

```bash
RUST_LOG=info cargo run --example 13_custom_errors
```

### 2. Test Normal Request

From the host machine, request the root path:

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6153
```

*Result:* `200 OK` from Upstream Blue.

### 3. Test Custom Error

From the host machine, request the trigger path `/oops`:

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6153/oops
```

*Result:* `400 Bad Request`.
The body should be our custom JSON:

```json
{"status": "error", "code": 400, "message": "We caught a custom error!"}
```

# Lesson 14: HTTP/2 Support

HTTP/2 (H2) is a major upgrade to the HTTP protocol, introducing binary framing, header compression (HPACK), and multiplexing (multiple requests over one TCP connection).

Pingora supports HTTP/2 on both sides of the proxy:

1. **Downstream (Client → Proxy)**: Negotiated via TLS ALPN (Application-Layer Protocol Negotiation).
2. **Upstream (Proxy → Backend)**: Configured explicitly in the `HttpPeer` options.

In this lesson, we configure **End-to-End HTTP/2**. We will proxy traffic from an H2 client to our "Advanced" Nginx upstream, which is also listening on H2.

## Key Concepts

* **ALPN (Application-Layer Protocol Negotiation)**: An extension to TLS where the client sends a list of supported protocols (e.g., `h2`, `http/1.1`) during the handshake. The server selects one. To enable this in Pingora, we call `tls_settings.enable_h2()`.
* **`ALPN` Enum**: When connecting to an upstream, we must tell Pingora which protocols to offer.
  * `ALPN::H2H1`: Prefer HTTP/2, but fallback to HTTP/1.1 (safest).
  * `ALPN::H2`: Force HTTP/2.
* **`SSL_CERT_FILE`**: Pingora uses OpenSSL (via `boringssl`). It respects standard environment variables. Because our Docker container has `SSL_CERT_FILE=/keys/ca.crt` set, Pingora automatically trusts our lab's local Certificate Authority. We do *not* need to disable certificate verification manually.

## The Code (`examples/14_http2_support.rs`)

We configure the listener on port `6154` to accept H2. We configure the upstream peer to target `advanced.pingora.local:443` (our Nginx container) and offer H2.

```rust
use async_trait::async_trait;
use log::info;
use pingora::listeners::tls::TlsSettings;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::{ALPN, HttpPeer};
use std::path::Path;

pub struct Http2Proxy;

#[async_trait]
impl ProxyHttp for Http2Proxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Target the "Advanced" Nginx upstream on port 443 (HTTPS)
        let addr = ("172.28.0.22", 443);
        
        // true = Enable TLS for the upstream connection
        let mut peer = Box::new(HttpPeer::new(
            addr,
            true,
            "advanced.pingora.local".to_string(),
        ));
        
        // Offer HTTP/2 to the upstream, fallback to HTTP/1.1
        peer.options.alpn = ALPN::H2H1;

        info!("Forwarding to Upstream Advanced via HTTPS (ALPN: H2/H1)");
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // Nginx requires the Host header to match the server_name
        upstream_request.insert_header("Host", "advanced.pingora.local")?;
        Ok(())
    }
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, Http2Proxy);

    // 1. Configure Downstream TLS
    let cert_path = "/keys/server.crt";
    let key_path = "/keys/server.key";

    if !Path::new(cert_path).exists() {
        return Err(format!("Missing keys at {}", cert_path).into());
    }

    let mut tls_settings = TlsSettings::intermediate(cert_path, key_path)?;
    
    // CRITICAL: This enables H2 negotiation with the Client
    tls_settings.enable_h2();
    
    my_proxy.add_tls_with_settings("0.0.0.0:6154", None, tls_settings);

    info!("HTTP/2 Proxy running on 0.0.0.0:6154");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verify that the connection uses HTTP/2 using `curl`.

### 1. Start the Proxy

```bash
RUST_LOG=debug cargo run --example 14_http2_support
```

*Note: We use `debug` logs to see the ALPN handshake details.*

### 2. Test with Curl

From the host machine, we run `curl` inside the client container. We use the `--http2` flag to encourage H2 usage.

```bash
docker exec -it pingora_client_1 curl -v --http2 \
  --cacert /keys/ca.crt \
  https://dev.pingora.local:6154
```

### 3. Result Analysis

* **Handshake**: You should see `ALPN: server accepted h2`.
* **Protocol**: `using HTTP/2`.
* **Certificate**: `SSL certificate verify ok`. This confirms that our `SSL_CERT_FILE` environment variable correctly pointed to the CA, allowing the client to trust the proxy, and the proxy to trust the upstream.
* **Response**: `Response from Advanced Upstream (HTTPS + HTTP/2)`.

# Lesson 15: H2C (HTTP/2 Cleartext)

While HTTP/2 is almost exclusively used over HTTPS (TLS) on the public internet, the specification also defines a cleartext version known as **h2c**.

**h2c** is widely used in internal infrastructure, particularly for **gRPC** microservices running inside a secure cluster (like Kubernetes). It allows services to benefit from HTTP/2's multiplexing and binary framing without the CPU overhead of encryption/decryption at every hop.

In this lesson, we demonstrate **Protocol Translation**:

* **Downstream (Client → Proxy)**: Standard HTTP/1.1.
* **Upstream (Proxy → Backend)**: HTTP/2 Cleartext (h2c).

## Key Concepts

* **Forcing HTTP/2**: When using TLS, the protocol is negotiated via ALPN. With Cleartext, there is no handshake to negotiate the protocol. Therefore, we must explicitly tell Pingora to treat the connection as HTTP/2 immediately upon connecting.
* **`ALPN::H2`**: By setting `peer.options.alpn = ALPN::H2` combined with `tls: false`, we instruct the connection pool to start the HTTP/2 "preface" sequence immediately.
* **No Fallback**: Unlike `ALPN::H2H1`, there is no fallback mechanism here. If the upstream server does not speak H2C, the connection will fail instantly with a protocol error.

## The Code (`examples/15_h2c_support.rs`)

We configure the proxy to listen for standard HTTP traffic on port `6155`. It forwards requests to the lab's **Advanced Upstream** on port **8081**, which is configured to accept H2C traffic.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::{ALPN, HttpPeer};

pub struct H2cProxy;

#[async_trait]
impl ProxyHttp for H2cProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Target the "Advanced" Nginx upstream on port 8081 (H2C)
        let addr = ("172.28.0.22", 8081);

        // TLS is false (Cleartext)
        let mut peer = Box::new(HttpPeer::new(
            addr,
            false, 
            "advanced.pingora.local".to_string(),
        ));
        
        // We must Force H2.
        // There is no negotiation (ALPN) in Cleartext; we just send H2 frames.
        peer.options.alpn = ALPN::H2;

        info!("Forwarding to Upstream Advanced via H2C (Port 8081)");
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> 
    where
        Self::CTX: Send + Sync,
    {
        // Nginx requires the Host header to match the server block
        upstream_request.insert_header("Host", "advanced.pingora.local")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, H2cProxy);

    // We accept standard HTTP/1.1 on the front end for simplicity
    my_proxy.add_tcp("0.0.0.0:6155");

    info!("Proxy running on 0.0.0.0:6155 (HTTP/1.1) -> Forwarding to Upstream (H2C)");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verify that the proxy accepts HTTP/1.1 but receives a response from the H2C-only upstream port.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 15_h2c_support
```

### 2. Test with Curl

We use a standard `curl` command (which defaults to HTTP/1.1).

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6155
```

### 3. Result Analysis

* **Client Side**: `HTTP/1.1 200 OK`. The client (curl) spoke HTTP/1.1 to the proxy.
* **Proxy Logs**: `Forwarding to Upstream Advanced via H2C`.
* **Body**: `Response from Advanced Upstream (H2C - Cleartext HTTP/2)`. This specific body text confirms that we successfully hit the Nginx server block listening on port 8081.


# Module 3: Upstream Management

In the previous modules, we focused on the request lifecycle—how the proxy accepts, routes, and modifies traffic from the client. Now, we turn our attention to the **Upstream**: the backend services your proxy protects and serves.

A production proxy rarely talks to a single, static IP address. It must navigate dynamic environments where services scale up and down, reside behind secure TLS layers, or communicate over specialized protocols like gRPC and WebSockets.

In this module, we will explore the mechanics of connectivity. You will learn how to:

* **Discover Peers**: Switch from hardcoded IPs to dynamic DNS resolution and Unix Domain Sockets.
* **Secure Connections**: Manage TLS handshakes, SNI routing, and Mutual TLS (mTLS) authentication.
* **Handle Advanced Protocols**: Tunnel traffic via `CONNECT`, upgrade connections for WebSockets, and proxy gRPC streams.
* **Tune Performance**: optimize connection reuse (Keep-Alive) and configure granular timeouts to ensure resilience.






# Lesson 16: Static Peer

The simplest way to connect to an upstream service is by using a **Static Peer**. In this configuration, the IP address and port of the backend server are known ahead of time and do not change (or change very rarely).

While modern cloud environments often rely on dynamic service discovery, static definitions are still widely used for:

* Connecting to legacy infrastructure with fixed IPs.
* Routing traffic to local sidecars (e.g., sending to `127.0.0.1:8080`).
* Simple, high-performance setups where DNS overhead is undesirable.

## Key Concepts

* **`HttpPeer`**: This is the fundamental struct Pingora uses to represent a backend connection. It encapsulates three critical pieces of information:
  1. **Address**: A `SocketAddr` (IP + Port).
  2. **TLS Config**: A boolean flag (`true` for HTTPS, `false` for HTTP).
  3. **SNI (Server Name Indication)**: The domain name associated with the backend.
* **The Role of SNI**: Even when `use_tls` is false, providing a valid SNI string is important. Pingora often uses this string to populate the default `Host` header if the request doesn't explicitly provide one.

## The Code (`examples/16_static_peer.rs`)

We configure the proxy to route all traffic to **Upstream Blue** at the hardcoded address `172.28.0.20:8080`.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct StaticPeerProxy;

#[async_trait]
impl ProxyHttp for StaticPeerProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // 1. Define the Socket Address
        // In a static setup, this is hardcoded or loaded from a config file.
        // It accepts any type that implements ToSocketAddrs (e.g., tuple or string)
        let addr = ("172.28.0.20", 8080);

        // 2. Configure TLS
        // false = Plaintext (HTTP)
        let use_tls = false;

        // 3. Define SNI
        // Used for TLS handshake and Host header generation
        let sni = "blue.pingora.local".to_string();

        let peer = Box::new(HttpPeer::new(addr, use_tls, sni));

        info!("Connecting to static peer: {:?}", addr);
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, StaticPeerProxy);
    my_proxy.add_tcp("0.0.0.0:6160");

    info!("Static Peer Proxy running on 0.0.0.0:6160");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verify that the proxy successfully connects to the specific static IP provided.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 16_static_peer
```

### 2. Test Connection

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6160
```

### 3. Result Analysis

* **Response**: `200 OK` containing `'Response from BLUE'`.
* **Logs**: You should see the log entry `Connecting to static peer: ("172.28.0.20", 8080)`. This confirms the `upstream_peer` hook executed and selected the correct hardcoded address.

# Lesson 17: DNS Peer

In dynamic environments like Kubernetes, AWS, or Docker, backend IP addresses change frequently. Hardcoding IPs is brittle; instead, we rely on DNS to resolve service names (e.g., `blue.pingora.local`) to their current IP addresses at runtime.

### Key Concepts

* **Async Resolution**: Standard Rust DNS resolution (`std::net::ToSocketAddrs`) is **blocking**. Using it inside Pingora's async runtime will freeze the worker thread, causing massive latency spikes. You **must** use an async resolver like `tokio::net::lookup_host`.
* **Dynamic `HttpPeer`**: Instead of a static constant, we create the `HttpPeer` struct dynamically inside `upstream_peer` based on the result of the DNS lookup.
* **Error Handling**: DNS lookups can fail (NXDOMAIN, timeouts). Robust proxies must catch these errors and return appropriate internal error codes.

## The Code (`examples/17_dns_peer.rs`)

We perform an asynchronous DNS lookup for `blue.pingora.local` and use the resolved IP to construct the peer connection.

```rust
use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use tokio::net::lookup_host;

pub struct DnsPeerProxy;

#[async_trait]
impl ProxyHttp for DnsPeerProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let hostname = "blue.pingora.local";
        let port = 8080;
        let target = format!("{}:{}", hostname, port);

        info!("Resolving host: {}", target);

        // 1. Perform Async DNS Lookup
        // We use tokio::net::lookup_host to avoid blocking the runtime.
        let mut addrs = lookup_host(&target).await
            .map_err(|_e| pingora::Error::new(ErrorType::Custom("DNSResolutionFailed")))?;

        // 2. Select an Address
        // A hostname might resolve to multiple IPs. We pick the first one.
        if let Some(addr) = addrs.next() {
            info!("Resolved {} -> {}", hostname, addr);
            let peer = Box::new(HttpPeer::new(addr, false, hostname.to_string()));
            Ok(peer)
        } else {
            error!("DNS lookup returned no records for {}", hostname);
            Err(pingora::Error::new(ErrorType::Custom("DNSNoRecords")))
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, DnsPeerProxy);
    my_proxy.add_tcp("0.0.0.0:6161");

    info!("DNS Peer Proxy running on 0.0.0.0:6161");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verify that the proxy correctly resolves the internal Docker DNS name to an IP address.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 17_dns_peer
```

### 2. Test Connection

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6161
```

### 3. Result Analysis

* **Response**: `200 OK` from Upstream Blue.
* **Logs**:
   ```text
   INFO  Resolving host: blue.pingora.local:8080
   INFO  Resolved blue.pingora.local -> 172.28.0.20:8080
   ```

The logs confirm that `lookup_host` successfully returned the internal IP address `172.28.0.20`.

# Lesson 18: Unix Domain Socket (UDS) Peer

In high-performance local environments—such as communicating with a sidecar proxy (e.g., Envoy, Linkerd) or a local PHP-FPM process—using TCP/IP can introduce unnecessary overhead. **Unix Domain Sockets (UDS)** allow processes on the same machine to communicate via the kernel without touching the network stack.

Pingora supports proxying traffic to UDS endpoints natively. This is often used to offload TLS termination or WAF duties to Pingora while the application logic runs locally on a socket.

## Key Concepts

* **`HttpPeer::new_uds()`**: This constructor is distinct from the standard `new()`. It takes a file system path (e.g., `/tmp/upstream.sock`) instead of an IP address. Note that it returns a `Result`, so it must be unwrapped with `?`.
* **The "Host" in UDS**: Even though we are connecting to a file, the HTTP protocol still requires a `Host` header. You must still provide a valid SNI/Host string (e.g., `"uds.local"`).
* **Mocking in Rust**: Since our lab environment lacks a real UDS upstream (like a local Python server), we implement a mock upstream using `tokio::net::UnixListener`. Because Pingora controls the main thread, we spawn this mock server in a background thread with its own Tokio runtime.

## The Code (`examples/18_uds_peer.rs`)

We launch a background thread to act as the "Upstream Server," listening on `/tmp/upstream.sock`. The Proxy then accepts traffic on TCP port `6162` and tunnels it to that socket file.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

pub struct UdsProxy;

#[async_trait]
impl ProxyHttp for UdsProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Connect to the Unix Domain Socket file
        // Note: new_uds returns a Result, so we use '?'
        let peer = Box::new(HttpPeer::new_uds(
            "/tmp/upstream.sock",
            false, // TLS is rarely used over UDS
            "uds.local".to_string(),
        )?);

        info!("Forwarding to UDS: /tmp/upstream.sock");
        Ok(peer)
    }
}

// A simple Mock Server that listens on a Unix Socket
async fn run_mock_uds_server(path: &'static str) {
    // 1. Clean up old socket file if it exists
    let _ = std::fs::remove_file(path);

    // 2. Bind to the path
    let listener = UnixListener::bind(path).expect("Failed to bind UDS");
    info!("Mock Upstream running at {}", path);

    // 3. Accept loop
    tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _addr)) = listener.accept().await {
                tokio::spawn(async move {
                    // Simple Read (drain request)
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    
                    // Simple Write (static response)
                    let response = "HTTP/1.1 200 OK\r\nContent-Length: 13\r\nConnection: close\r\n\r\nHello via UDS";
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        }
    });
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Spawn our mock upstream server in the background
    // We use a separate thread + runtime to avoid conflicting with Pingora's main loop
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_mock_uds_server("/tmp/upstream.sock"));
        std::thread::park(); // Keep the thread alive
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, UdsProxy);
    my_proxy.add_tcp("0.0.0.0:6162");

    info!("UDS Proxy running on 0.0.0.0:6162");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We verify that the proxy can bridge a standard TCP HTTP request to a local Unix Domain Socket.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 18_uds_peer
```

### 2. Test Connection

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6162
```

### 3. Result Analysis

* **Logs**:
   ```text
   INFO  Mock Upstream running at /tmp/upstream.sock
   INFO  Forwarding to UDS: /tmp/upstream.sock
   ```
* **Response**: `200 OK` with body `Hello via UDS`.

# Lesson 19: Peer Options

In a complex microservices architecture, not all upstreams are created equal. A real-time bidding server might need to fail fast (e.g., 50ms), while a legacy reporting service might need a generous 30-second window.

Pingora allows granular control over connection parameters via the `peer.options` struct. This allows you to apply different Service Level Agreements (SLAs) dynamically per request.

## Key Concepts

* **`connection_timeout`**: The maximum time allowed to establish the TCP (and TLS) handshake.
* **`read_timeout`**: The maximum time allowed between bytes when reading the response body.
* **The "Blackhole" Pattern**: To test connection timeouts reliably in a local lab, we route traffic to `192.0.2.1`. This is a reserved "TEST-NET" IP address that is guaranteed not to route, causing the connection attempt to hang until the timeout fires.
* **`fail_to_proxy`**: Timeouts in Pingora generate specific error types (`ConnectTimedout`, `ReadTimedout`). We catch these here to return a specific `504 Gateway Timeout` status instead of a generic 502.

## The Code (`examples/19_peer_options.rs`)

We configure the proxy to switch behaviors based on the URL path:

1. **`/normal`**: Connects to the standard upstream with a 2-second timeout.
2. **`/timeout`**: Attempts to connect to the "Blackhole" IP with a **100ms** timeout. This guarantees a `ConnectTimedout` error.

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use std::time::Duration;
use pingora::proxy::FailToProxy;

pub struct TimeoutProxy;

#[async_trait]
impl ProxyHttp for TimeoutProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let path = session.req_header().uri.path();

        // 1. Determine Target based on path
        let (addr, sni, timeout) = if path == "/timeout" {
            // Case A: The "Blackhole"
            // 192.0.2.1 is a reserved Test-Net IP. It will not respond.
            // We set a 100ms timeout to fail fast.
            info!("Routing to Blackhole IP (192.0.2.1) to force timeout...");
            (("192.0.2.1", 80), "blackhole.local", Duration::from_millis(100))
        } else {
            // Case B: The Happy Path
            info!("Routing to Blue (Standard Timeout)");
            (("172.28.0.20", 8080), "blue.pingora.local", Duration::from_secs(2))
        };

        let mut peer = Box::new(HttpPeer::new(addr, false, sni.to_string()));

        // 2. Apply the specific timeout config
        peer.options.connection_timeout = Some(timeout);
        // We set generous read/write timeouts to ensure only connection time is tested
        peer.options.read_timeout = Some(Duration::from_secs(2)); 
        peer.options.write_timeout = Some(Duration::from_secs(2));

        Ok(peer)
    }

    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        _ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        // 3. Handle the Timeout Error
        match e.etype {
            ErrorType::ConnectTimedout | ErrorType::ReadTimedout => {
                warn!("Custom Error Handler: Connection Timed Out!");
                let _ = session.respond_error(504).await;
                return FailToProxy {
                    error_code: 504,
                    can_reuse_downstream: false,
                };
            }
            _ => {
                // Fallback for other errors
                warn!("Fail to proxy: {:?}", e);
                let _ = session.respond_error(502).await;
                FailToProxy{
                    error_code: 502,
                    can_reuse_downstream: false,
                }
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, TimeoutProxy);
    my_proxy.add_tcp("0.0.0.0:6163");

    info!("Timeout Config Proxy running on 0.0.0.0:6163");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

**1. Start the Proxy**

```bash
RUST_LOG=info cargo run --example 19_peer_options
```

**2. Test Normal Request**

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6163/normal
```

*Result:* `200 OK` from Blue.

**3. Test Timeout Request**

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6163/timeout
```

*Result:* After exactly 100ms, you will receive `504 Gateway Timeout`.
*Logs:* `Routing to Blackhole IP...` followed by `Custom Error Handler: Connection Timed Out!`.

# Lesson 20: SNI Routing

In modern multi-tenant architectures (like CDNs or SaaS platforms), a single backend IP address often serves thousands of different domains. To route traffic correctly, the proxy must tell the upstream server *which* domain it is trying to reach during the TLS handshake. This is done via the **SNI (Server Name Indication)** extension.

In this lesson, we will build a proxy that dynamically selects the SNI based on the request path.

## Key Concepts

* **SNI (Server Name Indication):** A TLS extension where the client (Pingora) sends the hostname it wants to connect to in the initial `ClientHello` packet. This allows the backend (Nginx) to select the correct certificate and server block.
* **Context (`CTX`)**: We use the request context to store the decision made in `request_filter` (the routing logic) so it can be retrieved later in `upstream_peer` (where the TLS connection is created).
* **Host Header vs. SNI**: While they often match, they are distinct. SNI is Layer 4 (TLS), while the Host header is Layer 7 (HTTP). To avoid confusion or rejection by the upstream, it is best practice to keep them in sync.
* **Wildcard Certificates**: In our lab, the upstream Nginx server (`advanced.pingora.local`) holds a wildcard certificate for `*.api.pingora.local`. This allows it to accept connections for both `v1.api.pingora.local` and `v2.api.pingora.local` using the same certificate.

## The Code (`examples/20_sni_routing.rs`)

We map incoming paths to different subdomains:

* `/v1/...` → `v1.api.pingora.local`
* Other → `v2.api.pingora.local`

We use the `SniCtx` struct to pass this decision from the filter phase to the peer selection phase.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;

pub struct SniRouter;

// 1. Define Context to share data between hooks
pub struct SniCtx {
    pub target_host: String,
}

#[async_trait]
impl ProxyHttp for SniRouter {
    type CTX = SniCtx;

    // Initialize the context for each new request
    fn new_ctx(&self) -> Self::CTX {
        SniCtx {
            target_host: String::new(),
        }
    }

    // 2. Determine Routing Logic early (Layer 7 Filter)
    async fn request_filter(&self, session: &mut Session, ctx: &mut Self::CTX) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let path = session.req_header().uri.path();

        // Calculate the target hostname based on path prefix
        ctx.target_host = if path.starts_with("/v1") {
            "v1.api.pingora.local".to_string()
        } else {
            "v2.api.pingora.local".to_string()
        };

        Ok(false)
    }

    // 3. Configure the Connection (Layer 4 Peer Selection)
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let addr = ("172.28.0.22", 443);
        
        // Retrieve the SNI we decided on earlier
        let sni = ctx.target_host.clone();
        
        // Create the peer with TLS enabled (true)
        // Because the upstream cert covers *.api.pingora.local, 
        // both "v1" and "v2" SNIs will be accepted and validated.
        let peer = Box::new(HttpPeer::new(addr, true, sni.clone()));
        
        info!("Connecting to IP: {:?} with SNI: {}", addr, sni);
        Ok(peer)
    }

    // 4. Sync the Host Header (Layer 7 Request Modification)
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // Ensure the Host header matches the SNI to satisfy the web server
        upstream_request.insert_header("Host", &ctx.target_host)?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, SniRouter);
    my_proxy.add_tcp("0.0.0.0:6164");

    info!("SNI Routing Proxy running on 0.0.0.0:6164");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify that Pingora connects to the same upstream IP but presents different identities (SNI) depending on the URL.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 20_sni_routing
```

### 2. Test V1 Path

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6164/v1/test
```

*Result:* `200 OK`.
*Log:* `Connecting to ... SNI: v1.api.pingora.local`

### 3. Test V2 Path

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6164/v2/test
```

*Result:* `200 OK`.
*Log:* `Connecting to ... SNI: v2.api.pingora.local`

This works seamlessly because we updated our certificate infrastructure to include `*.api.pingora.local` in the upstream certificate. Pingora successfully negotiates a secure TLS connection for both subdomains using the same backend IP.

# Lesson 21: Mutual TLS (mTLS)

Standard TLS (HTTPS) involves one-way authentication: the client validates the server's identity. **Mutual TLS (mTLS)** adds a second layer of security where the server also validates the client's identity. This is commonly used in Zero Trust architectures to ensure only authorized proxies or microservices can talk to sensitive backends.

In this lesson, we will configure Pingora to present a **Client Certificate** when connecting to a secured upstream.

## Key Concepts

* **Client Certificate (`client.crt`)**: A digital identity card for the proxy. It is signed by a Root CA trusted by the upstream.
* **Private Key (`client.key`)**: The secret key used to prove ownership of the certificate during the handshake.
* **`CertKey` Struct**: Pingora's internal wrapper for an `X509` certificate chain and a `PKey` private key.
* **`upstream_peer` Hook**: We attach the credentials to the `HttpPeer` struct dynamically. We can choose to send them for some requests (e.g., `/auth`) and omit them for others (e.g., `/anon`).
* **Performance**: Parsing certificates from PEM files is CPU-intensive. We must load them into memory **once** during startup and wrap them in an `Arc` (Atomic Reference Counted) pointer to share them efficiently across thousands of requests.

## The Code (`examples/21_mutual_tls.rs`)

We create a proxy that connects to the **Advanced Upstream** on port `8443`, which enforces mTLS.

* If the path is `/auth`, we attach the client certificate.
* If the path is `/anon`, we attempt to connect without one.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::utils::tls::CertKey;
use pingora::tls::x509::X509;
use pingora::tls::pkey::PKey;
use std::fs;
use std::sync::Arc;

// The struct holds the parsed Certificate/Key pair in an Arc.
// This is thread-safe and cheap to clone for every request.
pub struct MtlsProxy {
    client_cert: Arc<CertKey>
}

#[async_trait]
impl ProxyHttp for MtlsProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let path = session.req_header().uri.path();
        
        // Target the mTLS port (8443) on the Advanced Upstream
        let addr = ("172.28.0.22", 8443);
        let sni = "advanced.pingora.local";

        let mut peer = Box::new(HttpPeer::new(addr, true, sni.to_string()));

        // Decision Logic: To Auth or Not to Auth?
        if path == "/auth" {
            // Case 1: Authenticated
            // We attach the Arc<CertKey>. Pingora will use this in the TLS handshake.
            info!("Attaching Client Certificate for /auth request...");
            peer.client_cert_key = Some(self.client_cert.clone());
        } else {
            // Case 2: Anonymous
            // We explicitly leave client_cert_key as None.
            info!("Connecting anonymously (no cert) for {}...", path);
        }
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 1. Load Credentials at Startup (Critical for Performance)
    let cert_path = "/keys/client.crt";
    let key_path = "/keys/client.key";

    if !std::path::Path::new(cert_path).exists() {
        return Err(format!("Client keys missing at {}. Run 00-setup-certs.sh", cert_path).into());
    }

    let cert_bytes = fs::read(cert_path)?;
    let key_bytes = fs::read(key_path)?;

    // 2. Parse PEM into OpenSSL Objects
    let x509 = X509::from_pem(&cert_bytes[..])
        .map_err(|e| format!("Failed to parse certificate: {}", e))?;
    let key = PKey::private_key_from_pem(&key_bytes)
        .map_err(|e| format!("Failed to parse private key: {}", e))?;

    // 3. Wrap in CertKey and Arc
    let cert_key = CertKey::new(vec![x509], key);
    let client_cert = Arc::new(cert_key);

    let my_proxy = MtlsProxy { client_cert };

    let mut my_service = http_proxy_service(&my_server.configuration, my_proxy);
    my_service.add_tcp("0.0.0.0:6165");

    info!("mTLS Proxy running on 0.0.0.0:6165");
    my_server.add_service(my_service);
    my_server.run_forever();
}
```

## Verification

We will verify that the upstream server rejects anonymous connections but accepts authenticated ones.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 21_mutual_tls
```

### 2. Test Anonymous Access (Expected Failure)

The upstream Nginx is configured with `ssl_verify_client on`. It should reject connections that lack a certificate.

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6165/anon
```

*Result:* `400 Bad Request`.
*Body:* The HTML error page contains `<center>No required SSL certificate was sent</center>`.

### 3. Test Authenticated Access (Expected Success)

We hit the `/auth` path, causing Pingora to attach the client certificate.

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6165/auth
```

*Result:* `200 OK`.
*Body:* `Response from mTLS Protected Upstream. Hello, Authenticated Client!`

# Lesson 22: Proxy Connect (Tunneling)

## 1. Proxy Tunnels

In enterprise environments, direct internet access is rarely granted to backend servers. Instead, all outbound traffic must pass through a designated **Forward Proxy** (like Squid, Zscaler, or an AWS NAT Gateway).

If your Pingora instance is running in such a restricted environment, it cannot simply open a TCP connection to `api.stripe.com`. It must ask the corporate proxy to open that connection on its behalf. This is achieved using the HTTP **`CONNECT`** method.

In this lesson, we will implement a custom connector that allows Pingora to "tunnel" through an intermediate proxy to reach a secure upstream.

---

## 2. Understanding HTTP `CONNECT`

The `CONNECT` method is unique because it asks the proxy to stop acting like an HTTP parser and start acting like a blind pipe. This is essential for secure traffic (HTTPS), because the intermediate proxy cannot read the encrypted data—it just shovels bytes back and forth.

### The "Double Handshake"

Tunneling involves two distinct connection phases:

1. **The Setup (Plaintext):**
   The client (Pingora) connects to the **Proxy** and sends a `CONNECT` request specifying the ultimate destination.
   ```http
   CONNECT advanced.pingora.local:443 HTTP/1.1
   Host: advanced.pingora.local:443
   ```
2. **The Tunnel (Encrypted):**
   If allowed, the Proxy connects to the destination and replies `200 Connection Established`.
   At this precise moment, the connection transforms. The HTTP layer vanishes, and it becomes a raw TCP stream. Pingora then initiates the TLS handshake *through* this stream, effectively talking directly to the destination.

---

## 3. The Challenge: Pingora's Limitation

As of today, Pingora's high-level API (`peer.proxy`) is optimized for Unix Domain Sockets (UDS). It does **not** natively support configuring a standard TCP/IP address (like `10.0.0.5:3128`) as a forward proxy.

If you attempt to set `peer.proxy` to a standard HTTP proxy address, Pingora will struggle to establish the connection because it expects a local socket file path.

**The Consequence:** We cannot rely on simple configuration flags. To support this common enterprise requirement, we must extend Pingora's networking capabilities manually.

---

## 4. The Solution: Transport Layer Hijacking

To solve this, we will use Pingora's `L4Connect` trait. This powerful interface allows us to intercept the "Dialing" phase of the connection lifecycle.

We will implement a custom connector that performs a "Bait and Switch" operation:

1. **Intercept the Dial:** When Pingora asks to connect to a peer, our custom code takes over.
2. **Dial the Proxy:** Instead of connecting to the Upstream, we physically connect to the **Forward Proxy IP** (e.g., `127.0.0.1:3128`).
3. **Perform the Handshake:** We manually write the `CONNECT` headers to the socket and wait for the `200 OK` response.
4. **Return the Socket:** We hand this established tunnel back to Pingora.

Pingora's core doesn't know (or care) that the socket is tunneled. It simply sees a valid TCP stream and proceeds to perform the TLS handshake on top of it.

### The "FD Mismatch" Trap

There is one critical safety mechanism we must navigate: **File Descriptor (FD) Reuse Checks**.
Pingora checks if the connected socket's remote address matches the configured `HttpPeer` address.

* **If we configure:** Peer = `advanced.pingora.local`
* **But we connect to:** Socket = `127.0.0.1`

Pingora will detect this mismatch ("I asked for X but you gave me Y!") and close the connection to prevent security risks.

**The Fix:** We must align the physical and logical layers:

* **Physical Layer:** We configure the `HttpPeer` address to be the **Proxy's IP**. This satisfies the safety check.
* **Logical Layer:** We manually set the **SNI** and **Host Header** to the **Upstream's Domain**. This ensures the TLS handshake and HTTP request are valid for the destination.

## 5. The Code (`examples/22_proxy_connect.rs`)

This implementation is advanced because it requires us to touch three different layers of the networking stack simultaneously:

1. **Layer 4 (Transport):** We physically connect to the Proxy IP.
2. **Layer 5 (Session):** We manually negotiate the HTTP `CONNECT` tunnel.
3. **Layer 7 (Application):** We lie to the application layer, telling it we are connecting to the Upstream Host so that SNI and Host headers are generated correctly.

### The Components

* **`ProxyTunnelConnector`**: This struct implements `L4Connect`. It is the "driver" that Pingora uses when it needs to establish a TCP connection. Instead of dialing the destination, it dials the proxy, negotiates the tunnel, and returns the active socket.
* **`TunnelProxy`**: The main proxy logic. Its job is to configure the `HttpPeer` with the **Physical Address** (Proxy IP) to satisfy safety checks, while injecting our custom connector to handle the **Logical Connection** (Target Host).
* **`run_mock_forward_proxy`**: A minimal TCP proxy running in the background to simulate a corporate firewall like Zscaler or Squid.

```rust
use async_trait::async_trait;
use log::{error, info};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::connectors::L4Connect;
// We alias the specific Stream type Pingora expects for L4 connections
use pingora::protocols::l4::stream::Stream as L4Stream;
use pingora::protocols::l4::socket::SocketAddr as PingoraSocketAddr;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// --- COMPONENT 1: The Custom Connector ---
// This acts as the "Client" in the CONNECT handshake.
#[derive(Debug)]
struct ProxyTunnelConnector {
    proxy_addr: SocketAddr, // Physical: Where packets go (127.0.0.1:3128)
    remote_host: String,    // Logical: Who we want to reach (advanced.pingora.local)
    remote_port: u16,       // Logical: Port (443)
}

#[async_trait]
impl L4Connect for ProxyTunnelConnector {
    // Pingora calls this method when it wants to open a connection.
    // We ignore the `_addr` argument provided by Pingora because we are hijacking the destination.
    async fn connect(&self, _addr: &PingoraSocketAddr) -> Result<L4Stream> {
        info!("Connector: Dialing Proxy at {:?}...", self.proxy_addr);
        
        // 1. Establish Physical TCP Connection
        let mut socket = TcpStream::connect(self.proxy_addr).await.map_err(|e| {
            Error::explain(ErrorType::ConnectError, format!("Failed to connect to proxy: {}", e))
        })?;

        // 2. Perform the HTTP CONNECT Handshake
        // We construct a raw HTTP request asking the proxy to open a tunnel.
        let connect_req = format!(
            "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
            self.remote_host, self.remote_port, self.remote_host, self.remote_port
        );

        info!("Connector: Sending CONNECT request for {}:{}", self.remote_host, self.remote_port);
        socket.write_all(connect_req.as_bytes()).await.map_err(|e| {
            Error::explain(ErrorType::WriteError, format!("Failed to write CONNECT req: {}", e))
        })?;

        // 3. Verify the Tunnel
        // We must read the response headers to ensure we got a "200 Connection Established".
        let mut buf = [0u8; 4096];
        let n = socket.read(&mut buf).await.map_err(|e| {
            Error::explain(ErrorType::ReadError, format!("Failed to read proxy resp: {}", e))
        })?;

        let response = String::from_utf8_lossy(&buf[..n]);
        if !response.contains(" 200 ") {
             return Err(Error::explain(
                ErrorType::ConnectProxyFailure,
                format!("Proxy refused tunnel: {}", response.lines().next().unwrap_or("Unknown"))
            ));
        }

        info!("Connector: Tunnel established! Handing socket to Pingora core.");
        
        // 4. Handover
        // We wrap the standard Tokio TcpStream into Pingora's L4Stream wrapper.
        // Pingora's TLS layer will now take this socket and perform the SSL Handshake *over* it.
        Ok(L4Stream::from(socket))
    }
}

// --- COMPONENT 2: The Main Proxy Logic ---
pub struct TunnelProxy;

#[async_trait]
impl ProxyHttp for TunnelProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let upstream_host = "advanced.pingora.local";
        let upstream_port = 443;
        
        // The Proxy's physical address (The "Next Hop")
        let proxy_addr_str = "127.0.0.1:3128";
        let proxy_socket_addr: SocketAddr = proxy_addr_str.parse().unwrap();

        // THE CRITICAL CONFIGURATION:
        // 1. Peer Address: Set to the PROXY IP. 
        //    This prevents "FD Mismatch" errors. Pingora checks: "Is the socket connected to 
        //    the address in the peer struct?" Since our connector dials 127.0.0.1, this must match.
        // 2. SNI: Set to the UPSTREAM HOST.
        //    This ensures the ClientHello generated by Pingora asks for the correct certificate.
        let mut peer = Box::new(HttpPeer::new(
            proxy_socket_addr,        // Address matches physical socket
            true,                     // TLS matches final destination
            upstream_host.to_string() // SNI matches final destination
        ));

        // 3. Inject the Custom Connector
        // This overrides the default logic of "Just dial the Peer Address".
        let connector = ProxyTunnelConnector {
            proxy_addr: proxy_socket_addr,
            remote_host: upstream_host.to_string(),
            remote_port: upstream_port,
        };

        peer.options.custom_l4 = Some(Arc::new(connector));
        
        Ok(peer)
    }

    // 4. Override the Host Header
    // Even though we are physically talking to 127.0.0.1, the HTTP request 
    // inside the encrypted tunnel must look like it's going to the upstream.
    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "advanced.pingora.local")?;
        Ok(())
    }
}

// --- COMPONENT 3: Mock Forward Proxy (Background Task) ---
// Simulates a corporate proxy that accepts CONNECT requests.
async fn run_mock_forward_proxy() {
    let addr = "0.0.0.0:3128";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind Mock Proxy");
    info!("Mock Proxy listening on {}", addr);

    loop {
        if let Ok((mut client_socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = client_socket.read(&mut buf).await.unwrap_or(0);
                if n == 0 { return; }

                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("CONNECT") {
                    info!("Mock Proxy: Connecting to upstream_advanced...");
                    // In a real proxy, we would parse the requested host.
                    // Here we hardcode the destination for the lab.
                    if let Ok(mut upstream) = TcpStream::connect("172.28.0.22:443").await {
                        // Reply "Success" to the client
                        let _ = client_socket.write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n").await;
                        // Enter "Blind Pipe" mode
                        let _ = tokio::io::copy_bidirectional(&mut client_socket, &mut upstream).await;
                    }
                }
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Start the mock proxy in a background thread
    std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_mock_forward_proxy());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, TunnelProxy);
    my_proxy.add_tcp("0.0.0.0:6166");

    info!("Pingora Tunnel Service running on 0.0.0.0:6166");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## 6. Verification

We will confirm that traffic is actually flowing through the proxy by observing the logs from our Mock Proxy component.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 22_proxy_connect
```

### 2. Make the Request

We send a request to the Pingora listener (port 6166). We expect Pingora to transparently handle the complex tunneling.

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6166/
```

### 3. Log Analysis

Observe the `stdout` of your running Rust program. You should see the following sequence of events:

1. **`Connector: Dialing Proxy...`**: The `upstream_peer` hook executed, and our custom connector was invoked. It is connecting to `127.0.0.1:3128`.
2. **`Mock Proxy: Connecting to upstream_advanced...`**: The background thread received the connection and the `CONNECT` header.
3. **`Connector: Tunnel established!`**: The Mock Proxy replied with `200 OK`, and our connector handed the socket back to Pingora.
4. **`Response from Advanced Upstream...`**: Pingora successfully performed the TLS handshake *inside* the tunnel and retrieved the response.

### 4. Why this matters

If you attempted to use `HttpPeer::new_proxy` with a TCP address, you would have seen an error like `No such file or directory` (OS Error 2). This confirms that native support is lacking for TCP, and that our "Hijack" method is currently the correct way to implement TCP Tunneling in Pingora.























# Lesson 23: WebSocket Upgrade

## 1. WebSockets
In modern real-time applications (chat apps, live sports feeds, stock tickers), the request-response model of HTTP is often insufficient. These applications rely on **WebSockets**, which allow for persistent, bidirectional communication between the client and the server.

A WebSocket connection begins its life as a standard HTTP request but quickly "upgrades" into a raw TCP stream. In this lesson, we will configure Pingora to handle this upgrade process and maintain these long-lived connections.

## 2. How the Upgrade Works

The WebSocket protocol (RFC 6455) uses a specific "handshake" to establish the connection:

1. **Client Request:** The client sends an HTTP GET request with two special headers:
   * `Connection: Upgrade`
   * `Upgrade: websocket`
2. **Proxy Behavior:** Pingora forwards this request to the upstream.
3. **Upstream Response:** If the upstream accepts the upgrade, it replies with `101 Switching Protocols`.
4. **The Switch:** Upon receiving the 101 status, both the client and the upstream (and the proxy in the middle) stop speaking HTTP. They switch to the WebSocket binary protocol over the same underlying TCP connection.

## 3. The Critical Challenge: Timeouts

The most common issue when proxying WebSockets is premature disconnection.

* **HTTP Mindset:** HTTP proxies are designed for short bursts of activity. If a connection is idle for 60 seconds, it's usually considered dead or "stuck," and the proxy kills it to free up resources.
* **WebSocket Reality:** WebSockets are often idle for long periods (e.g., waiting for a chat message).

**The Solution:**
When configuring the `HttpPeer` for WebSocket traffic, we must explicitly **disable or extend the read/write timeouts**. If we leave the defaults (e.g., 60s), Pingora will aggressively terminate perfectly healthy WebSocket connections during quiet periods.

## 4. Setup for this Lesson

To demonstrate a complete WebSocket lifecycle without external dependencies, we will build a self-contained system in a single file:

1. **The Echo Server (Background Thread):** A simple server using `tokio-tungstenite` that accepts WebSocket connections and echoes back any text it receives with a timestamp.
2. **The Test Client (Background Thread):** A script that connects to our proxy, sends a "Ping" every second, and logs the "Pong" it receives.
3. **The Pingora Proxy:** The intermediary that routes the traffic between them.

This "Self-Driving" example allows you to verify the entire flow—handshake, message passing, and termination—just by watching the logs.

## 5. The Code (`examples/23_websocket_upgrade.rs`)

First, ensure your `Cargo.toml` includes the necessary dependencies for handling WebSockets and time formatting.

```toml
[dependencies]
# ... previous dependencies ...
tokio-tungstenite = "0.28.0"
futures-util = "0.3.31"
chrono = "0.4.42"
```

The following code sets up a complete ecosystem: a Proxy, a Mock Server, and a Test Client.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::sleep;

// WebSocket Dependencies
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::protocol::Message};

pub struct WebSocketProxy;

#[async_trait]
impl ProxyHttp for WebSocketProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Forward to the Mock Server running in the background
        let addr = ("127.0.0.1", 9091);
        let mut peer = Box::new(HttpPeer::new(addr, false, "".to_string()));

        // CRITICAL: WebSocket Configuration
        // 1. read_timeout: Set to None (infinity). WebSockets are often idle.
        //    If we leave the default (e.g., 60s), Pingora will kill the connection.
        // 2. connection_timeout: Standard TCP connect timeout.
        peer.options.read_timeout = None;
        peer.options.write_timeout = None;
        peer.options.connection_timeout = Some(Duration::from_secs(5));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        // Pingora automatically preserves the "Connection" and "Upgrade" headers
        // required for the handshake. We log here purely for visibility.
        if let Some(upgrade) = upstream_request.headers.get("Upgrade") {
            info!("Proxy: Detected Upgrade Header: {:?}", upgrade);
        }
        Ok(())
    }
}

// --- Background Task: The Upstream Server ---
async fn run_echo_server() {
    let addr = "127.0.0.1:9091";
    let listener = TcpListener::bind(addr).await.expect("Failed to bind Echo Server.");
    info!("Echo Server listening on {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let mut ws_stream = accept_async(stream).await.expect("Failed to accept WS");
            
            while let Some(msg) = ws_stream.next().await {
                let msg = msg.expect("Error reading message.");
                if msg.is_text() {
                    let text = msg.to_text().unwrap();
                    // Reply with a timestamped echo
                    let response_text = format!("Echo [{}]: {}", chrono::Local::now().format("%H:%M:%S"), text);

                    info!("Server: Received '{}', Replying...", text);
                    ws_stream.send(Message::Text(response_text.into())).await.unwrap();
                }
            }
        });
    }
}

// --- Background Task: The Test Client ---
async fn run_test_client() {
    // Wait for Proxy to bind port
    sleep(Duration::from_secs(2)).await;

    let url = "ws://127.0.0.1:6167";
    info!("Client: Connection to Proxy at {}", url);

    let (mut ws_stream, _) = connect_async(url).await.expect("Failed to connect to proxy.");
    info!("Client: Connected! Starting message loop...");

    // Send 3 Pings with a 1-second delay
    for i in 1..=3 {
        let msg = format!("Ping #{}", i);
        ws_stream.send(Message::Text(msg.into())).await.expect("Failed to send");

        if let Some(resp) = ws_stream.next().await {
            let resp_msg = resp.expect("Failed to read response");
            info!("Client: Received '{}'", resp_msg.to_text().unwrap());
        }
        sleep(Duration::from_secs(1)).await;
    }

    info!("Client: Test Complete. Closing connection.");
    ws_stream.close(None).await.unwrap();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Spawn Background Services
    let _server_handle = std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_echo_server());
    });

    let _client_handle = std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_test_client());
    });

    // Start Proxy Service
    let mut my_proxy = http_proxy_service(&my_server.configuration, WebSocketProxy);
    my_proxy.add_tcp("0.0.0.0:6167");

    info!("WebSocket Proxy running on 0.0.0.0:6167");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## 6. Verification

This script verifies itself automatically. When you run it, you will see the logs from all three components (Client, Proxy, Server) interleaved, showing the flow of data.

**1. Run the Example**

```bash
RUST_LOG=info cargo run --example 23_websocket_upgrade
```

**2. Analyze the Output**
You should see the following sequence:

1. **Setup:** The Proxy and Echo Server start.
2. **Handshake:** The Client connects (`ws://127.0.0.1:6167`). The Proxy detects the `Upgrade` header.
3. **Traffic:** The Client sends "Ping #1". The Server receives it and replies with "Echo [Time]: Ping #1".
4. **Repeat:** This happens 3 times over the same persistent connection.

**Sample Log Output:**

```text
[INFO] Echo Server listening on 127.0.0.1:9091
[INFO] WebSocket Proxy running on 0.0.0.0:6167
[INFO] Client: Connection to Proxy at ws://127.0.0.1:6167
[INFO] Proxy: Detected Upgrade Header: "websocket"
[INFO] Client: Connected! Starting message loop...
[INFO] Client: Sending 'Ping #1'
[INFO] Server: Received 'Ping #1', Replying...
[INFO] Client: Received 'Echo [11:00:19]: Ping #1'
...
[INFO] Client: Test Complete. Closing connection.
```

# Lesson 24: gRPC Proxy

gRPC is a high-performance RPC framework that runs on top of **HTTP/2**. Unlike standard REST APIs, gRPC relies heavily on specific HTTP/2 features:

* **Binary Framing:** It uses Protocol Buffers (protobufs) instead of JSON text.
* **Trailers:** It sends status codes (like `grpc-status`) in a separate header block *after* the response body has finished.
* **Multiplexing:** It allows multiple requests to be interleaved over a single TCP connection.

To proxy gRPC traffic effectively, Pingora must be configured to establish an **HTTP/2 connection** to the upstream server. If the proxy falls back to HTTP/1.1, standard gRPC backends will often reject the connection immediately.

## Key Concepts

### 1. ALPN (Application-Layer Protocol Negotiation)

When establishing a TLS connection, the client and server perform a handshake to decide which protocol to speak (e.g., "http/1.1" or "h2").

* **The Problem:** By default, connections might settle on HTTP/1.1 for compatibility.
* **The Fix:** We must explicitly configure Pingora's upstream peer to request `h2` during the TLS handshake.

### 2. Trailers

In REST, an HTTP 200 OK means success. In gRPC, an HTTP 200 OK just means "I received your message." The actual success or failure is communicated in the **Trailers** (headers sent *after* the body). Pingora supports HTTP trailers natively, ensuring this critical status information is preserved.

### 3. End-to-End Encryption (and Trust)

In previous steps, we generated a specific certificate for our gRPC container (`grpc.pingora.local`). Because we set the `SSL_CERT_FILE` environment variable in our docker-compose, Pingora automatically trusts our Root CA. This allows us to connect securely to the upstream without disabling certificate verification—a best practice for production gRPC services.

## The Code (`examples/24_grpc_proxy.rs`)

In this example, we configure Pingora to target the secure port (9001) of our `grpcbin` container. The critical line is forcing the ALPN negotiation to `H2`.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
// We import ALPN from the peer module to control protocol negotiation
use pingora::upstreams::peer::{HttpPeer, ALPN};

pub struct GrpcProxy;

#[async_trait]
impl ProxyHttp for GrpcProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Target the gRPC server (Secure Port 9001)
        let addr = ("172.28.0.23", 9001);
        let sni = "grpc.pingora.local";

        // 1. Enable TLS
        // Pingora will use the system trust store (defined by SSL_CERT_FILE)
        // to verify the upstream's certificate.
        let mut peer = Box::new(HttpPeer::new(addr, true, sni.to_string()));

        // 2. Force HTTP/2 via ALPN
        // This is mandatory. If we negotiate HTTP/1.1, most gRPC servers will
        // close the connection or return a protocol error.
        peer.options.alpn = ALPN::H2;

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Optional: Log when we see gRPC traffic
        if let Some(ctype) = upstream_request.headers.get("content-type") {
            if ctype.to_str().unwrap_or("").starts_with("application/grpc") {
                info!("Proxying gRPC request...");
            }
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, GrpcProxy);

    // We listen on TLS so we can support secure gRPC clients.
    // This allows the client to negotiate H2 with Pingora as well.
    my_proxy.add_tls(
        "0.0.0.0:6168",
        "conf/keys/server.crt",
        "conf/keys/server.key"
    ).expect("Failed to add TLS listener");

    info!("gRPC Proxy running on 0.0.0.0:6168");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will use `curl` to simulate a gRPC client. Since we don't have a full gRPC client (like `grpcurl`) installed in our environment, we will manually construct a compliant HTTP/2 request.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 24_grpc_proxy
```

### 2. Send the Request

We target the Pingora proxy (`172.28.0.10:6168`). We must provide the Root CA so `curl` trusts Pingora's TLS certificate.

```bash
docker exec -it pingora_client_1 curl -v \
  --cacert /keys/ca.crt \
  -H "Content-Type: application/grpc" \
  -H "TE: trailers" \
  -X POST https://172.28.0.10:6168/grpc.health.v1.Health/Check
```

### 3. Analyze the Output

You should observe the following in the `curl` output:

1. **Status 200 OK:** `< HTTP/1.1 200 OK` (or `HTTP/2`). This indicates the upstream accepted the connection.
2. **gRPC Headers:** You will see headers like `Content-Type: application/grpc` and trailers like `Grpc-Status`.
3. **Logs:** The proxy logs will show `Proxying gRPC request...`.

**Note on Protocol Versions:**
Even if your `curl` client negotiates HTTP/1.1 with Pingora (as seen in some test environments), Pingora is actively translating that traffic and speaking **HTTP/2** to the upstream `grpcbin`. This confirms that Pingora is correctly acting as a gateway, bridging the protocols as defined in our configuration.

# Lesson 25: Connection Reuse (Pooling)

Establishing a new TCP connection (and potentially a TLS handshake) for every single request is expensive. It adds significant latency and consumes CPU on both the proxy and the upstream.

**Connection Reuse** (or "Keep-Alive") allows Pingora to keep a connection to an upstream open after a request finishes, putting it into a "Pool." When a new request arrives for that same upstream, Pingora checks the pool first. If a healthy connection exists, it reuses it—skipping the handshake entirely.

In this lesson, we will configure connection pooling and, critically, **prove** it works by capturing the reuse flag using Pingora's lifecycle hooks.

## Key Concepts

### 1. The `CTX` (Context) Pattern

Since Pingora's request lifecycle is split across multiple asynchronous phases, variables don't persist automatically between them. To track whether a connection was reused, we must:

1. Define a custom `CTX` struct.
2. Capture the `reused` boolean in the `connected_to_upstream` hook.
3. Store it in our `CTX`.
4. Read it back in the `logging` hook.

### 2. Tuning the Pool

* **`idle_timeout`:** How long a connection can sit in the pool doing nothing before Pingora closes it. If this is too short (e.g., 0), reuse will never happen.
* **`pool_max_idle`**: (Global setting, not shown here) Limits how many idle connections we keep per peer.

## The Code (`examples/25_connection_reuse.rs`)

We define a `ReusingCtx` to hold our state, and implement the `connected_to_upstream` hook to intercept the connection event.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::protocols::Digest;
use std::time::Duration;

pub struct ReusingProxy;

// 1. Define a Context to hold state across the request lifecycle
pub struct ReusingCtx {
    pub is_reused: bool,
}

#[async_trait]
impl ProxyHttp for ReusingProxy {
    // 2. Use the custom Context type
    type CTX = ReusingCtx;

    fn new_ctx(&self) -> Self::CTX {
        ReusingCtx { is_reused: false }
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Target Upstream Blue
        let addr = ("172.28.0.20", 8080);
        let mut peer = Box::new(HttpPeer::new(addr, false, "blue.pingora.local".to_string()));

        // --- CONNECTION REUSE CONFIGURATION ---
        
        // idle_timeout: Controls how long an idle connection stays in the pool.
        // We set this to 30s. If we set this to 0, the connection would likely 
        // close immediately after use, preventing reuse.
        peer.options.idle_timeout = Some(Duration::from_secs(30));

        peer.options.connection_timeout = Some(Duration::from_secs(1));

        Ok(peer)
    }

    // 3. Intercept the connection step to capture the 'reused' flag
    // This hook runs immediately after Pingora gets a connection (either new or from pool).
    async fn connected_to_upstream(
        &self,
        _session: &mut Session,
        reused: bool,                 // <--- The flag we want!
        _peer: &HttpPeer,
        #[cfg(unix)] _fd: std::os::unix::io::RawFd,
        #[cfg(windows)] _sock: std::os::windows::io::RawSocket,
        _digest: Option<&Digest>,
        ctx: &mut Self::CTX,          // <--- Our mutable context
    ) -> Result<()> {
        // Store the status in our context for logging later
        ctx.is_reused = reused;
        Ok(())
    }

    async fn logging(
        &self,
        _session: &mut Session,
        _e: Option<&pingora::Error>,
        ctx: &mut Self::CTX,          // <--- Access the stored context
    ) {
        // 4. Log the state to prove reuse happened
        info!("Request Complete. Reused Connection: {}", ctx.is_reused);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut my_proxy = http_proxy_service(&my_server.configuration, ReusingProxy);
    my_proxy.add_tcp("0.0.0.0:6169");

    info!("Connection Reuse Demo running on 0.0.0.0:6169");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

To verify reuse, we need to send requests fast enough that the previous connection hasn't timed out yet. Using `curl` with two URLs in a single command is a great way to do this.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 25_connection_reuse
```

### 2. Send Sequential Requests

We ask `curl` to fetch the same URL twice in a row.

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6169/ http://172.28.0.10:6169/
```

### 3. Analyze the Logs

You will see exactly how the pooling mechanism works:

```text
[INFO] Request Complete. Reused Connection: false
[INFO] Request Complete. Reused Connection: true
```

* **Request 1 (`false`):** The pool was empty. Pingora had to perform a TCP handshake with `172.28.0.20`.
* **Request 2 (`true`):** Pingora checked the pool, found the live connection from Request 1, and reused it immediately.

This "0-RTT" (Zero Round Trip Time) connection setup is vital for high-performance proxies.

# Module 4: Load Balancing

Up to this point, our proxy has been a **1-to-1** conduit. We accepted a request and forwarded it to a single, hardcoded destination (e.g., `172.28.0.20`).

In **Module 4**, we graduate to **1-to-N** routing. We are no longer just "proxying"; we are **distributing** traffic. This module focuses on the `pingora-load-balancing` crate, which provides the logic to manage pools of upstream servers, ensuring high availability, scalability, and efficiency.

We will move away from defining a single `HttpPeer` and start defining **Clusters** of backends. We will explore:

* **Selection Algorithms:** How does Pingora decide which server gets the next request? (Round Robin, Weighted, Hashing).
* **Health Checking:** How does Pingora automatically detect and remove dead servers from rotation *before* they cause errors?
* **Session Persistence:** How do we ensure a user always returns to the same backend for stateful applications?
* **Dynamic Discovery:** How do we update our list of backends without restarting the server?

This is where Pingora transforms from a simple pipe into a robust Edge Gateway capable of handling production-scale traffic.

# Lesson 26: Round Robin Load Balancing

In previous modules, our proxy acted as a simple pipe: one listener mapped to one upstream. In **Module 4**, we unlock the power of **Load Balancing**, transforming Pingora into a gateway that distributes traffic across a cluster of servers.

We start with the most fundamental algorithm: **Round Robin**. This strategy rotates requests sequentially through the list of available backends (`Blue -> Green -> Blue -> Green`). It is ideal for stateless services where all backends have roughly equal capacity.

## Key Concepts

### 1. The `LoadBalancer` Struct

The `pingora_load_balancing::LoadBalancer<S>` struct is the "brain" of this module.

* **Inventory:** It holds the list of all backend servers.
* **State:** It tracks which servers are healthy (and eligible for traffic) and which are not.
* **Strategy (`S`):** It is generic over a selection algorithm. In this lesson, we specify `LoadBalancer<RoundRobin>`.

### 2. The Background Service

Load balancing involves more than just picking a random IP. It often requires active maintenance tasks like:

* Pinging servers to check if they are alive (Health Checks).
* Querying DNS or APIs to find new servers (Service Discovery).

To perform these tasks without blocking the main proxy thread, Pingora runs the `LoadBalancer` as a separate **Background Service**. Even if we provide a static list of IPs (as we do here), using this architecture from the start prepares us for dynamic features later.

## The Code (`examples/26_round_robin.rs`)

This code sets up a Load Balancer with two upstreams (`Blue` and `Green`) and routes traffic between them.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;
use std::sync::Arc;

// 1. The Struct holding our Load Balancer state
// We wrap it in Arc so it can be shared between the Background Service (which updates it)
// and the Proxy Service (which reads from it).
pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // 2. Load Balancing Selection
        // select() takes a key (for hashing) and a max_iterations limit.
        // Since we are using RoundRobin, the key (b"") is ignored.
        // We look up to 256 times to find a healthy backend.
        let upstream = self.0
            .select(b"", 256) 
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Selected upstream: {:?}", upstream);

        // 3. Construct the Peer
        // We use the address provided by the Load Balancer.
        // "upstream" is a generic SNI/Host since we are hitting simple echo servers.
        let peer = Box::new(HttpPeer::new(
            upstream, 
            false, // No TLS for these specific echo containers
            "upstream".to_string()
        ));
        
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        // Just for clarity in the logs
        upstream_request.insert_header("Host", "round-robin-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 4. Initialize the Load Balancer
    // We create a static list. In later modules, this can be dynamic.
    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080", // Blue
        "172.28.0.21:8080", // Green
    ])?;

    // 5. Create the Background Service
    // This allows the LoadBalancer to run tasks (like health checks) in the background.
    let background = background_service("round_robin_lb", upstreams);
    
    // Get the Arc pointer to the LB to pass to our proxy
    let lb_ref = background.task();

    // 6. Create the Proxy Service
    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6170");

    info!("Round Robin Load Balancer running on 0.0.0.0:6170");
    
    // 7. Register both services
    my_server.add_service(background);
    my_server.add_service(my_proxy);
    
    my_server.run_forever();
}
```

## Verification

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 26_round_robin
```

### 2. Observe the "Service Exited" Log

You might see a log line saying `[INFO pingora_core::server] service exited`.
**Do not panic.**

* This refers to the `round_robin_lb` background service.
* Because we haven't configured any **Health Checks** or **Dynamic Discovery** yet, the background service realized it had no periodic work to do and exited to save resources.
* The Load Balancer state (the list of IPs) remains safely in memory, held by the Proxy Service.

### 3. Send Traffic

From another terminal, send two requests:

```bash
curl http://172.28.0.10:6170/
curl http://172.28.0.10:6170/
```

### 4. Analyze Output

You will see the responses alternate perfectly:

```text
'Response from BLUE'
'Response from GREEN'
```

In the proxy logs, you will see the selection logic at work:

```text
[INFO] Selected upstream: Backend { addr: Inet(172.28.0.20:8080), ... }
[INFO] Selected upstream: Backend { addr: Inet(172.28.0.21:8080), ... }
```

# Lesson 27: Weighted Load Balancing

In a real-world cluster, not all servers are created equal. You might have a beefy bare-metal server ("Blue") alongside a smaller virtual machine ("Green"). If you use standard Round Robin, you will overload the small server or underutilize the big one.

**Weighted Load Balancing** solves this by assigning a "weight" to each backend. A server with Weight 3 receives 3x more requests than a server with Weight 1.

In this lesson, we will configure:

* **Upstream Blue (172.28.0.20):** Weight **3** (High Capacity).
* **Upstream Green (172.28.0.21):** Weight **1** (Low Capacity).

## Key Concepts

### 1. The Initialization "Trap"

In the previous lesson, we used `LoadBalancer::try_from_iter(["ip1", "ip2"])`.

* **The Problem:** This convenience method takes raw strings and converts them into `Backend` objects with a **default weight of 1**. If we used it here, it would wipe out our custom weights.
* **The Fix:** We must manually create `Backend` objects using `Backend::new_with_weight`. Then, we manually build the discovery pipeline:
  `Backend` → `BTreeSet` → `Static Discovery` → `Backends` → `LoadBalancer`.

### 2. The `RoundRobin` Type Alias

You might think we need to define our balancer as `LoadBalancer<Weighted<RoundRobin>>`.
**Do not do this.**
In Pingora, the `RoundRobin` type you import is actually a type alias for `Weighted<algorithms::RoundRobin>`. It supports weights natively. If you try to wrap it manually, you will get complex compilation errors about unsatisfied trait bounds.

## The Code (`examples/27_weighted_lb.rs`)

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

// Key Imports for Load Balancing
use pingora_load_balancing::{LoadBalancer, Backend, Backends};
use pingora_load_balancing::discovery::Static;
use pingora_load_balancing::selection::RoundRobin; // This alias includes Weighted logic
use std::collections::BTreeSet;
use std::sync::Arc;

// The struct wraps the LoadBalancer.
// Note: We use 'RoundRobin' as the generic type. In Pingora, the 'RoundRobin' type alias
// is actually defined as 'Weighted<algorithms::RoundRobin>', so it supports weights natively.
pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Selection: b"" key is ignored by RoundRobin.
        // The balancer automatically handles the 3:1 distribution logic.
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Selected upstream: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "weighted.upstream".to_string()
        ));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "weighted-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 1. Create Backends with specific Weights
    let blue = Backend::new_with_weight("172.28.0.20:8080", 3)?;
    let green = Backend::new_with_weight("172.28.0.21:8080", 1)?;

    // 2. Construct the Upstream Set Manually
    // We CANNOT use LoadBalancer::try_from_iter() here, because that helper method
    // extracts IPs and creates *new* Backend objects with default weight (1).
    // We must pass our pre-weighted Backend objects into a Static discovery service.
    let mut set = BTreeSet::new();
    set.insert(blue);
    set.insert(green);

    let discovery = Static::new(set);
    let backends = Backends::new(discovery);
    let upstreams = LoadBalancer::from_backends(backends);

    // 3. Create Background Service
    let background = background_service("weighted_lb", upstreams);
    let lb_ref = background.task();

    // 4. Create Proxy Service
    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6171");

    info!("Weighted Load Balancer running on 0.0.0.0:6171");
    info!("Configuration: Blue (Weight 3) vs Green (Weight 1)");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 27_weighted_lb
```

### 2. Run the Traffic Test

We will run `curl` 4 times. Based on our 3:1 configuration, we expect 3 responses from Blue and 1 from Green.

```bash
docker exec -it pingora_client_1 bash -c "for i in {1..4}; do curl -s http://172.28.0.10:6171/; echo; done"
```

### 3. Analyze Output

You should see a pattern similar to this (order may vary slightly, but the ratio holds):

```text
'Response from BLUE'
'Response from BLUE'
'Response from BLUE'
'Response from GREEN'
```

In the proxy logs, you can confirm the selection details:

```text
[INFO] Selected upstream: Backend { addr: Inet(172.28.0.20:8080), weight: 3, ... }
[INFO] Selected upstream: Backend { addr: Inet(172.28.0.20:8080), weight: 3, ... }
[INFO] Selected upstream: Backend { addr: Inet(172.28.0.20:8080), weight: 3, ... }
[INFO] Selected upstream: Backend { addr: Inet(172.28.0.21:8080), weight: 1, ... }
```

# Lesson 28: Consistent Hashing (Ketama)

In a typical load balancing setup (like Round Robin), requests are distributed evenly. However, for applications relying on **local caching** or **user sessions**, this is inefficient. If User A logs in on Server 1, but their next request goes to Server 2, they might be logged out or experience a "cold" cache.

**Consistent Hashing** ensures that requests with the same "Key" (e.g., URL path, User ID, or IP address) are **always** routed to the same upstream server.

In this lesson, we will implement the **Ketama** algorithm. We will route requests based on their **URL Path**:

* `/user/123` → Always hits Server A.
* `/user/456` → Always hits Server B.

## Key Concepts

### The Hash Ring

Imagine a clock face (a ring).

1. **Nodes:** We hash our servers (Blue, Green) to specific points on this ring.
2. **Keys:** We hash the incoming request path (e.g., `/user/123`) to a point on the ring.
3. **Routing:** To find the correct server, we move **clockwise** from the request's point until we hit a server.

**Why Ketama?**
It minimizes disruption. If you add or remove a server, only the keys immediately "behind" that server on the ring are reassigned. In standard modulo hashing (`hash % N`), changing `N` (the number of servers) would reshuffle nearly 100% of your traffic.

## The Code (`examples/28_consistent_hashing.rs`)

We configure the `LoadBalancer` to use `KetamaHashing` as its selection strategy. The critical change happens in `upstream_peer`, where we extract the path and pass it as the hash key.

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::LoadBalancer;
// Import the specific Ketama algorithm
use pingora_load_balancing::selection::consistent::KetamaHashing;
use std::sync::Arc;

// 1. The Struct: Wraps LoadBalancer with the KetamaHashing strategy
pub struct LB(Arc<LoadBalancer<KetamaHashing>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // 2. The Key: Extract the path from the request URI
        // This is what makes the routing "sticky" to the URL.
        let path = session.req_header().uri.path();
        let key = path.as_bytes();

        // 3. Selection: Pass the path as the hash key.
        // This ensures Deterministic Routing: Same Key -> Same Server.
        let upstream = self.0
            .select(key, 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Path '{}' hashed to upstream: {:?}", path, upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "consistent.cluster".to_string()
        ));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "consistent-hash-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 4. Initialization: Create the pool.
    // Ketama handles distribution via the ring, so 'try_from_iter' is sufficient.
    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080", // Blue
        "172.28.0.21:8080", // Green
    ])?;

    // 5. Background Service: Essential for LB maintenance
    let background = background_service("consistent_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6172");

    info!("Consistent Hashing LB running on 0.0.0.0:6172");
    info!("Try: curl http://127.0.0.1:6172/user/123 vs /user/456");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

To verify that the hashing is consistent, we need to show that requesting the same URL repeatedly *always* results in the same backend being chosen, but different URLs might map to different backends.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 28_consistent_hashing
```

### 2. Run the Test Loop

Run the following command in your client terminal. It loops through 5 different "User IDs" (paths), hitting each one twice.

```bash
docker exec -it pingora_client_1 bash -c "for i in {1..5}; do curl -s http://172.28.0.10:6172/user/\$i; echo; curl -s http://172.28.0.10:6172/user/\$i; echo; echo ---; done"
```

### 3. Analyze Results

You will observe **Sticky** behavior:

* **User 1:** Might go to **Green** both times.
* **User 3:** Might go to **Blue** both times.
* Unlike Round Robin, you will never see `/user/1` go to Blue and then immediately swap to Green.

**Sample Log Output:**

```text
[INFO] Path '/user/1' hashed to upstream: Backend { addr: Inet(172.28.0.21:8080)... }
[INFO] Path '/user/1' hashed to upstream: Backend { addr: Inet(172.28.0.21:8080)... }
...
[INFO] Path '/user/3' hashed to upstream: Backend { addr: Inet(172.28.0.20:8080)... }
[INFO] Path '/user/3' hashed to upstream: Backend { addr: Inet(172.28.0.20:8080)... }
```

This confirms that Pingora is correctly using the path as a stable routing key.

# Lesson 29: Sticky Sessions (Cookie-Based)

In the previous lesson, we used **Consistent Hashing** on the *URL Path*. This ensures that `/image.png` always comes from the same server, which is great for caching.

However, for stateful applications (e.g., a shopping cart or a login session), we need **User Affinity**. We want User A to stay on Server Blue, regardless of whether they visit `/home`, `/profile`, or `/cart`.

To achieve this, we combine **Consistent Hashing** with **Cookies**.

## Key Concepts

### 1. The Session Cookie

The logic flow is:

1. **Incoming Request:** Does it have a `session-id` cookie?
2. **No (New User):** Generate a new ID (e.g., `user-100`), hash it to pick a server, and send a `Set-Cookie` header so the client remembers it.
3. **Yes (Returning User):** Extract the ID, hash it (which results in the exact same server), and route the request.

### 2. The Context (`CTX`) Bridge

This lesson demonstrates a critical pattern in Pingora: **Sharing state between Request and Response phases.**

* We detect/generate the session ID in `upstream_peer` (Request Phase).
* We need to write the `Set-Cookie` header in `response_filter` (Response Phase).
* We use a custom `StickyCtx` struct to carry this data across the lifecycle.

## The Code (`examples/29_sticky_sessions.rs`)

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;
use pingora::http::ResponseHeader;
use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::consistent::KetamaHashing;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// Global counter to generate simple unique session IDs for this demo
static SESSION_COUNTER: AtomicUsize = AtomicUsize::new(100);

// 1. Context: Holds state between the Request phase and the Response phase.
// If we generate a new ID, we must store it here to inject the Set-Cookie header later.
pub struct StickyCtx {
    pub new_session_id: Option<String>,
}

// Struct wrapping the Ketama-based Load Balancer
pub struct LB(Arc<LoadBalancer<KetamaHashing>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = StickyCtx;

    fn new_ctx(&self) -> Self::CTX {
        StickyCtx { new_session_id: None }
    }

    async fn upstream_peer(
        &self,
        session: &mut Session,
        ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let mut session_id = String::new();

        // 2. Cookie Parsing Logic
        // We look for "Cookie: session-id=xyz".
        if let Some(cookie_val) = session.req_header().headers.get("Cookie") {
            let cookie_str = cookie_val.to_str().unwrap_or("");
            for part in cookie_str.split(';') {
                let part = part.trim();
                if part.starts_with("session-id=") {
                    session_id = part.trim_start_matches("session-id=").to_string();
                    info!("Found existing session cookie: {}", session_id);
                    break;
                }
            }
        }

        // 3. New Session Generation
        // If no cookie was found, create a new ID and mark it for injection.
        if session_id.is_empty() {
            let new_id = SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
            session_id = format!("user-{}", new_id);
            
            info!("No cookie found. Generated new session id: {}", session_id);
            ctx.new_session_id = Some(session_id.clone());
        }

        // 4. Consistent Hashing Selection
        // Crucial: Use the session_id as the hash key, NOT the request path.
        let key = session_id.as_bytes();
        let upstream = self.0
            .select(key, 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Session '{}' stuck to upstream: {:?}", session_id, upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "sticky.cluster".to_string()
        ));

        Ok(peer)
    }

    // 5. Response Filter
    // Injects the Set-Cookie header if we generated a new session ID.
    async fn response_filter(
        &self,
        _session: &mut Session,
        upstream_response: &mut ResponseHeader,
        ctx: &mut Self::CTX,
    ) -> Result<()> {
        if let Some(new_id) = &ctx.new_session_id {
            let cookie_value = format!("session-id={}; Path=/", new_id);
            upstream_response.insert_header("Set-Cookie", cookie_value)?;
            info!("Injected Set-Cookie header for {}", new_id);
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 6. Initialization
    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080", // Blue
        "172.28.0.21:8080", // Green
    ])?;

    let background = background_service("sticky_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6173");

    info!("Sticky Session LB running on 0.0.0.0:6173");
    
    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

To verify sticky sessions, we must act like a browser that saves cookies. We will use `curl`'s "cookie jar" feature.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 29_sticky_sessions
```

### 2. The First Visit (New User)

Run this command. It saves received cookies to `cookies.txt`.

```bash
docker exec -it pingora_client_1 curl -v -c /cookies.txt http://172.28.0.10:6173/
```

* **Result:** You will see `< Set-Cookie: session-id=user-100; Path=/`.
* **Routing:** The logs will show "No cookie found" and routing to a specific server (e.g., Green).

### 3. The Second Visit (Returning User)

Run this command. It reads cookies from `cookies.txt`.

```bash
docker exec -it pingora_client_1 curl -v -b /cookies.txt http://172.28.0.10:6173/
```

* **Result:** You will see `> Cookie: session-id=user-100`.
* **Routing:** The logs will show "Found existing session cookie" and routing to **Green** (the same server).

### 4. The Third Visit (Different URL)

Try a different path:

```bash
docker exec -it pingora_client_1 curl -v -b /cookies.txt http://172.28.0.10:6173/different/path
```

* **Result:** Still routed to **Green**. The routing is now tied to the *User*, not the *URL*.

# Lesson 30: TCP Health Checks

In previous lessons, our Load Balancer was "blind." If an upstream server crashed, Pingora would still try to route requests to it, resulting in errors for the client (502 Bad Gateway) until the server came back online.

**Active Health Checking** solves this. Pingora can actively probe your servers in the background. If a server fails to respond, it is marked "unhealthy" and removed from the rotation *before* a real user request hits it.

## Key Concepts

### 1. Active vs. Passive

* **Passive Checking:** The load balancer watches real traffic. If 5 requests fail in a row, it marks the server down. *Downside: Real users have to see errors for the system to react.*
* **Active Checking:** The load balancer opens a separate connection (probe) every second. If the probe fails, the server is marked down. *Upside: The system often reacts before users notice.* We are using this approach today.

### 2. The `TcpHealthCheck`

This is a Layer 4 check. It simply tries to establish a TCP handshake.

* **Success:** Handshake completes.
* **Failure:** Connection refused or timeout.

### 3. The Race Condition

There is always a tiny window between probes (e.g., 1 second) where a server could die. To handle this, we also set a short `connection_timeout` on the actual request peer. This ensures that if we *do* hit a dead server during that window, we fail fast rather than hanging for a minute.

## The Code (`examples/30_health_check_tcp.rs`)

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;
use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::RoundRobin;
use pingora_load_balancing::health_check::TcpHealthCheck;
use std::sync::Arc;
use std::time::Duration;

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Selection: The LoadBalancer handles filtering.
        // If a backend is marked unhealthy, select() will effectively skip it.
        // If ALL backends are unhealthy, this returns None (mapped to Error).
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "All upstreams are down"))?;

        info!("Routed to upstream: {:?}", upstream);

        let mut peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "health-check.cluster".to_string()
        ));
        
        // Critical: Set timeouts on the actual request peer as well.
        // If the health check hasn't caught a failure yet (race condition window),
        // we don't want the client waiting 60s.
        peer.options.read_timeout = Some(Duration::from_secs(1));
        peer.options.connection_timeout = Some(Duration::from_secs(1));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "health-check-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 1. Initialize Load Balancer
    let mut upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080", // Blue
        "172.28.0.21:8080", // Green
    ])?;

    // 2. Configure the TCP Health Check
    let mut hc = TcpHealthCheck::new();
    // Fail Fast: probe times out after 1s
    hc.peer_template.options.connection_timeout = Some(Duration::from_secs(1));
    // Sensitivity: 1 failure marks it down, 1 success marks it up
    hc.consecutive_success = 1;
    hc.consecutive_failure = 1;

    // 3. Attach Check to LB
    upstreams.set_health_check(hc);
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.parallel_health_check = true;

    // 4. Background Service (The "Heart" of the health check)
    // The health checks run inside this service.
    let background = background_service("health_check_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6174");

    info!("TCP Health Check LB running on 0.0.0.0:6174");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

To verify this, we will crash a server mid-stream and watch Pingora react.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 30_health_check_tcp
```

### 2. Start the Traffic Loop

In a separate terminal, run a continuous check against the proxy.

```bash
while true; do docker exec pingora_client_1 curl -s http://172.28.0.10:6174/; sleep 0.5; done
```

*Output:* You should see alternating `Response from BLUE` and `Response from GREEN`.

### 3. Kill Blue

In a third terminal, stop the Blue container.

```bash
docker container stop pingora-guide-upstream_blue-1
```

### 4. Observe Reaction

1. **Client:** The output will immediately switch to `Response from GREEN` only. You might see one single error if you hit the race window, but otherwise, it's seamless.
2. **Logs:** You will see the detection log:
   `[WARN] Backend { ... } becomes unhealthy, ConnectTimedout`

### 5. Revive Blue

Start the container again.

```bash
docker container start pingora-guide-upstream_blue-1
```

* **Logs:** `[INFO] Backend { ... } becomes healthy`
* **Client:** Traffic seamlessly resumes alternating between Blue and Green.

This confirms that Pingora is actively managing the health of your upstream pool.

# Lesson 31: HTTP Health Checks

In the previous lesson, we used **TCP Health Checks** to verify that a server's port was open. However, "Port Open" does not mean "Application Working."

A server can be:

* **Deadlocked:** The process is running and accepting TCP connections, but stuck in an infinite loop.
* **Overloaded:** It accepts connections but never responds.
* **Broken:** It returns `500 Internal Server Error` for every request.

A TCP check will pass in all these cases. An **HTTP Health Check** (Layer 7) solves this by sending a real request (e.g., `GET /`) and requiring a valid `200 OK` response.

## Key Concepts

### 1. Layer 4 vs. Layer 7 Checks

* **Layer 4 (TCP):** "Is the phone line connected?" (Fast, cheap, catches crashes).
* **Layer 7 (HTTP):** "Is the person answering the phone making sense?" (Slower, more expensive, catches application bugs).

### 2. The `HttpHealthCheck`

This struct performs the active probing.

* **Request:** `GET /` (default, but customizable).
* **Validation:** Requires status `200 OK` (default).
* **Timeout:** If the server accepts the connection but doesn't send headers back within `read_timeout`, it is marked unhealthy.

## The Code (`examples/31_health_check_http.rs`)

```rust
use async_trait::async_trait;
use log::info;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::RoundRobin;
use pingora_load_balancing::health_check::HttpHealthCheck;
use std::sync::Arc;
use std::time::Duration;

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Selection: Returns only endpoints responding 200 OK to probes
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "All upstreams are down"))?;

        info!("Routed to upstream: {:?}", upstream);

        let mut peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "http-health-check.cluster.local".to_string(),
        ));

        // Critical: Set timeouts on client traffic too.
        // This ensures the client doesn't hang if the server dies between probes.
        peer.options.read_timeout = Some(Duration::from_secs(1));
        peer.options.connection_timeout = Some(Duration::from_secs(1));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()> {
        upstream_request.insert_header("Host", "http-health-check-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 1. Initialize Load Balancer
    let mut upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    // 2. Configure HTTP Health Check
    // "localhost" -> The Host header sent in the probe
    // false -> No TLS (use HTTP)
    let mut hc = Box::new(HttpHealthCheck::new("localhost", false));

    // 3. Tuning
    // Read timeout catches the "Hang": connection established, but no data returned.
    hc.peer_template.options.read_timeout = Some(Duration::from_secs(1));
    hc.peer_template.options.connection_timeout = Some(Duration::from_secs(1));
    hc.consecutive_success = 1;
    hc.consecutive_failure = 1;

    // 4. Attach Check
    // Note: HttpHealthCheck must be manually boxed here.
    upstreams.set_health_check(hc);
    
    // Frequency: How often to probe (every 1s)
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.parallel_health_check = true;

    // 5. Background Service
    let background = background_service("http_health_check_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6175");

    info!("HTTP Health Check LB running on 0.0.0.0:6175");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

To verify the power of Layer 7 checks, we will simulate a "Frozen App."

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 31_health_check_http
```

### 2. Start Traffic

```bash
while true; do docker exec pingora_client_1 curl -s http://172.28.0.10:6175/; sleep 0.5; done
```

### 3. Freeze the App (Docker Pause)

Instead of stopping the container (which closes the port), we `pause` it. This freezes the process in memory. The TCP stack (kernel) is still alive, so `telnet` would succeed, but the app cannot respond.

```bash
docker container pause pingora-guide-upstream_blue-1
```

### 4. Observe Reaction

* **Logs:** `[WARN] Backend ... becomes unhealthy, ReadTimedout`.
  * Pingora connected (TCP success), waited 1 second for headers, got nothing, and marked it dead.
* **Traffic:** All traffic shifts to Green.

### 5. Unfreeze

```bash
docker container unpause pingora-guide-upstream_blue-1
```

The app wakes up, answers the next probe, and re-enters rotation.

# Lesson 32: Custom Health Checks (Body Inspection)

Standard HTTP health checks have a limitation: they typically only validate the **Status Code**. A server might return `200 OK` but serve an error page saying "Database Connection Failed."

To catch these "zombie" servers, we need **Deep Inspection**. We must read the response body and verify that it matches a specific pattern.

In this lesson, we enforce a strict business rule:

* **Rule:** A server is healthy **only** if its response contains the string `"BLUE"`.
* **Scenario:**
* **Upstream Blue:** Returns "Response from BLUE" -> **Pass**.
* **Upstream Green:** Returns "Response from GREEN" -> **Fail** (even though it returns 200 OK).

## Key Concepts

### 1. The `HealthCheck` Trait

Pingora provides built-in `TcpHealthCheck` and `HttpHealthCheck`, but for custom logic, we implement the `HealthCheck` trait ourselves. This gives us complete control. We can:

* Validate specific JSON fields.
* Check cryptographic signatures.
* Verify database connectivity.

### 2. Manual Socket Handling

Because we are bypassing the standard check, we must handle the networking manually.

1. **Connect:** Open a TCP stream to the backend.
2. **Write:** Send a raw HTTP request bytes (`GET / ...`).
3. **Read:** Buffer the response.
4. **Validate:** check if the buffer contains our target string.

## The Code (`examples/32_custom_health_check.rs`)

We define a struct `BodyMatchCheck` and implement the health check logic to scan the response body.

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;
use pingora_load_balancing::{LoadBalancer, Backend};
use pingora_load_balancing::selection::RoundRobin;
use pingora_load_balancing::health_check::HealthCheck;
use pingora::protocols::l4::socket::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// 1. Define Custom Health Check Struct
pub struct BodyMatchCheck {
    pub host: String,
    pub expected_string: String,
}

// 2. Implement the HealthCheck Trait
#[async_trait]
impl HealthCheck for BodyMatchCheck {
    async fn check(&self, target: &Backend) -> Result<()> {
        // A. Extract Standard Socket Address
        // Pingora uses a custom SocketAddr enum (Inet/Unix). We convert it for Tokio.
        let addr = match &target.addr {
            SocketAddr::Inet(addr) => *addr,
            SocketAddr::Unix(_) => {
                return Err(Error::explain(ErrorType::InternalError, "Custom check only supports TCP backends"));
            }
        };

        // B. Connect (Fail Fast Timeout)
        let mut stream = match tokio::time::timeout(
            Duration::from_secs(1), 
            TcpStream::connect(addr),
        ).await {
            Ok(Ok(s)) => s,
            _ => return Err(Error::explain(ErrorType::ConnectTimedout, "Connection timed out")),
        };

        // C. Send Raw HTTP Request
        let request = format!(
            "GET / HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", 
            self.host
        );
        if let Err(e) = stream.write_all(request.as_bytes()).await {
             return Err(Error::explain(ErrorType::WriteError, e.to_string()));
        }

        // D. Read Response
        let mut buffer = [0u8; 1024];
        let n = match tokio::time::timeout(
            Duration::from_secs(1), 
            stream.read(&mut buffer),
        ).await {
            Ok(Ok(n)) => n,
            _ => return Err(Error::explain(ErrorType::ReadTimedout, "Read timed out")),
        };

        // E. Validate Content
        let response_text = String::from_utf8_lossy(&buffer[..n]);
        if response_text.contains(&self.expected_string) {
            Ok(())
        } else {
            warn!("Custom Check Failed for {:?}: Body did not contain '{}'", addr, self.expected_string);
            Err(Error::explain(ErrorType::Custom("InvalidBody"), "Body validation failed"))
        }
    }

    fn health_threshold(&self, _success: bool) -> usize { 
        1 
    }
}

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Selection: Automatically filters out nodes failing the custom check
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "All upstreams are down"))?;

        info!("Routed to upstream: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "custom-check.cluster.local".to_string(),
        ));
        
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX
    ) -> Result<()> {
        upstream_request.insert_header("Host", "custom-check-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let mut upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080", // Blue
        "172.28.0.21:8080", // Green
    ])?;

    // Configure the Custom Check
    let hc = BodyMatchCheck {
        host: "localhost".to_string(),
        expected_string: "BLUE".to_string(),
    };

    // Attach it (Boxing required)
    upstreams.set_health_check(Box::new(hc));
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.parallel_health_check = true;

    let background = background_service("custom_health_check", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6176");

    info!("Custom Health Check LB running on 0.0.0.0:6176");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

We will verify that **Upstream Green** is removed from the rotation because it fails the string match.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 32_custom_health_check
```

### 2. Observe the Logs

Watch the logs immediately. You will see Pingora rejecting Green:

```text
[WARN] Custom Check Failed for 172.28.0.21:8080: Body did not contain 'BLUE'
[WARN] Backend { ... } becomes unhealthy, InvalidBody context: Body validation failed
```

### 3. Verify Traffic

Run a loop against the proxy.

```bash
while true; do docker exec pingora_client_1 curl -s http://172.28.0.10:6176/; sleep 0.5; done
```

**Expected Output:**
You should see 100% Blue responses.

```text
'Response from BLUE'
'Response from BLUE'
'Response from BLUE'
```

Green is returning `200 OK` (so a standard HTTP check would pass it), but our custom logic successfully filtered it out based on the content.

# Lesson 33: Active-Passive Load Balancing (Failover)

In standard load balancing (like Round Robin), all servers are "Active." Traffic is shared among them.

**Active-Passive** routing is different. You have a **Primary** server (or pool) that handles 100% of the traffic. You also have a **Backup** server that sits idle, receiving zero traffic, until the Primary fails. This is common for disaster recovery setups or when the backup is a "Sorry Page" server hosted in a different region.

## Key Concepts

### 1. Pool Separation

To achieve this in Pingora, we **do not** put the Backup server into the `LoadBalancer`.

* If we put both Blue and Green in the `LoadBalancer`, Pingora would try to route traffic to both (Active-Active).
* Instead, we put **only Blue** in the `LoadBalancer`. Green is defined manually in the code as a fallback.

### 2. The Logic Flow

We rely on the return value of `select()`:

* **`Some(upstream)`:** The Primary is healthy. Use it.
* **`None`:** The Primary is unhealthy (the pool is empty). Execute the fallback logic to route to the Backup.

## The Code (`examples/33_active_passive_lb.rs`)

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
// Import background service for running health checks
use pingora::services::background::background_service;
use pingora_load_balancing::LoadBalancer;
use pingora_load_balancing::selection::RoundRobin;
use pingora_load_balancing::health_check::TcpHealthCheck;
use std::sync::Arc;
use std::time::Duration;

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // 1. Attempt to select from the Primary Pool
        let selection = self.0.select(b"", 256);
        
        match selection {
            Some(upstream) => {
                // HAPPY PATH: Primary is healthy
                info!("Primary (Blue) is healthy. Routing to {:?}", upstream.addr);
                let mut peer = Box::new(HttpPeer::new(
                    upstream,
                    false,
                    "primary.cluster.local".to_string()
                ));
                // Important: Set timeouts to ensure we don't hang if the server dies mid-request
                peer.options.read_timeout = Some(Duration::from_secs(1));
                peer.options.connection_timeout = Some(Duration::from_secs(1));
                Ok(peer)
            }
            None => {
                // FAILURE PATH: Primary is down (Health Check removed it)
                warn!("FAILOVER ALERT: Primary pool is empty! Switching to Backup (Green).");
                
                // Manually define the Backup IP
                let backup_addr = ("172.28.0.21", 8080);
                let peer = Box::new(HttpPeer::new(
                    backup_addr,
                    false,
                    "backup.cluster.local".to_string()
                ));
                Ok(peer)
            }
        }
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "active-passive-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 2. Initialize Load Balancer with ONLY the Primary (Blue)
    let mut upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080", 
    ])?;

    // 3. Configure Aggressive Health Check (Fail Fast)
    let mut hc = TcpHealthCheck::new();
    hc.peer_template.options.connection_timeout = Some(Duration::from_secs(1));
    hc.consecutive_success = 1;
    hc.consecutive_failure = 1;

    upstreams.set_health_check(hc);
    upstreams.health_check_frequency = Some(Duration::from_secs(1));
    upstreams.parallel_health_check = true;

    // 4. Background Service
    let background = background_service("primary_pool_lb", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6177");

    info!("Active-Passive LB running on 0.0.0.0:6177");
    info!("Primary: Blue (Checked). Backup: Green (Static Fallback).");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

To verify this, we will crash the Primary and watch the traffic automatically flow to the Backup.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 33_active_passive_lb
```

### 2. Start the Traffic Loop

```bash
while true; do docker exec pingora_client_1 curl -s http://172.28.0.10:6177/; sleep 1; done
```

* **Status:** `Response from BLUE`
* **Logs:** `Primary (Blue) is healthy.`

### 3. Kill the Primary (Blue)

```bash
docker container stop pingora-guide-upstream_blue-1
```

* **Status:** `Response from GREEN` (Immediate switch).
* **Logs:**
   ```text
   [WARN] Backend ... becomes unhealthy
   [WARN] FAILOVER ALERT: Primary pool is empty! Switching to Backup (Green).
   ```

### 4. Revive the Primary

```bash
docker container start pingora-guide-upstream_blue-1
```

* **Status:** `Response from BLUE` (Automatic recovery).
* **Logs:**
   ```text
   [INFO] Backend ... becomes healthy
   [INFO] Primary (Blue) is healthy.
   ```

# Lesson 34: Service Discovery (File-Based)

In all previous lessons, we hardcoded IP addresses (e.g., `172.28.0.20:8080`) directly into the Rust source code. This is fine for a lab, but in production, servers change IP addresses, scale up, or scale down constantly. Recompiling and restarting the proxy every time an IP changes is not acceptable.

**Service Discovery** allows Pingora to fetch the list of upstream servers from an external source (a file, an API, DNS, Consul, etc.) dynamically.

In this lesson, we will implement **File-Based Discovery**:

1. Pingora will read `conf/upstreams.txt`.
2. It will populate the Load Balancer with the IPs found there.
3. It will watch the file for changes. If you edit the file, Pingora updates its routing table **instantly** without a restart.

## Key Concepts

### 1. The `ServiceDiscovery` Trait

This is the heart of dynamic routing in Pingora. It defines a single method: `discover()`.

* **Input:** None.
* **Output:** A list of `Backend` objects.
* **Behavior:** The Load Balancer calls this method periodically (e.g., every 1 second). If the list returned is different from the current list, the Load Balancer performs an atomic swap.

### 2. Hot Reloading (Atomic Swaps)

When the discovery service returns a new list of backends, Pingora replaces the old list in memory.

* **Existing connections** continue to finish on the old servers.
* **New requests** immediately use the new list.
* This ensures **Zero Downtime** during configuration changes.

## The Code (`examples/34_service_discovery_file.rs`)

Since Pingora provides the trait but leaves the specific implementation details flexible, we will implement a simple `FileDiscovery` struct that reads a file and parses IP addresses line-by-line.

```rust
use async_trait::async_trait;
use log::{info, error};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
use pingora::services::background::background_service;

use pingora_load_balancing::{Backends, LoadBalancer, Backend};
use pingora_load_balancing::discovery::ServiceDiscovery;
use pingora_load_balancing::selection::RoundRobin;
// We need the internal SocketAddr enum to construct Backends manually
use pingora::protocols::l4::socket::SocketAddr;

use std::sync::Arc;
use std::time::Duration;
use std::path::PathBuf;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use http::Extensions;

// 1. Define Custom Discovery Struct
// This holds the configuration path state.
pub struct FileDiscovery {
    pub path: PathBuf,
}

// 2. Implement the ServiceDiscovery Trait
// This is the contract Pingora uses to pull backend updates.
#[async_trait]
impl ServiceDiscovery for FileDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        // A. Read the file
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to read upstream file: {}", e);
                return Err(Error::explain(ErrorType::InternalError, e.to_string()))
            },
        };

        // B. Parse Line-by-Line
        let mut upstreams = BTreeSet::new();
        for line in contents.lines() {
            let line = line.trim();
            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("#") {
                continue;
            }
            
            // C. Resolve Address
            // Converts "1.2.3.4:80" into a SocketAddr
            match line.to_socket_addrs() {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        let backend = Backend {
                            addr: SocketAddr::Inet(addr),
                            weight: 1, 
                            ext: Extensions::new()
                        };
                        upstreams.insert(backend);
                    }
                },
                Err(e) => error!("Failed to parse address '{}': {}", line, e),
            }
        }
        
        // Return the new set of backends. 
        // The second return value (HashMap) is for readiness/health overrides, which we leave empty.
        Ok((upstreams, HashMap::new()))
    }
}

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Selection: The LoadBalancer has the most recent list from the file
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Routed to upstream: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "file-discovery.cluster".to_string()
        ));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "file-discovery-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 1. Initialize Custom Discovery
    let discovery = FileDiscovery {
        path: PathBuf::from("conf/upstreams.txt"),
    };

    // 2. Setup Backends and LoadBalancer
    // We wrap our discovery struct in the Backends container
    let backends = Backends::new(Box::new(discovery));
    let mut upstreams = LoadBalancer::from_backends(backends);
    
    // 3. Configure Update Frequency
    // Critical: This tells Pingora to call discover() every 1 second.
    upstreams.update_frequency = Some(Duration::from_secs(1));

    // 4. Background Service
    let background = background_service("file_discovery", upstreams);
    let lb_ref = background.task();

    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6178");

    info!("File-Based Discovery LB running on 0.0.0.0:6178");
    info!("Watching file: conf/upstreams.txt");

    my_server.add_service(background);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

We will start with one server, verify traffic, then edit the file on the fly to switch servers.

### 1. Setup the Config File

Create a `conf` directory if it doesn't exist, and add the **Blue** upstream.

```bash
mkdir -p conf
echo "172.28.0.20:8080" > conf/upstreams.txt
```

### 2. Start the Proxy

```bash
RUST_LOG=info cargo run --example 34_service_discovery_file
```

### 3. Start Traffic (Terminal 2)

```bash
while true; do docker exec pingora_client_1 curl -s http://172.28.0.10:6178/; sleep 1; done
```

* **Output:** `Response from BLUE`
* **Logs:** `Routed to upstream: ... 172.28.0.20:8080`

### 4. Hot Reload (Terminal 3)

While the curl loop is running, overwrite the file with the **Green** upstream.

```bash
echo "172.28.0.21:8080" > conf/upstreams.txt
```

### 5. Observe Results

Within 1 second (our configured update frequency), you will see the traffic shift in the client terminal:

```text
'Response from BLUE'
'Response from BLUE'
'Response from GREEN'  <-- Automatic Switch
'Response from GREEN'
```

And in the proxy logs:

```text
[INFO] Routed to upstream: Backend { addr: Inet(172.28.0.20:8080)... }
[INFO] Routed to upstream: Backend { addr: Inet(172.28.0.21:8080)... }
```

This confirms that Pingora successfully discovered the new configuration and applied it dynamically.

# Lesson 35: Dynamic Reconfiguration (Hot-Swap API)

In the previous lesson, we used **File-Based Discovery**, where Pingora "pulled" configuration changes by watching a file. While effective, modern cloud-native environments often prefer a **"Push" model**.

In this lesson, we will implement an **Admin Control Plane**. We will run two separate services inside our Pingora process:

1. **Proxy Service (Port 6179):** Handles standard user traffic.
2. **Admin Service (Port 9090):** Listens for configuration commands.

We will share state between them using a thread-safe lock (`RwLock`). When you send a POST request to the Admin Port, it updates the memory immediately, and the Proxy Service sees the change instantly.

## Key Concepts

### 1. Shared State (`Arc<RwLock<...>>`)

To make two independent services talk to each other, we create a shared "Source of Truth" for the upstream list.

* **`Arc`:** Allows multiple parts of the code to own the data.
* **`RwLock`:** Allows multiple readers (the Load Balancer) to access data simultaneously, but ensures exclusive access for the writer (the Admin API) during updates.

### 2. The Admin Service

This is a `BackgroundService` that opens a TCP listener on port 9090. It parses incoming HTTP requests, extracts the new IP address from the body, and updates the shared state.

### 3. In-Memory Discovery

We implement a custom `ServiceDiscovery` struct. Instead of reading a file or querying DNS, its `discover()` method simply acquires a **Read Lock** on the shared state and returns a copy of the current list.

## The Code (`examples/35_dynamic_reconfiguration.rs`)

```rust
use async_trait::async_trait;
use log::{info, error};
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::server::ShutdownWatch;
use pingora::upstreams::peer::HttpPeer;
// We import BackgroundService to run our Admin API alongside the proxy
use pingora::services::background::{background_service, BackgroundService};
use pingora_load_balancing::{Backends, LoadBalancer, Backend};
use pingora_load_balancing::discovery::ServiceDiscovery;
use pingora_load_balancing::selection::RoundRobin;
use pingora::protocols::l4::socket::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use tokio::sync::RwLock;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use http::Extensions;

// 1. Shared State Container
// This holds the "Source of Truth" for our upstreams.
// - Wrapped in Arc for shared ownership across threads/tasks.
// - Wrapped in RwLock for thread-safe access (Multiple Readers, One Writer).
#[derive(Clone)]
pub struct UpstreamState {
    pub upstreams: Arc<RwLock<BTreeSet<Backend>>>,
}

// 2. In-Memory Discovery Strategy
// This struct implements the ServiceDiscovery trait required by the LoadBalancer.
// Instead of reading a file, it simply locks the shared memory and returns a copy.
pub struct InMemoryDiscovery {
    pub state: UpstreamState,
}

#[async_trait]
impl ServiceDiscovery for InMemoryDiscovery {
    async fn discover(&self) -> Result<(BTreeSet<Backend>, HashMap<u64, bool>)> {
        // READ LOCK: Fast and non-blocking for concurrent readers
        let guard = self.state.upstreams.read().await;
        Ok((guard.clone(), HashMap::new()))
    }
}

// 3. Admin API Service
// This runs as a generic background service. It binds to port 9090 and
// listens for configuration updates (POST requests).
pub struct AdminApiService {
    pub state: UpstreamState,
}

#[async_trait]
impl BackgroundService for AdminApiService {
    async fn start(&self, mut shutdown: ShutdownWatch) {
        let listener = TcpListener::bind("0.0.0.0:9090").await.expect("Failed to bind Admin Port 9090");
        info!("Admin API listening on 0.0.0.0:9090");

        loop {
            // We use tokio::select! to handle incoming connections OR shutdown signals simultaneously.
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("Admin API shutting down...");
                    return;
                }
                res = listener.accept() => {
                    match res {
                        Ok((mut socket, addr)) => {
                            info!("Admin connection from {}", addr);
                            let state = self.state.clone();
                            
                            // Spawn a new lightweight task for each connection so we don't block the listener.
                            tokio::spawn(async move {
                                let mut buf = [0u8; 1024];
                                let n = match socket.read(&mut buf).await {
                                    Ok(n) if n > 0 => n,
                                    _ => return, // Connection closed
                                };
                                
                                // Basic HTTP parsing (Demonstration purposes only)
                                let req_str = String::from_utf8_lossy(&buf[..n]);
                                
                                // Extract body (everything after the double newline)
                                let body = if let Some(idx) = req_str.find("\r\n\r\n") {
                                    req_str[idx+4..].trim()
                                } else {
                                    req_str.trim()
                                };

                                if !body.is_empty() {
                                    // Parse the IP address from the body
                                    if let Ok(mut addrs) = body.to_socket_addrs() {
                                        if let Some(new_addr) = addrs.next() {
                                            // WRITE LOCK: Exclusive access to update the config
                                            let mut guard = state.upstreams.write().await;
                                            guard.clear();
                                            guard.insert(Backend {
                                                addr: SocketAddr::Inet(new_addr),
                                                weight: 1,
                                                ext: Extensions::new(),
                                            });
                                            info!("Admin: Hot-swapped upstream to {}", new_addr);
                                            
                                            let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK").await;
                                        } else {
                                            let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\nInvalid IP").await;
                                        }
                                    } else {
                                        error!("Admin: Failed to parse IP: {}", body);
                                        let _ = socket.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\nParse Error").await;
                                    }
                                }
                            });
                        }
                        Err(e) => error!("Admin accept error: {}", e),
                    }
                }
            }
        }
    }
}

pub struct LB(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        // Selection: Uses the InMemoryDiscovery logic implicitly
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        info!("Routed to upstream: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(
            upstream,
            false,
            "hot-swap.cluster".to_string()
        ));

        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<()> {
        upstream_request.insert_header("Host", "hot-swap-cluster")?;
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // 1. Initialize Shared State (Default: Blue)
    let initial_addr = "172.28.0.20:8080".to_socket_addrs()?.next().unwrap();
    let mut initial_set = BTreeSet::new();
    initial_set.insert(Backend {
        addr: SocketAddr::Inet(initial_addr),
        weight: 1,
        ext: Extensions::new(),
    });
    
    let state = UpstreamState {
        upstreams: Arc::new(RwLock::new(initial_set)),
    };

    // 2. Setup Load Balancer with InMemoryDiscovery
    // Pass a clone of the state so the LB can read from it
    let discovery = InMemoryDiscovery { state: state.clone() };
    let backends = Backends::new(Box::new(discovery));
    let mut upstreams = LoadBalancer::from_backends(backends);
    
    // Update Frequency: Poll the shared memory state every 500ms
    upstreams.update_frequency = Some(Duration::from_millis(500));

    // 3. Register Services
    
    // A. LB Updater: Runs discover() periodically
    let lb_service = background_service("lb_updater", upstreams);
    let lb_ref = lb_service.task();

    // B. Admin API: Listens on 9090 and updates state
    let admin_task = AdminApiService { state: state.clone() };
    let admin_service = background_service("admin_api", admin_task);

    // C. Proxy: Listens on 6179 and serves traffic
    let mut my_proxy = http_proxy_service(&my_server.configuration, LB(lb_ref));
    my_proxy.add_tcp("0.0.0.0:6179");

    info!("Hot-Swap Proxy running on 0.0.0.0:6179");
    info!("Admin API running on 0.0.0.0:9090");

    my_server.add_service(lb_service);
    my_server.add_service(admin_service);
    my_server.add_service(my_proxy);

    my_server.run_forever();
}
```

## Verification

This verification requires us to act as both the **End User** (calling the proxy) and the **System Admin** (calling the admin port).

### 1. Start the Server

```bash
RUST_LOG=info cargo run --example 35_dynamic_reconfiguration
```

* **Logs:** `Admin API listening on 0.0.0.0:9090` and `Hot-Swap Proxy running on 0.0.0.0:6179`.

### 2. Generate Traffic (Terminal 2)

The client will hit the proxy continuously.

```bash
while true; do docker exec pingora_client_1 curl -s http://172.28.0.10:6179/; sleep 1; done
```

* **Output:** `Response from BLUE`
* **Logs:** `Routed to upstream: ... 172.28.0.20:8080`

### 3. Perform Hot-Swap (Terminal 1 or 3)

We will now use `curl` to send a POST request to the Admin API.
**Important:** This command must be run **from the developer container** (where the server is running on localhost:9090), not from the client container.

```bash
# -d sends the new IP in the body
curl -v -d "172.28.0.21:8080" http://127.0.0.1:9090/
```

### 4. Observe Results

* **Admin Logs:** `Admin: Hot-swapped upstream to 172.28.0.21:8080`
* **Traffic Output:**
```text
'Response from BLUE'
'Response from GREEN'  <-- Instant switch
'Response from GREEN'
```

You have successfully built a control plane that allows you to reconfigure Pingora's routing logic in real-time without restarting the process.

# Module 5: Traffic Control & Security

We have built a proxy that routes, balances, and heals itself. But a production system must also **protect** itself.

A proxy is the gateway to your infrastructure. If it allows every request through unchecked, your backend services will be overwhelmed by traffic spikes, abusive bots, or malicious attackers.

In **Module 5**, we will implement the defensive layer of Pingora using the `pingora-limits` crate and custom filters. We will explore:

* **Rate Limiting:** How to cap the number of requests a user can send per second (Fixed & Sliding Windows).
* **Concurrency Control:** How to limit the number of active connections to a fragile backend to prevent it from crashing under load.
* **Security Filtering:** How to block IPs at the TCP level and reject requests that are too large.
* **Authentication:** How to validate Bearer tokens and Basic Auth headers *at the edge*, offloading this work from your microservices.

By the end of this module, your proxy will not just be a router; it will be a shield.

# Lesson 36: Basic Rate Limiting (Fixed Window)

A proxy isn't just a router; it's a bouncer. Without limits, a single abusive client or a buggy script can overwhelm your backend services.

In this lesson, we implement a **Fixed Window Rate Limiter**.

* **The Rule:** A client IP can send max **5 requests** in a **1-second window**.
* **The Consequence:** If they exceed this, the proxy rejects them immediately with `429 Too Many Requests`. The request never touches the upstream server.

## Key Concepts

### 1. The `request_filter` Hook

Up until now, we have mostly focused on `upstream_peer`. However, for security logic, we need to intervene *before* we even select a server.
The `request_filter` method in the `ProxyHttp` trait allows us to inspect the request and make a go/no-go decision.

* **Return `Ok(false)`:** "Everything looks good, proceed to upstream."
* **Return `Ok(true)`:** "Stop here. I have handled the response (e.g., sent an error)."

### 2. The `pingora-limits` Crate

Pingora provides a high-performance counting library called `pingora-limits`. It uses a "double-buffered" window implementation suitable for high-concurrency environments.

* **`Rate` Struct:** The central counter.
* **`observe(key, count)`:** Increments the counter for a specific key (IP address) and returns the *current* total for the active window.

## The Code (`examples/36_basic_rate_limit.rs`)

This code initializes a global rate limiter and checks every request's IP address against it.

```rust
use async_trait::async_trait;
use log::{info, warn};
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::configuration::Opt;
use pingora::server::Server;
use pingora::upstreams::peer::HttpPeer;
// Import the specific Rate struct from the limits crate
use pingora_limits::rate::Rate;
use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;
use std::time::Duration;

// 1. Global Rate Limiter State
// We use `Lazy` to initialize the rate limiter globally.
// The `Rate` struct manages the sliding windows internally.
// We set the window duration to 1 second.
static RATE_LIMITER: Lazy<Rate> = Lazy::new(|| { Rate::new(Duration::from_secs(1)) });

// The Threshold: 5 requests per second per IP
const MAX_REQ_PER_SEC: isize = 5;

pub struct RateLimiterProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for RateLimiterProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    // 2. The Upstream Selection (Standard Round Robin)
    // This only runs if request_filter returns Ok(false).
    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;
        info!("Selected upstream: {:?}", upstream);
        let peer = Box::new(HttpPeer::new(upstream, false, "rate-limited.cluster".to_string()));
        Ok(peer)
    }

    // 3. The Security Filter (The Core Logic)
    async fn request_filter(
        &self, session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // IP Extraction Logic:
        // Pingora's SocketAddr is an enum (Inet or Unix). We expect TCP connections here.
        // We unwrap the Inet variant to get a hashable IP address.
        let client_ip = match session.client_addr() {
            Some(addr) => addr.as_inet().unwrap().ip(),
            None => {
                warn!("Could not determine client IP, allowing request.");
                return Ok(false);
            }
        };

        // 4. Observe and Check
        // .observe() increments the counter for this IP and returns the *current* total.
        let curr_req_count = RATE_LIMITER.observe(&client_ip, 1);

        // 5. Enforcement
        if curr_req_count > MAX_REQ_PER_SEC {
            warn!("Rate Limit Exceeded for {}: {} req/s", client_ip, curr_req_count);
            
            // Return standard HTTP 429 response
            session.respond_error(429).await?;
            
            // Critical: Return `true` to tell Pingora to STOP processing this request.
            // The request will NOT be forwarded to the upstream_peer.
            return Ok(true);
        }
        
        // Return `false` to continue normal processing
        Ok(false)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Standard Load Balancer setup (Blue/Green)
    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, RateLimiterProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6180");

    info!("Rate Limiter Proxy running on 0.0.0.0:6180");
    info!("Limit: {} requests per second per IP", MAX_REQ_PER_SEC);

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will confirm that the first 5 requests pass, and subsequent requests fail immediately.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 36_basic_rate_limit
```

### 2. Burst Traffic Test

Run this loop in your client terminal to fire 10 requests rapidly:

```bash
docker exec -it pingora_client_1 bash -c 'for i in {1..10}; do curl -s -o /dev/null -w "%{http_code}\n" http://172.28.0.10:6180/; done'
```

### 3. Analyze Output

You will see a clean cutoff:

```text
200  <- 1 (Allowed)
200  <- 2 (Allowed)
200  <- 3 (Allowed)
200  <- 4 (Allowed)
200  <- 5 (Allowed, bucket full)
429  <- 6 (Blocked)
429  <- 7 (Blocked)
429  <- 8 (Blocked)
429  <- 9 (Blocked)
429  <- 10 (Blocked)
```

### 4. Check Proxy Logs

The server logs confirm the interception logic works:

```text
[WARN] Rate Limit Exceeded for 172.28.0.30: 6 req/s
[WARN] Rate Limit Exceeded for 172.28.0.30: 7 req/s
...
```

Notice there are **no** `Selected upstream` logs for requests 6–10. The `request_filter` successfully short-circuited the pipeline.

# Lesson 37: Sliding Window Rate Limiting

The **Fixed Window** approach (Lesson 36) has a flaw known as the "boundary burst." If the window resets at 1.0s, a user could send 5 requests at 0.9s and 5 requests at 1.1s. In a 0.2s span, they sent 10 requests, effectively doubling the allowed rate, yet the limiter sees two separate valid windows.

To solve this, we use a **Sliding Window** (smoothed) limiter. This algorithm calculates the request rate as a weighted average over time, preventing boundary gaming and allowing for more "elastic" traffic handling.

## Key Concepts

### 1. `observe()` vs `rate()`

When using Pingora's `pingora_limits::rate::Rate`:

* **`observe(key, count)`:** This acts as the "writer." It increments the counter for the current time bucket. You **must** call this for every request.
* **`rate(key)`:** This acts as the "reader." It returns an `f64` representing the smoothed requests per second. It looks at both the current bucket and the previous bucket to interpolate the actual velocity of traffic.

### 2. The Logic Flow

1. **Request arrives.**
2. **Increment:** `observe(&ip, 1)` (Record the event).
3. **Calculate:** `rate(&ip)` (Check the speed).
4. **Decide:** If `rate > 5.0`, block.

## The Code (`examples/37_sliding_window.rs`)

```rust
use async_trait::async_trait;
use log::{info, warn};
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora_limits::rate::Rate;
use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;
use std::time::Duration;

// 1. Global Rate Limiter State
// The Rate struct manages the sliding window logic (Red/Blue slots) internally.
// A 1-second window allows us to calculate "Requests Per Second".
static RATE_LIMITER: Lazy<Rate> = Lazy::new(|| { Rate::new(Duration::from_secs(1)) });

// Threshold: 5.0 requests per second (Smoothed)
const MAX_REQ_PER_SEC: f64 = 5.0;

pub struct SlidingWindowProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for SlidingWindowProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    // 2. The Security Filter
    async fn request_filter(
        &self, 
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // IP Extraction Logic (Unwrap Inet address)
        let client_ip = match session.client_addr() {
            Some(addr) => addr.as_inet().unwrap().ip(),
            None => {
                warn!("Could not determine client IP, allowing request.");
                return Ok(false);
            }
        };

        // 3. Observe 
        // Vital: We must increment the counter first.
        // .observe() returns the raw count (isize), but we ignore it here.
        RATE_LIMITER.observe(&client_ip, 1);

        // 4. Check Rate
        // .rate() returns the smoothed requests per second as an f64.
        // This handles the interpolation between previous and current windows.
        let current_rate = RATE_LIMITER.rate(&client_ip);

        // 5. Enforcement
        if current_rate > MAX_REQ_PER_SEC {
            warn!("Rate Limit Exceeded for {}: {:.2} req/s (Limit: {:.1})", 
                  client_ip, current_rate, MAX_REQ_PER_SEC);
            
            session.respond_error(429).await?;
            return Ok(true); // Short-circuit
        }

        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "sliding-window.cluster".to_string()));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, SlidingWindowProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6181");

    info!("Sliding Window Rate Limiter running on 0.0.0.0:6181");
    info!("Limit: {:.1} requests per second (Smoothed)", MAX_REQ_PER_SEC);

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

To verify the "smoothing" effect, we will run two tests: one exactly at the limit, and one slightly over.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 37_sliding_window
```

### 2. Test A: Respecting the Limit (0.2s interval)

Requests sent every 0.20s equal exactly 5 req/s. The smoothed limiter should allow this indefinitely.

```bash
docker exec -it pingora_client_1 bash -c 'for i in {1..15}; do curl -s -o /dev/null -w "%{http_code}\n" http://172.28.0.10:6181/; sleep 0.20; done'
```

* **Result:** 15 `200 OK` responses.

### 3. Test B: Overload (0.15s interval)

Requests sent every 0.15s equal ~6.6 req/s.

```bash
docker exec -it pingora_client_1 bash -c 'for i in {1..15}; do curl -s -o /dev/null -w "%{http_code}\n" http://172.28.0.10:6181/; sleep 0.15; done'
```

* **Result:** The first ~7 requests pass (burst allowance), but then the moving average crosses 5.0, and the rest return `429`.
* **Logs:**
```text
[WARN] Rate Limit Exceeded for 172.28.0.30: 7.00 req/s (Limit: 5.0)
```

This confirms the limiter is successfully calculating velocity rather than just counting bucket hits.

# Lesson 38: In-flight Concurrency Limiting

**Rate Limiting** (Lessons 36-37) controls *how often* a user can knock on your door.
**In-flight Limiting** controls *how many* people can be inside the house at once.

This distinction is vital. A heavy API endpoint (e.g., "Generate PDF Report") might take 5 seconds to process. Even if a user only sends 1 request every 5 seconds (low rate), if 100 users do it simultaneously, you have 100 active threads, which could crash your database.

In this lesson, we implement a **Concurrency Limiter**.

* **Rule:** Max **2 simultaneous requests** allowed globally.
* **Result:** The 3rd simultaneous request gets rejected immediately with `429 Too Many Requests`.

## Key Concepts

### 1. The `Inflight` Struct and `Guard`

The `pingora-limits` crate uses a semaphore-like pattern.

* `incr(key)`: Increments the counter and returns a `Guard`.
* `Guard`: This is a "ticket." As long as this object exists in memory, the "slot" is occupied.
* **Automatic Cleanup:** When the `Guard` is dropped (goes out of scope), the counter is automatically decremented.

### 2. The Context (`CTX`) Bridge

Since a request takes time to complete, we need to hold onto the `Guard` from the beginning of the request until the end.
We define a custom Context struct (`InflightCtx`) to store this guard.

* **Request Start (`request_filter`):** We acquire the guard and move it into `ctx.guard`.
* **Request End:** Pingora drops the `ctx`, which drops the `guard`, which frees the slot.

## The Code (`examples/38_inflight_limit.rs`)

We intentionally add a **2-second sleep** in `upstream_peer` to simulate a slow backend. This ensures requests overlap in time so we can verify the limit.

```rust
use async_trait::async_trait;
use log::{info, warn};
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;

use pingora_limits::inflight::{Inflight, Guard};
use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;
use std::time::Duration;

// 1. Global In-flight Limiter State
// Counts active connections. Does not rely on time windows.
static INFLIGHT_LIMITER: Lazy<Inflight> = Lazy::new(|| Inflight::new());

// Limit: Max 2 simultaneous requests
const MAX_CONCURRENT_REQ: isize = 2;

// 2. The Context
// This struct holds the "ticket" (Guard) for the duration of the request.
pub struct InflightCtx {
    pub guard: Option<Guard>,
}

pub struct InflightLimitProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for InflightLimitProxy {
    type CTX = InflightCtx;

    fn new_ctx(&self) -> Self::CTX {
        InflightCtx { guard: None }
    }

    // 3. The Security Filter
    async fn request_filter(
        &self, 
        session: &mut Session,
        ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // Define the scope of the limit (Global vs Per-IP)
        let key = "global_limit";

        // Increment the counter. Returns the guard and the *new* count.
        let (guard, count) = INFLIGHT_LIMITER.incr(key, 1);

        // 4. Check Limit
        if count > MAX_CONCURRENT_REQ {
            warn!("In-flight Limit Exceeded {}/{}", count, MAX_CONCURRENT_REQ);
            
            session.respond_error(429).await?;
            
            // CRITICAL: We do NOT put the guard into 'ctx'.
            // When this function returns, 'guard' goes out of scope here.
            // Rust calls drop(guard), which decrements the counter immediately.
            return Ok(true);
        }

        // 5. Acquire Slot
        // Move the guard into the context. It will stay there until the 
        // request completes (success or failure).
        ctx.guard = Some(guard);

        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        // SIMULATION: Artificial 2s delay.
        // This forces requests to overlap in time, triggering the concurrency limit.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let peer = Box::new(HttpPeer::new(upstream, false, "inflight.cluster".to_string()));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, InflightLimitProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6182");

    info!("In-flight Limiter running on 0.0.0.0:6182");
    info!("Max Concurrent Requests: {}", MAX_CONCURRENT_REQ);
    info!("Note: Upstream connection has artificial 2s delay to simulate load.");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

To verify this, we need to send requests faster than the server can finish them (which is easy, because the server takes 2 seconds).

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 38_inflight_limit
```

### 2. Run Parallel Requests

We use a bash loop to fire 5 requests simultaneously into the background (`&`) and then wait for them all to finish.

```bash
docker exec -it pingora_client_1 bash -c 'for i in {1..5}; do curl -s -o /dev/null -w "%{http_code}\n" http://172.28.0.10:6182/ & done; wait'
```

### 3. Analyze Output

You should see a mix of `200` and `429`:

```text
429
429
429
200
200
```

* **200s:** Two requests grabbed the slots and slept for 2 seconds.
* **429s:** Three requests arrived, saw the limit `3/2` (or `4/2`, `5/2`), and were rejected instantly.

### 4. Check Logs

The logs confirm the rejection:

```text
[WARN] In-flight Limit Exceeded 3/2
[WARN] In-flight Limit Exceeded 3/2
[WARN] In-flight Limit Exceeded 3/2
```

This proves the proxy is actively protecting your backend from concurrency overload.

# Lesson 39: IP Filtering (Access Control)

In network security, one of the most basic but effective defenses is the Access Control List (ACL). You simply maintain a list of who is allowed in and who is not.

While specialized firewalls (like `iptables` or AWS Security Groups) often handle this, doing it at the proxy level gives you finer control. You can log the attempts, return custom error pages, or dynamically update the blocklist without touching the OS kernel.

In this lesson, we implement a simple **IP Blocklist**:

* **Client 1 (`172.28.0.30`):** Allowed.
* **Client 2 (`172.28.0.31`):** Blocked.

## Key Concepts

### 1. `request_filter` vs. `ConnectionFilter`

* **`request_filter` (Layer 7):** This runs after the TCP handshake and TLS termination. We have access to the full HTTP request.
* *Pros:* Can return a polite `403 Forbidden` HTML page. Can log detailed metadata (User-Agent, Path).
* *Cons:* Slightly more resource-intensive as we parse the request first.
* *Usage:* We use this today.


* **`ConnectionFilter` (Layer 4):** This runs during the TCP handshake.
* *Pros:* extremely fast. Drops the packet instantly.
* *Cons:* The user sees a "Connection Reset" error, not a helpful HTTP message.
* *Note:* While effective, the Layer 4 API in Pingora is advanced/experimental, so we will focus on Layer 7 filtering which is the standard for application proxies.

### 2. Implementation Logic

1. **Extract IP:** Get the IP from the session.
2. **Match:** Check against our "Bad Actor" list.
3. **Reject:** If matched, send `403` and return `Ok(true)` to stop processing.

## The Code (`examples/39_connection_filter.rs`)

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

pub struct FirewallProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for FirewallProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    // 1. The Access Control Logic
    // This hook runs immediately after the request headers are parsed.
    async fn request_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // Extract Client IP
        // We unwrap the Inet address to get a comparable IP object.
        let client_ip = match session.client_addr() {
            Some(addr) => addr.as_inet().unwrap().ip(),
            None => {
                warn!("Unknown client address, allowing...");
                return Ok(false);
            }
        };

        // 2. Define Blocklist
        // In a production scenario, this would likely be an optimized
        // Set (HashSet) or an IP CIDR matcher (IpNet).
        let bad_actor_ip = "172.28.0.31".parse::<std::net::IpAddr>().unwrap();

        // 3. Check and Block
        if client_ip == bad_actor_ip {
            warn!("Access Denied for IP: {}", client_ip);
            
            // Return 403 Forbidden
            // This sends a standard error page to the client.
            session.respond_error(403).await?;
            
            // Short-circuit: Return `true` to signal that the request 
            // has been fully handled and should NOT proceed to the upstream.
            return Ok(true);
        }

        // Allow legitimate traffic
        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "firewall.cluster".to_string()));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, FirewallProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6183");

    info!("Firewall Proxy running on 0.0.0.0:6183");
    info!("Blocking Bad Actor: 172.28.0.31");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will confirm that Client 1 is allowed and Client 2 is blocked.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 39_connection_filter
```

### 2. Test Client 1 (Allowed)

Run this command from your host machine (assuming the docker setup):

```bash
docker exec -it pingora_client_1 curl -v http://172.28.0.10:6183/
```

* **Result:** `HTTP/1.1 200 OK`
* **Log:** No warnings. Normal routing.

### 3. Test Client 2 (Blocked)

```bash
docker exec -it pingora_client_2 curl -v http://172.28.0.10:6183/
```

* **Result:** `HTTP/1.1 403 Forbidden`
* **Log:**
   ```text
   [WARN] Access Denied for IP: 172.28.0.31
   ```

This confirms the IP filter is actively protecting your service.

# Lesson 40: Request Size Limiting

Handling large file uploads is resource-intensive. If a client starts uploading a 10GB video to an endpoint meant for 1KB JSON payloads, your server waits, buffers, and eventually crashes or runs out of bandwidth.

Pingora allows us to enforce limits at the edge. While `pingora-web` (the higher-level framework) has global settings for this, implementing it manually in `pingora-proxy` gives us fine-grained control—for example, allowing 500MB on `/upload` but only 2KB on `/login`.

In this lesson, we implement a **Dual-Layer Defense**:

1. **Fast Path:** Check the `Content-Length` header. If it says "1GB", reject immediately.
2. **Slow Path:** If the client lies or uses `Transfer-Encoding: chunked` (no total size declared), we count the bytes as they stream in and cut the connection if they exceed the limit.

## Key Concepts

### 1. `request_body_filter`

This is a new hook in the `ProxyHttp` trait. It runs *for every chunk* of data received from the client.

* **Input:** `body: &mut Option<Bytes>`.
* **Action:** We inspect the size, increment a counter in our `CTX`, and decide whether to allow it or return an error.

### 2. `fail_to_proxy`

If `request_body_filter` returns an error, Pingora aborts the upstream connection. However, by default, it might just close the socket or send a 502. To send a proper `413 Payload Too Large` to the client, we must implement the `fail_to_proxy` hook to catch our specific error and format the response.

## The Code (`examples/40_request_size_limit.rs`)

We set an artificially low limit of **100 bytes** to make testing easy.

```rust
use async_trait::async_trait;
use bytes::Bytes;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::proxy::FailToProxy;
use http::header::CONTENT_LENGTH;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

// Max 100 bytes (Artificially low for testing)
const MAX_BODY_SIZE: usize = 100;

// 1. Context to track streaming body size
// This persists across multiple calls to request_body_filter
pub struct SizeCtx {
    pub bytes_read: usize,
}

pub struct SizeLimitProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for SizeLimitProxy {
    type CTX = SizeCtx;

    fn new_ctx(&self) -> Self::CTX {
        SizeCtx { bytes_read: 0 }
    }

    // 2. Filter 1: Check Content-Length Header
    // This catches large requests EARLY, before we accept the body payload.
    async fn request_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(value) = session.req_header().headers.get(CONTENT_LENGTH) {
            if let Ok(len_str) = value.to_str() {
                if let Ok(len) = len_str.parse::<usize>() {
                    if len > MAX_BODY_SIZE {
                        warn!("Rejecting request by Header: Content-Length: {} > {}", len, MAX_BODY_SIZE);
                        session.respond_error(413).await?;
                        return Ok(true); // Short-circuit
                    }
                }
            }
        }
        Ok(false)
    }

    // 3. Filter 2: Check Streaming Body
    // This catches "Transfer-Encoding: chunked" or lying Content-Length headers.
    async fn request_body_filter(
        &self,
        _session: &mut Session,
        body: &mut Option<Bytes>,
        _end_of_stream: bool,
        ctx: &mut Self::CTX
    ) -> Result<()>
    where
        Self::CTX: Send + Sync,
    {
        if let Some(b) = body {
            ctx.bytes_read += b.len();
            if ctx.bytes_read > MAX_BODY_SIZE {
                warn!("Rejecting request by Stream: Accumulated {} bytes > {}", ctx.bytes_read, MAX_BODY_SIZE);
                // We return a Custom error here. Standard errors would cause a 502.
                // We need `fail_to_proxy` to convert this into a 413.
                return Err(Error::explain(ErrorType::Custom("BodyTooLarge"), "Stream exceeded limit"));
            }
        }
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "size-limit.cluster".to_string()));
        Ok(peer)
    }

    // 4. Handle Streaming Errors
    // If request_body_filter raises an error, it ends up here.
    async fn fail_to_proxy(
        &self,
        session: &mut Session,
        e: &Error,
        _ctx: &mut Self::CTX
    ) -> FailToProxy
    where
        Self::CTX: Send + Sync,
    {
        // Convert our custom error into a proper HTTP 413 response
        if let ErrorType::Custom("BodyTooLarge") = e.etype {
            let _ = session.respond_error(413).await;
            return FailToProxy {
                error_code: 413,
                can_reuse_downstream: false,
            };
        }
        
        FailToProxy {
            error_code: 502,
            can_reuse_downstream: false,
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, SizeLimitProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6184");

    info!("Request Size Limiter running on 0.0.0.0:6184");
    info!("Max Body Size: {} bytes", MAX_BODY_SIZE);

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify both the fast header check and the slow streaming check.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 40_request_size_limit
```

### 2. Valid Request

Send "tiny" (4 bytes).

```bash
curl -v -d "tiny" http://172.28.0.10:6184/
```

* **Result:** `200 OK`.

### 3. Header Rejection (Fast Path)

We create a ~150 byte payload. `curl` automatically adds the `Content-Length: 150` header.

```bash
# Generate 150 'a' characters
payload=$(printf 'a%.0s' {1..150})
curl -v -d "$payload" http://172.28.0.10:6184/
```

* **Result:** `413 Payload Too Large` (Immediate).
* **Logs:** `Rejecting request by Header: Content-Length: 150 > 100`.

### 4. Streaming Rejection (Slow Path)

We force `chunked` encoding so `curl` does *not* send a `Content-Length`. Pingora must read the body to find the size.

```bash
curl -v -H "Transfer-Encoding: chunked" -d "$payload" http://172.28.0.10:6184/
```

* **Result:** `413 Payload Too Large` (After uploading).
* **Logs:** `Rejecting request by Stream: Accumulated 150 bytes > 100`.

This proves your proxy cannot be fooled by omitting the length header.

# Lesson 41: Authentication (Bearer Token)

One of the most powerful patterns in modern architecture is **Authentication Offloading** (or "Auth at the Edge").

Instead of requiring every single microservice to implement token validation, logic handling, and error formatting, you enforce it once at the Proxy/Gateway level. If a request reaches your upstream service, that service can assume the user is already authenticated.

In this lesson, we implement a simple API Gateway authentication check:

* **No Header?** -> `401 Unauthorized`.
* **Wrong Token?** -> `403 Forbidden`.
* **Correct Token?** -> Allowed (`200 OK`).

## Key Concepts

### 1. HTTP 401 vs 403

It is important to be semantically correct with HTTP errors:

* **401 Unauthorized:** The user has not provided credentials. The client should prompt the user to log in.
* **403 Forbidden:** The user provided credentials, but they are invalid or insufficient. Re-trying the same credentials will not work.

### 2. Header Inspection

We use `session.req_header().headers.get("Authorization")` to access the raw header value.

* **Performance Tip:** We compare the value as **bytes** (`as_bytes()`) rather than converting it to a Rust `String` (`to_str()`). This avoids unnecessary memory allocation and UTF-8 validation overhead, which is crucial for high-throughput gateways.

## The Code (`examples/41_auth_request.rs`)

We enforce a hardcoded token `Bearer super-secret-token`. In a real app, you might validate a JWT signature or query a Redis cache here.

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

// The shared secret (Hardcoded for this lesson)
const SECRET_TOKEN: &[u8] = b"Bearer super-secret-token";

pub struct AuthProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for AuthProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    // 1. Authentication Logic
    // Runs before upstream connection.
    async fn request_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        // Extract Authorization Header
        let auth_header = session.req_header().headers.get("Authorization");

        match auth_header {
            // Case A: Header Missing -> 401 Unauthorized
            None => {
                warn!("Auth Failed: Missing Authorization header");
                session.respond_error(401).await?;
                return Ok(true); // Stop processing
            }
            // Case B: Header Present -> Check Token
            Some(value) => {
                // Direct byte comparison (fast)
                if value.as_bytes() != SECRET_TOKEN {
                    warn!("Auth Failed: Invalid Token");
                    session.respond_error(403).await?;
                    return Ok(true); // Stop processing
                }
            }
        }

        // Case C: Valid Token -> Allow
        info!("Auth Success: Valid Token");
        Ok(false) // Continue to upstream_peer
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "auth-protected.cluster".to_string()));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, AuthProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6185");

    info!("Auth Proxy running on 0.0.0.0:6185");
    info!("Required Token: Bearer super-secret-token");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will cycle through the three authentication states to ensure strict enforcement.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 41_auth_request
```

### 2. Test A: Missing Header (401)

```bash
docker exec pingora_client_1 curl -v -o /dev/null http://172.28.0.10:6185/
```

* **Result:** `HTTP 401 Unauthorized`.
* **Logs:** `[WARN] Auth Failed: Missing Authorization header`.

### 3. Test B: Invalid Token (403)

```bash
docker exec pingora_client_1 curl -v -o /dev/null -H "Authorization: Bearer bad-token" http://172.28.0.10:6185/
```

* **Result:** `HTTP 403 Forbidden`.
* **Logs:** `[WARN] Auth Failed: Invalid Token`.

### 4. Test C: Valid Token (200)

```bash
docker exec pingora_client_1 curl -v -o /dev/null -H "Authorization: Bearer super-secret-token" http://172.28.0.10:6185/
```

* **Result:** `HTTP 200 OK`.
* **Logs:** `[INFO] Auth Success: Valid Token`.

The upstream service is now protected. No request reaches the backend unless it has the secret key.

# Lesson 42: Basic Authentication

In Lesson 41, we implemented Bearer Token authentication, where the client is expected to know the secret beforehand.

**Basic Authentication** follows a different flow called "Challenge-Response." If a user visits your site without credentials, the server must **challenge** them to provide a username and password. This is what triggers the native login popup in web browsers.

In this lesson, we implement this flow:

1. **Challenge:** If the request has no credentials, return `401 Unauthorized` **AND** a `WWW-Authenticate` header.
2. **Verify:** If the request returns with credentials, valid them against our stored user (`admin:password`).

## Key Concepts

### 1. The `WWW-Authenticate` Header

Simply returning `401` isn't enough. Without the `WWW-Authenticate` header, a browser will simply display a generic error page.

* **Header:** `WWW-Authenticate: Basic realm="PingoraProxy"`
* **Behavior:** This tells the browser, "I need Basic credentials for the realm 'PingoraProxy'." The browser then opens the username/password dialog.

### 2. Manual Response Construction

The helper method `session.respond_error(401)` is convenient, but it doesn't allow us to inject custom headers like `WWW-Authenticate`.
To solve this, we must build the response manually using `ResponseHeader::build`, insert our custom header, and send it using `session.write_response_header`.

### 3. Base64 Encoding

Basic Auth credentials are sent as `username:password` encoded in Base64.

* **User:** `admin`
* **Pass:** `password`
* **String:** `admin:password`
* **Base64:** `YWRtaW46cGFzc3dvcmQ=`
* **Header:** `Authorization: Basic YWRtaW46cGFzc3dvcmQ=`

## The Code (`examples/42_basic_auth.rs`)

To keep our dependencies minimal, we perform a direct string comparison against the pre-calculated Base64 string rather than importing a Base64 library.

```rust
use async_trait::async_trait;
use log::{info, warn};
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader; // Required for building custom headers

use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use std::sync::Arc;

// Expected Header: "Authorization: Basic YWRtaW46cGFzc3dvcmQ="
// We compare the raw base64 string directly for performance, 
// avoiding the need to import a base64 decoding crate.
const EXPECTED_AUTH: &[u8] = b"Basic YWRtaW46cGFzc3dvcmQ=";

pub struct BasicAuthProxy(Arc<LoadBalancer<RoundRobin>>);

#[async_trait]
impl ProxyHttp for BasicAuthProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    async fn request_filter(
        &self,
        session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<bool>
    where
        Self::CTX: Send + Sync,
    {
        let auth_header = session.req_header().headers.get("Authorization");

        match auth_header {
            // Case A: Missing Credentials -> Send Challenge
            None => {
                warn!("Auth Failed: Missing Header. Sending Challenge");
                
                // 1. Build a raw 401 response
                let mut header = ResponseHeader::build(401, Some(3)).unwrap();
                
                // 2. Inject the mandatory WWW-Authenticate header
                // This tells the client (browser) to prompt for "Basic" credentials.
                header.insert_header("WWW-Authenticate", "Basic realm=\"PingoraProxy\"").unwrap();
                header.insert_header("Content-Length", "0").unwrap();

                // 3. Write response and close stream (true = End of Stream)
                session.write_response_header(Box::new(header), true).await?;
                
                return Ok(true); // Stop processing
            }
            // Case B: Header Present -> Validate
            Some(value) => {
                if value.as_bytes() != EXPECTED_AUTH {
                    warn!("Auth Failed: Wrong Credentials");
                    // Return 403 to indicate credentials were received but rejected
                    session.respond_error(403).await?;
                    return Ok(true);
                }
            }
        }

        // Case C: Success
        info!("Auth Success: admin:password verified");
        Ok(false)
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        let upstream = self.0
            .select(b"", 256)
            .ok_or_else(|| Error::explain(ErrorType::Custom("NoUpstreamAvailable"), "Empty upstream pool"))?;

        let peer = Box::new(HttpPeer::new(upstream, false, "basic-auth.cluster".to_string()));
        Ok(peer)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    let upstreams = LoadBalancer::try_from_iter([
        "172.28.0.20:8080",
        "172.28.0.21:8080",
    ])?;

    let mut my_proxy = http_proxy_service(&my_server.configuration, BasicAuthProxy(Arc::new(upstreams)));
    my_proxy.add_tcp("0.0.0.0:6186");

    info!("Basic Auth Proxy running on 0.0.0.0:6186");
    info!("Credentials: admin / password");

    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify the Challenge, the Failure, and the Success states.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 42_basic_auth
```

### 2. Test A: The Challenge (Missing Credentials)

```bash
curl -v http://172.28.0.10:6186/
```

* **Result:** `401 Unauthorized`.
* **Crucial Header:** Look for `< WWW-Authenticate: Basic realm="PingoraProxy"`. If this is missing, a browser will not show the login popup.

### 3. Test B: The Rejection (Wrong Password)

```bash
curl -v -u "admin:wrong" http://172.28.0.10:6186/
```

* **Result:** `403 Forbidden`.
* **Log:** `[WARN] Auth Failed: Wrong Credentials`.

### 4. Test C: The Success (Correct Password)

```bash
curl -v -u "admin:password" http://172.28.0.10:6186/
```

* **Result:** `200 OK`.
* **Log:** `[INFO] Auth Success: admin:password verified`.

# Module 6: Caching

The fastest network request is the one you never make.

In high-scale systems, 80% of the traffic often hits just 20% of the content. Repeatedly asking your backend database to "Get User Profile 123" or "Render Home Page" burns CPU cycles and latency unnecessarily.

**Module 6** introduces the **Pingora Cache** system. We will transform our proxy from a simple router into a high-performance content delivery node.

We will cover:

* **Storage Backends:** How to store responses in memory (RAM) to serve them in microseconds.
* **Cache Control:** How to obey standard HTTP headers (`Cache-Control`, `Expires`) so the proxy knows *what* to cache and for *how long*.
* **Request Coalescing:** The "Thundering Herd" problem—what happens when 1,000 users ask for the same missing file at the exact same millisecond? We will learn how to "lock" the cache so only *one* request hits the origin while the others wait.
* **Advanced Patterns:** Implementing `PURGE` to instantly clear content and `stale-while-revalidate` to update content in the background without slowing down users.

By the end of this module, you will have built a caching layer capable of drastically reducing the load on your upstream servers.

# Lesson 43: In-Memory Caching

The fastest way to serve a request is to avoid sending it to the backend at all. **Caching** allows the proxy to store the response from an upstream server (like Blue or Green) and serve it to subsequent users directly from memory.

In this lesson, we implement a **Basic In-Memory Cache**.
To demonstrate this reliably, we will spin up a small internal "Mock Upstream" that returns a response with `Cache-Control: max-age=60`.

## Key Concepts

### 1. The Caching Lifecycle

Pingora does not cache by default. You must opt-in via two distinct hooks:

1. **`request_cache_filter` (The Setup):**
   * Runs *before* the upstream connection.
   * **Action:** Call `session.cache.enable()`. This selects the storage backend (e.g., Memory) and sets the **Cache Key** (e.g., the URL path).
   * If you skip this, caching is disabled for the request.
2. **`response_cache_filter` (The Decision):**
   * Runs *after* the upstream responds but *before* sending headers to the client.
   * **Action:** Determine if the response is worth keeping. Usually, this involves parsing the `Cache-Control` header.

### 2. `MemCache` Backend

Pingora provides `pingora_cache::memory::MemCache`, a high-performance in-memory storage. It is ephemeral (lost on restart) but extremely fast, making it ideal for hot content.

## The Code (`examples/43_memory_cache.rs`)

We include a helper function `run_mock_upstream` that listens on port `6191`. It mimics a backend that explicitly allows caching via headers.

```rust
use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

// Imports for Caching
use pingora_cache::{MemCache, CacheKey, CacheMetaDefaults, RespCacheable, CachePhase};
use pingora_cache::cache_control::CacheControl;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::borrow::Cow;

// 1. Initialize Global Memory Storage
static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);

pub struct CacheProxy;

#[async_trait]
impl ProxyHttp for CacheProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    // 2. Enable Caching for the Request
    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        // Define the lookup key: we use the URL path.
        let key = CacheKey::new("", session.req_header().uri.path(), "");
        
        // Enable cache using our MemCache backend.
        // Args: (Storage, EvictionManager, Predictor, Lock, Stats)
        session.cache.enable(&*MEM_CACHE, None, None, None, None);
        session.cache.set_cache_key(key);
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Route to our internal mock upstream
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6191), false, "cache.local".to_string()));
        Ok(peer)
    }

    // 3. Decide if Response is Cacheable
    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX,
    ) -> Result<RespCacheable> {
        // Parse the upstream headers (Cache-Control, Expires, etc.)
        let cc = CacheControl::from_resp_headers(resp);

        // Define defaults if headers are missing (e.g., cache 200 OK for 60s)
        let defaults = CacheMetaDefaults::new(
            |status| if status.as_u16() == 200 { Some(Duration::from_secs(60)) } else { None },
            0,
            0
        );

        // Return the decision
        Ok(pingora_cache::filters::resp_cacheable(
            cc.as_ref(),
            resp.clone(),
            false,
            &defaults,
        ))
    }

    // 4. Log Cache Status
    async fn logging(
        &self,
        session: &mut Session,
        _e: Option<&Error>,
        _ctx: &mut Self::CTX,
    ) {
        match session.cache.phase() {
            CachePhase::Hit => info!("Cache Status: HIT (Served from Memory)"),
            CachePhase::Miss => info!("Cache Status: MISS (Fetched from Upstream)"),
            CachePhase::Expired => info!("Cache Status: EXPIRED (Revalidating)"),
            _ => {}
        }
    }
}

// Internal Mock Server to ensure consistent Cache-Control headers
async fn run_mock_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6191").await.unwrap();
    info!("Mock Upstream started on 127.0.0.1:6191");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;

                let body = "Response from Local Mock";
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Cache-Control: public, max-age=60\r\n\
                    Connection: close\r\n\
                    \r\n\
                    {}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    // Required for cache metadata compression
    pingora_cache::set_compression_dict_content(Cow::Borrowed(b""));

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    // Start the mock upstream in a background thread
    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_mock_upstream());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, CacheProxy);
    my_proxy.add_tcp("0.0.0.0:6190");

    info!("Memory Cache Proxy running on 0.0.0.0:6190");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify the entire lifecycle: **Miss** (First fetch) -> **Hit** (Memory serve) -> **Expired** (Re-fetch).

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 43_memory_cache
```

### 2. First Request (Miss)

```bash
docker exec pingora_client_1 curl -s http://172.28.0.10:6190/
```

* **Output:** `Response from Local Mock`
* **Log:** `Cache Status: MISS (Fetched from Upstream)`

### 3. Second Request (Hit)

Run the same command immediately.

* **Output:** `Response from Local Mock`
* **Log:** `Cache Status: HIT (Served from Memory)`
* **Observation:** The proxy did *not* connect to the backend (port 6191). It served the data instantly from RAM.

### 4. Expired Request

Wait 60 seconds (the `max-age` defined in the mock), then run the command again.

* **Output:** `Response from Local Mock`
* **Log:** `Cache Status: EXPIRED (Revalidating)`
* **Observation:** Pingora detected the TTL had passed, fetched a fresh copy from the backend, and updated the cache.

## Theory

The Pingora caching system transforms your proxy from a simple traffic router into a sophisticated content delivery engine. To effectively use it, we must understand the specific Rust structures that manage the "Hit or Miss" decision process.

Here is a detailed breakdown of the key components and syntax used in our code.

### 1. The Core Components

#### **MemCache**

* **Definition:** An in-memory implementation of the `Storage` trait provided by `pingora-cache`.
* **Role:** It acts as the backend "hard drive" for the cache, but stores data in RAM (using a specialized hash map) instead of on disk.
* **Key Characteristics:**
  * **Volatile:** Since it lives in RAM, all cache data is lost if the proxy process restarts.
  * **Fast:** Lookups avoid disk I/O entirely, offering microsecond-level latency.
  * **Concurrency:** It uses `RwLock` (Read-Write Lock) mechanisms to safely allow multiple concurrent readers while ensuring data integrity during writes.
  * **Storage Logic:** It maps a hashed `CacheKey` to a `CacheObject`, which contains both the metadata header (expiry, headers) and the body payload.

#### **CacheKey**

* **Definition:** A struct that uniquely identifies a cacheable asset.
* **Role:** It functions as the lookup address for the storage backend. If two different user requests generate the same `CacheKey`, they will map to the same cached object.
* **Composition:**
  1. **Primary Key:** Usually derived from the URL path (e.g., `/images/logo.png`).
  2. **Namespace:** An optional prefix to isolate distinct datasets (e.g., `v1` vs `v2` of an API).
  3. **User Tag:** An optional tag to shard storage by user (e.g., specific cache partitions for specific customers).
  4. **Variance:** Handles `Vary` headers. For example, it ensures a request with `Accept-Encoding: gzip` gets a compressed version, while `identity` gets a plain version, even if the URL is the same.


* **Hashing:** Internally, Pingora uses the **Blake2b** algorithm to generate a 128-bit binary hash from these components for efficient storage lookup.

#### **CacheMetaDefaults**

* **Definition:** A configuration struct that defines the "fallback" behavior.
* **Role:** Upstream servers are often imperfect and forget to send `Cache-Control` headers. This struct acts as the proxy's internal policy engine to decide what to do in those cases.
* **Key Fields:**
  * `fresh_sec_fn`: A function mapping an HTTP Status Code to a Duration. (e.g., "If `200 OK`, cache for 60s. If `500 Error`, do not cache").
  * `stale_while_revalidate_sec`: Defines the window where it is acceptable to serve expired content while a background fetch refreshes the data.
  * `stale_if_error_sec`: Defines how long to serve old content if the upstream server goes down.

#### **RespCacheable**

* **Definition:** An enum representing the final verdict on whether a specific response *can* be cached.
* **Role:** This is the output of your logic in `response_cache_filter`.
* **Variants:**
  1. **`Cacheable(CacheMeta)`**: The verdict is **Yes**. The struct contains the calculated metadata (expiry timestamp, headers to preserve).
  2. **`Uncacheable(NoCacheReason)`**: The verdict is **No**. It includes the specific reason (e.g., `OriginNotCache` if the upstream sent `no-store`, or `ResponseTooLarge`).

#### **CachePhase**

* **Definition:** An enum tracking the lifecycle state of a request relative to the cache system.
* **Role:** It acts as a state machine indicator, primarily useful for observability (logging/metrics) and debugging.
* **Common States:**
  * `Disabled`: The cache was never enabled for this request.
  * `Miss`: Content was not found in storage; currently fetching from upstream.
  * `Hit`: Content was found and is fresh; serving directly from cache.
  * `Stale`: Content was found but has expired; a revalidation request is required.
  * `Revalidated`: Stale content was confirmed as still valid by the upstream (via a `304 Not Modified` response).

#### **CacheControl**

* **Definition:** A parser for the standard HTTP `Cache-Control` header.
* **Role:** It takes raw header strings (e.g., `public, max-age=3600, no-transform`) and parses them into a structured `DirectiveMap`.
* **Capabilities:**
  * Extracts critical directives like `max-age`, `s-maxage`, `no-store`, and `private`.
  * Handles edge cases like quoting and case-insensitivity (e.g., `Max-Age="60"`).
  * Implements `InterpretCacheControl` to calculate actual Time-To-Live (TTL) durations according to strict RFC 9111 rules.

---

### 2. Explanation of `&*` in `enable`

You may have noticed this specific syntax in the code:

```rust
static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);
// ...
session.cache.enable(&*MEM_CACHE, ...);
```

This `&*` pattern is a Rust idiom used to satisfy type coercion and lifetime requirements when dealing with `Lazy` static variables.

1. **The Type of `MEM_CACHE`:**
   `MEM_CACHE` is declared as `Lazy<MemCache>`. It is not a `MemCache` struct directly; it is a wrapper that ensures initialization happens only on first access.
2. **The Dereference (`*`):**
   `Lazy<T>` implements the `Deref` trait.
   * `*MEM_CACHE` dereferences the `Lazy` wrapper to access the underlying value.
   * Effectively, this operation extracts the actual `MemCache` instance inside the static variable.
3. **The Reference (`&`):**
   The `&` operator then takes a reference to that dereferenced value.
   * So, `&*MEM_CACHE` results in a reference of type `&MemCache`.
4. **The Target Type (`dyn Storage`):**
   The `enable` function signature expects a reference to a Trait Object:
   ```rust
   pub fn enable(&mut self, storage: &'static (dyn Storage + Sync), ...)
   ```
   * Since `MemCache` implements the `Storage` trait, Rust automatically coerces the concrete `&MemCache` into the dynamic `&dyn Storage` interface.
5. **Why not just pass `&MEM_CACHE`?**
   If you tried to pass `&MEM_CACHE`, you would be passing a reference to the wrapper itself (`&Lazy<MemCache>`). The `Lazy` struct does *not* implement the `Storage` trait; only the inner `MemCache` does. Therefore, you must manually peel off the wrapper (`*`) and then reference the inner object (`&`).
   
**Summary:** `&*MEM_CACHE` translates to: "Give me a reference to the *actual initialized cache* inside the global wrapper."

# Lesson 44: Respecting Cache-Control Headers

In Lesson 43, we manually forced the proxy to cache everything for 60 seconds. While useful for testing, a production proxy should never do this. It should respect the **origin server's** instructions.

If an upstream server sends `Cache-Control: no-store` (e.g., banking data) or `max-age=5` (e.g., stock tickers), the proxy must obey. Pingora provides built-in parsers to handle strict RFC compliance out of the box.

In this lesson, we implement a **Smart Caching Proxy** that adapts its behavior based on the URL:

* `/short` -> Cache for 5 seconds.
* `/long` -> Cache for 60 seconds.
* `/no_store` -> Do not cache at all.

## Key Concepts

### 1. The `Cache-Control` Header

This is the standard mechanism for web caching.

* `max-age=N`: The content is fresh for N seconds.
* `no-store`: The content must **never** be stored in non-volatile storage.
* `private`: The content is for a specific user (e.g., a profile page) and should not be stored by a shared proxy.

### 2. RFC Compliance via `resp_cacheable`

Pingora provides a helper function `pingora_cache::filters::resp_cacheable`.
Instead of writing complex `if` statements (e.g., "if header contains 'no-store'..."), you pass the parsed headers to this function. It returns:

* `RespCacheable::Cacheable(meta)`: If valid, with the calculated TTL.
* `RespCacheable::Uncacheable(reason)`: If invalid (e.g., `origin_no_store`, `response_too_large`).

## The Code (`examples/44_cache_control.rs`)

We update our mock upstream to be "dynamic"—it reads the request path and changes the response headers accordingly.

```rust
use async_trait::async_trait;
use log::info;
use once_cell::sync::Lazy;
use pingora::prelude::*;
use pingora::server::{configuration::Opt, Server};
use pingora::upstreams::peer::HttpPeer;
use pingora::http::ResponseHeader;

use pingora_cache::{MemCache, CacheKey, CacheMetaDefaults, RespCacheable, CachePhase};
use pingora_cache::cache_control::CacheControl;
use std::borrow::Cow;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static MEM_CACHE: Lazy<MemCache> = Lazy::new(MemCache::new);

pub struct CacheControlProxy;

#[async_trait]
impl ProxyHttp for CacheControlProxy {
    type CTX = ();
    fn new_ctx(&self) -> Self::CTX {}

    // 1. Setup Storage
    fn request_cache_filter(&self, session: &mut Session, _ctx: &mut Self::CTX) -> Result<()> {
        let key = CacheKey::new("", session.req_header().uri.path(), "");
        session.cache.enable(&*MEM_CACHE, None, None, None, None);
        session.cache.set_cache_key(key);
        Ok(())
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX
    ) -> Result<Box<HttpPeer>> {
        // Connect to our dynamic mock upstream
        let peer = Box::new(HttpPeer::new(("127.0.0.1", 6193), false, "header.test.local".to_string()));
        Ok(peer)
    }

    // 2. The Decision Logic
    fn response_cache_filter(
        &self,
        _session: &Session,
        resp: &ResponseHeader,
        _ctx: &mut Self::CTX
    ) -> Result<RespCacheable> {
        // Parse headers using Pingora's built-in parser
        let cc = CacheControl::from_resp_headers(resp);
        
        // Define empty defaults (Cache nothing if headers are missing)
        let defaults = CacheMetaDefaults::new(|_| None, 0, 0);
        
        // Let Pingora decide based on RFC rules
        Ok(pingora_cache::filters::resp_cacheable(
            cc.as_ref(),
            resp.clone(),
            false,
            &defaults
        ))
    }

    async fn logging(&self, session: &mut Session, _e: Option<&Error>, _ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let path = session.req_header().uri.path();
        match session.cache.phase() {
            CachePhase::Hit => info!("Path {}, Status: HIT", path),
            CachePhase::Miss => info!("Path {}, Status: MISS", path),
            CachePhase::Expired => info!("Path {}, Status: EXPIRED", path),
            CachePhase::Disabled(_) => info!("Path {}, Status: SKIP (Uncacheable)", path),
            _ => {}
        }
    }
}

// 3. Dynamic Mock Upstream
// Returns different Cache-Control headers based on the request URL.
async fn run_dynamic_upstream() {
    let listener = TcpListener::bind("127.0.0.1:6193").await.unwrap();
    info!("Dynamic Upstream running on 127.0.0.1:6193");

    loop {
        if let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);

                // Determine Header based on Path
                let cc_header = if req.contains("GET /short") {
                    "max-age=5"
                } else if req.contains("GET /long") {
                    "max-age=60"
                } else if req.contains("GET /no_store") {
                    "no-store"
                } else {
                    "max-age=10"
                };

                let body = format!("Content for {}", cc_header);
                let response = format!(
                    "HTTP/1.1 200 OK\r\n\
                    Content-Length: {}\r\n\
                    Cache-Control: {}\r\n\
                    Connection: close\r\n\
                    \r\n\
                    {}\n",
                    body.len() + 1,
                    cc_header,
                    body
                );

                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    pingora_cache::set_compression_dict_content(Cow::Borrowed(b""));

    let opt = Opt::parse_args();
    let mut my_server = Server::new(Some(opt))?;
    my_server.bootstrap();

    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(run_dynamic_upstream());
    });

    let mut my_proxy = http_proxy_service(&my_server.configuration, CacheControlProxy);
    my_proxy.add_tcp("0.0.0.0:6192");

    info!("Cache Control Proxy running on 0.0.0.0:6192");
    my_server.add_service(my_proxy);
    my_server.run_forever();
}
```

## Verification

We will verify that Pingora adapts its caching strategy dynamically.

### 1. Start the Proxy

```bash
RUST_LOG=info cargo run --example 44_cache_control
```

### 2. Test A: `no-store` (Security Check)

```bash
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/no_store
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/no_store
```

* **Result:** Both requests are fetched from the upstream.
* **Log:** `Path /no_store, Status: SKIP (Uncacheable)`

### 3. Test B: `max-age=5` (Short Lived)

```bash
# Request 1 (Miss)
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/short
# Request 2 (Hit) - Immediate
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/short
# Wait 6 seconds...
sleep 6
# Request 3 (Expired)
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/short
```

* **Log:** `MISS` -> `HIT` -> `EXPIRED` (because 6s > 5s).

### 4. Test C: `max-age=60` (Long Lived)

```bash
# Request 1 (Miss)
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/long
# Wait 6 seconds...
sleep 6
# Request 2 (Hit)
docker exec pingora_client_1 curl -s http://172.28.0.10:6192/long
```

* **Log:** `MISS` -> `HIT` (because 6s < 60s).

This confirms the proxy is correctly parsing and enforcing the upstream's caching policies.