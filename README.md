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

We will verify that both services are running and successfully communicating via the shared state. Since `nc` (Netcat) is useful for testing network services, you may need to install it if you haven't already:

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