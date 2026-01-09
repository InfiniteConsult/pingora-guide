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

# Lesson 1: Configuration & Lifecycle

In Lesson 0, we built a server that ran with default settings. However, production services rarely run on defaults. They need to define how many worker threads to use, where to write error logs, where to store PID files, and how to handle process upgrades.

Pingora handles this "infrastructure" configuration separately from your traffic handling logic. This separation allows the framework to manage the process lifecycle (daemonization, restarts, upgrades) standardly across all Pingora applications.

### Key Concepts

1. **`Opt`**: This struct represents command-line arguments. Pingora provides a standard parser (via `clap`) that handles flags like `-c` (config file), `-d` (daemon mode), and `-u` (upgrade).
2. **`ServerConf`**: This struct holds the runtime configuration for the server process. It includes settings for:
   * **Threading**: `threads` and `work_stealing`.
   * **Process Management**: `pid_file`, `upgrade_sock`, `user`, `group`.
   * **Logging**: `error_log`.
   * **SSL/Network**: `ca_file`, `upstream_keepalive_pool_size`.
3. **`Server::new`**: This constructor is the bridge. It takes the command-line options (`Opt`), attempts to load the configuration file specified by `-c`, merges it with defaults, and returns a fully initialized `Server` instance.

### The Code (`examples/01_configuration.rs`)

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

### Running the Lesson

#### 1. Define a Configuration File

Create a file at `conf/01_config.yaml` with the following content. We specifically set `threads` to 2 to differentiate it from the default (which is usually 1 or the number of cores depending on environment).

```yaml
---
version: 1
threads: 2
pid_file: "/tmp/pingora_lesson_01.pid"
upgrade_sock: "/tmp/pingora_upgrade_01.sock"
error_log: "/tmp/pingora_error.log"

```

#### 2. Run with Defaults

First, run without arguments. Pingora will use its internal defaults.

```bash
RUST_LOG=info cargo run --example 01_configuration

```

You should see `threads: 1` and `pid_file: /tmp/pingora.pid`.

#### 3. Run with Configuration

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

### Key Concepts

1. **Daemonization Configuration**:
   * **`daemon: true`**: Tells Pingora to fork into the background.
   * **`pid_file`**: The path where the server writes its Process ID. External tools (like `systemd` or `monit`) use this to track and stop the server.
   * **`error_log`**: In daemon mode, `stdout` and `stderr` are closed. This setting redirects logs to a file so they aren't lost.
2. **`BackgroundService`**: A trait for tasks that run continuously until the server shuts down. It receives a `ShutdownWatch` to know when to exit gracefully.
3. **`background_service` Helper**: A utility function in the prelude that wraps your custom logic into a generic service container, saving you from implementing boilerplate.

### The Code (`examples/02_daemon_mode.rs`)

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

### Running the Lesson

To test daemonization, we must use a configuration file, as the behavior changes significantly from the default foreground mode.

#### 1. Define the Daemon Configuration

Create `conf/02_daemon.yaml`. We set `daemon: true` and define paths for logs and the PID file.

```yaml
---
version: 1
daemon: true
pid_file: "/tmp/pingora_02.pid"
error_log: "/tmp/pingora_02.log"
upgrade_sock: "/tmp/pingora_upgrade_02.sock"

```

#### 2. Start the Daemon

Run the server with the configuration.

```bash
RUST_LOG=info cargo run --example 02_daemon_mode -- -c conf/02_daemon.yaml

```

The program will print "Preparing to daemonize..." and then **exit immediately**. This is expected; the parent process exits while the child process continues in the background.

#### 3. Verify Background Execution

The server is now running silently. You can verify this by checking the PID file or listing processes.

```bash
# Read the PID
cat /tmp/pingora_02.pid

# Check if the process exists
ps -p $(cat /tmp/pingora_02.pid)

```

#### 4. Check the Logs

Since the process is detached, you won't see "Beep..." in your terminal. Tail the log file to see the output.

```bash
tail -f /tmp/pingora_02.log

```

You should see the heartbeat messages appearing every second.

#### 5. Stop the Daemon

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

### Key Concepts

* **`ShutdownWatch`**: This is a Tokio `watch` channel provided to every service's `start()` method. Services must monitor this to know when to stop accepting new work and begin their cleanup.
* **`grace_period_seconds`**: A setting in `ServerConf`. It defines the maximum time the server will wait for services to finish after a shutdown signal is received.
* **Signal Handling**: Pingora distinguishes between two types of shutdown:
* **Fast Shutdown (`SIGINT` / Ctrl+C)**: The server exits immediately. Use this during development or emergencies.
* **Graceful Shutdown (`SIGTERM`)**: The server enters the graceful shutdown phase described above. This is the standard signal used by deployment tools like Kubernetes or systemd.



### The Code (`examples/03_graceful_shutdown.rs`)

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

### Running the Lesson

To verify this lesson, we need to send specific signals to the process. We will test both the graceful path and the fast path.

#### 1. Test Graceful Shutdown (The Happy Path)

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

#### 2. Test Fast Shutdown (The Emergency Path)

We want to confirm that we can still force-kill the server if needed.

1. **Start the Server**:
   ```bash
   RUST_LOG=info cargo run --example 03_graceful_shutdown
   ```
2. **Wait for a Job to Start**.
3. **Press** `Ctrl+C`: This sends `SIGINT`.
4. **Observe the Logs**:
   The server should exit **immediately**. You will see `SIGINT received, exiting`, but you will *not* see the "Finishing Job" or "Job completed" messages. The work was abandoned instantly.
