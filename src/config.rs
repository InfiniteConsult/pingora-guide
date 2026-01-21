//! # Configuration Module
//!
//! This module defines the configuration schema for the Gateway. It uses `serde`
//! to deserialize a YAML file into strongly-typed structs that control the
//! behavior of the middlewares and upstreams.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `GatewayConf` Struct**:
//!     * The top-level container.
//!     * Fields:
//!         * `http_port`: `Option<u16>` (Default: 8080).
//!         * `admin_port`: `Option<u16>` (Default: 9090).
//!         * `upstreams`: `Vec<UpstreamConf>`.
//!         * `security`: `SecurityConf`.
//!
//! 2.  **Define `UpstreamConf` Struct**:
//!     * Represents a single backend cluster.
//!     * Fields:
//!         * `name`: `String` (e.g., "primary", "auth-service").
//!         * `addrs`: `Vec<String>` (IP:Port list).
//!         * `path_prefix`: `String` (For routing, e.g., "/api/v1").
//!         * `tls`: `bool` (Whether to use HTTPS to upstream).
//!         * `sni`: `Option<String>`.
//!
//! 3.  **Define `SecurityConf` Struct**:
//!     * Represents global security settings.
//!     * Fields:
//!         * `rate_limit`: `Option<i32>` (Req/sec per user).
//!         * `ip_allowlist`: `Option<Vec<String>>` (CIDR blocks).
//!         * `auth_token`: `Option<String>` (Static Bearer token for demo).
//!
//! 4.  **Helper Method**:
//!     * `load_from_yaml(path: &str) -> Result<Self>`:
//!         * specific function to read the file and run `serde_yaml::from_str`.

use std::time::Duration;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// A container for network addresses used by your monitoring tools. It decouples the
/// "Metric" system (Prometheus) from the "Trace" system (OpenTelemetry)
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
pub struct ObservabilityConf {
    pub prometheus_addr: Option<String>,
    pub otlp_endpoint: Option<String>,
}

/// Defines the SSL identity of a server listener. It is a grouping of filesystem paths.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TlsSettings {
    pub cert_path: String,
    pub key_path: String,
    pub mtls_ca_cert: Option<String>
}

fn default_weight() -> u16 { 1 }

/// A single destination server in a static list
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct BackendConf {
    pub address: String,
    #[serde(default = "default_weight")]
    pub weight: u16,
}

/// A toggle to tell the file-watcher which parser to use
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Json,
    Yaml,
}

/// The strategy used to pick the next backend
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancerSelection {
    #[default]
    RoundRobin,
    Random,
    Consistent
}

/// Defines what part of the incoming request is used as the key for Consistent Hashing
/// (Sticky Sessions)
#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum HashSource {
    #[default]
    None,
    ClientIp,
    Uri,
    Header { name: String},
    Cookie { name: String },
}


#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ClusterOptions {
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,

    #[serde(with = "humantime_serde")]
    pub read_timeout: Duration,

    #[serde(with = "humantime_serde")]
    pub write_timeout: Duration,

    #[serde(default="default_timeout", with = "option_humantime")]
    pub idle_timeout: Option<Duration>,
    pub enable_h2: bool,
    pub verify_hostname: bool,

    pub tls: bool,
    pub sni: Option<String>,
    #[serde(default="default_pool_size")]
    pub connection_pool_size: usize,
}

fn default_timeout() -> Option<Duration> { Some(Duration::from_secs(60)) }
fn default_pool_size() -> usize { 4 }

impl ClusterOptions {
    pub fn new(
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
        idle_timeout: Option<Duration>,
        enable_h2: bool,
        verify_hostname: bool,
        tls: bool,
        sni: Option<String>,
        connection_pool_size: usize
    ) -> Self {
        Self {
            connect_timeout,
            read_timeout,
            write_timeout,
            idle_timeout,
            enable_h2,
            verify_hostname,
            tls,
            sni,
            connection_pool_size,
        }
    }
}

impl Default for ClusterOptions {
    fn default() -> Self {
        Self::new(
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Some(Duration::from_secs(60)),
            false,
            true,
            false,
            None,
            default_pool_size()
        )
    }
}

/// The shared timing parameters for any health check (TCP or HTTP). Flattened into the specific health check variants later
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct HealthCheckCommon {
    #[serde(with = "humantime_serde", default = "default_health_interval")]
    pub interval: Duration,
    #[serde(with = "humantime_serde", default = "default_health_timeout")]
    pub timeout: Duration,
    #[serde(default = "default_health_success")]
    pub consecutive_success: usize,
    #[serde(default = "default_health_failure")]
    pub consecutive_failure: usize,
}

fn default_health_interval() -> Duration { Duration::from_secs(5) }
fn default_health_timeout() -> Duration { Duration::from_secs(1) }
fn default_health_success() -> usize { 1 }
fn default_health_failure() -> usize { 1 }

/// A discriminator for how the Router matches the URL path.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PathType {
    #[default]
    Prefix,
    Exact,
    Regex
}

/// A grouping of header manipulation rules
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
pub struct HeaderConf {
    #[serde(default)]
    pub add_req_headers: HashMap<String, String>,
    #[serde(default)]
    pub remove_req_headers: Vec<String>,
    #[serde(default)]
    pub add_resp_headers: HashMap<String, String>,
    #[serde(default)]
    pub remove_resp_headers: Vec<String>,
    #[serde(default)]
    pub preserve_host_header: bool,
}

/// Defines who gets limited. When a request comes in, we extract this key to check the counter in the Token Bucket
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitKey {
    #[default]
    Ip,
    Header(String)
}

/// A simple IP Firewall (Allow/Deny list)
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct AccessControlConf {
    #[serde(default)]
    pub allow: Vec<String>, // List of CIDR ranges allow
    #[serde(default)]
    pub deny: Vec<String>, // List of CIDR ranges to block
}

/// A discriminator for the type of authentication required on a route
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConf {
    Basic {
        realm: String,
        users: HashMap<String, String>
    },
    Request {
        auth_uri: String,
        headers_to_copy: Vec<String>
    },
}

/// Rules for caching HTTP responses in memory
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CacheConf {
    #[serde(default)]
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub default_ttl: Duration,
    #[serde(with = "option_humantime")]
    pub lock_timeout: Option<Duration>,
    #[serde(with = "option_humantime")]
    pub stale_while_revalidate: Option<Duration>,
    #[serde(default)]
    pub enable_purge: bool,
}


/// Represents a single open port on the server
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ListenerConf {
    pub address: String,
    pub tls: Option<TlsSettings>
}

fn default_status_200() -> u16 { 200 }

/// The polymorphic configuration for active health probing. It uses the HealthCheckCommon
/// primitive we defined
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "protocol", rename_all = "snake_case")]
pub enum HealthCheckConf {
    Tcp(HealthCheckCommon),
    Http {
        #[serde(flatten)]
        common: HealthCheckCommon,
        path: String,
        #[serde(default = "default_status_200")]
        expected_status: u16,
    },
    Custom {
        #[serde(flatten)]
        common: HealthCheckCommon,
        command: String,
    }
}

/// Parameters for the token bucket algorithm
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RateLimitConf {
    pub requests_per_sec: u64,
    pub burst: u64,
    #[serde(default)]
    pub key: RateLimitKey,
}

fn default_refresh() -> Duration { Duration::from_secs(60) }

/// Defines the discovery mechanism - where we find the backend IPs
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UpstreamSource {
    Static {
        backends: Vec<BackendConf>
    },
    Dns {
        hostname: String,
        #[serde(with = "humantime_serde", default = "default_refresh")]
        refresh_interval: Duration
    },
    File {
        path: String,
        format: FileFormat,
        #[serde(with = "humantime_serde", default = "default_refresh")]
        refresh_interval: Duration
    },
    Uds {
        path: String,
    }
}

/// The runtime container. It holds the listeners and process settings
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ServerConf {
    #[serde(default)]
    pub listeners: Vec<ListenerConf>,
    #[serde(default)]
    pub daemon: bool,
    pub pid_file: Option<String>,
    pub user: Option<String>,
    pub group: Option<String>,
    pub worker_threads: Option<usize>,
    #[serde(default)]
    pub watch_config: bool,
    pub client_max_body_size: Option<usize>,
    #[serde(default)]
    pub enable_h2: bool,
    #[serde(default)]
    pub enable_h2c: bool,
    #[serde(with = "option_humantime", default)]
    pub graceful_shutdown_timeout: Option<Duration>,
}

/// The "Class" of backend. It combines Discovery + Selection + Health + Connection settings.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct UpstreamConf {
    pub id: String,
    #[serde(flatten)]
    pub source: UpstreamSource,
    #[serde(default)]
    pub selection: LoadBalancerSelection,
    #[serde(default)]
    pub hash_source: HashSource,
    #[serde(default)]
    pub options: ClusterOptions,
    #[serde(default)]
    pub health_check: Option<HealthCheckConf>,
    #[serde(default)]
    pub backup_backends: Vec<BackendConf>,
}

/// The map that connects a Request Path to an Upstream ID
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RouteConf {
    pub path: String,
    #[serde(default)]
    pub path_type: PathType,
    pub upstream_id: String,
    pub hostnames: Option<Vec<String>>,
    pub rate_limit: Option<RateLimitConf>,
    pub auth: Option<AuthConf>,
    #[serde(default)]
    pub headers: HeaderConf,
    pub access_control: Option<AccessControlConf>,
    pub query_matches: Option<HashMap<String, String>>,
    pub strip_query_params: Option<Vec<String>>,
    #[serde(default)]
    pub compression: bool,
    pub body_deny_patterns: Option<Vec<String>>,
    #[serde(default)]
    pub websocket: bool,
    #[serde(default)]
    pub proxy_connect: bool,
    pub inflight_limit: Option<u32>,
    pub error_pages: Option<HashMap<u16, String>>,
    pub cache: Option<CacheConf>,
}

/// The entry point
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GatewayConf {
    pub server: ServerConf,
    pub observability: Option<ObservabilityConf>,
    pub upstreams: Vec<UpstreamConf>,
    pub routes: Vec<RouteConf>,
}

impl GatewayConf {
    pub fn load_from_yaml<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let conf: Self = serde_yaml::from_str(&content)?;
        Ok(conf)
    }
}

mod option_humantime {
    use std::time::Duration;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S> (val: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match val {
            Some(d) => humantime_serde::serialize(d, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D> (deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Val {
            None,
            Some(#[serde(with = "humantime_serde")] Duration),
        }

        let v = Val::deserialize(deserializer)?;
        Ok(match v {
            Val::None => None,
            Val::Some(d) => Some(d),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod cluster_options {
        use super::*;

        #[test]
        fn defaults_are_secure_and_sane() {
            let opt = ClusterOptions::default();
            assert_eq!(opt.connect_timeout, Duration::from_secs(5));
            assert_eq!(opt.read_timeout, Duration::from_secs(60));
            assert_eq!(opt.write_timeout, Duration::from_secs(60));
            assert_eq!(opt.idle_timeout, Some(Duration::from_secs(60)));
            assert!(!opt.enable_h2);
            assert!(opt.verify_hostname);
        }

        #[test]
        fn deserialization_uses_defaults_for_missing_fields() {
            let yaml = r#"enable_h2: true"#;
            match serde_yaml::from_str::<ClusterOptions>(yaml) {
                Ok(opt) => {
                    assert_eq!(opt.connect_timeout, Duration::from_secs(5));
                    assert_eq!(opt.read_timeout, Duration::from_secs(60));
                    assert_eq!(opt.write_timeout, Duration::from_secs(60));
                    assert_eq!(opt.idle_timeout, Some(Duration::from_secs(60)));
                    assert!(opt.enable_h2);
                    assert!(opt.verify_hostname);
                },
                Err(_) => { panic!("Error should not be returned") }
            }
        }

        #[test]
        fn deserialization_parses_human_readable_duration() {
            let yaml = r#"read_timeout: 1m 30s"#;
            match serde_yaml::from_str::<ClusterOptions>(yaml) {
                Ok(opt) => {
                    assert_eq!(opt.read_timeout, Duration::from_secs(90));
                },
                Err(_) => { panic!("Error should not be returned") }
            }
        }

        #[test]
        fn deserialization_handles_explicit_nulls() {
            let yaml = r#"idle_timeout: null"#;
            match serde_yaml::from_str::<ClusterOptions>(yaml) {
                Ok(opt) => {
                    assert_eq!(opt.idle_timeout, None);
                },
                Err(_) => { panic!("Error should not be returned") }
            }
        }

        #[test]
        fn deserialization_overrides_all_fields() {
            let yaml = r#"connect_timeout: 1s
read_timeout: 1s
write_timeout: 1s
idle_timeout: 1s
enable_h2: true
verify_hostname: false"#;
            match serde_yaml::from_str::<ClusterOptions>(yaml) {
                Ok(opt) => {
                    assert_eq!(opt.connect_timeout, Duration::from_secs(1));
                    assert_eq!(opt.read_timeout, Duration::from_secs(1));
                    assert_eq!(opt.write_timeout, Duration::from_secs(1));
                    assert_eq!(opt.idle_timeout, Some(Duration::from_secs(1)));
                    assert!(opt.enable_h2);
                    assert!(!opt.verify_hostname);
                },
                Err(_) => { panic!("Error should not be returned") }
            }
        }
    }

    mod hash_source {
        use super::HashSource;

        #[test]
        fn deserialization_parses_simple_variant() {
            let yaml = r#"type: client_ip"#;
            match serde_yaml::from_str::<HashSource>(yaml) {
                Ok(source) => { assert_eq!(source, HashSource::ClientIp) },
                Err(_) => { panic!("Error should not be returned") }
            }
        }

        #[test]
        fn deserialization_parses_complex_variant() {
            let yaml = "type: header\nname: x-user-id";

            match serde_yaml::from_str::<HashSource>(yaml) {
                Ok(source) => {
                    assert_eq!(source, HashSource::Header { name: "x-user-id".to_string() });
                },
                Err(e) => {
                    println!("{:?}", e);
                    panic!("Error should not be returned")
                }
            }
        }

        #[test]
        fn equality_check_works() {
            let a1 = HashSource::Header { name: "a".to_string() };
            let a2 = HashSource::Header { name: "a".to_string() };
            let b1 = HashSource::Header { name: "b".to_string() };

            assert_eq!(a1, a2);
            assert_ne!(b1, a1);
        }
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_load_full_kitchen_sink_yaml() {
        // Defines the path to the file in the project root
        let path = "gateway_full.yaml";

        // Guard: Check if file exists so the test doesn't panic on CI/clean clones
        if !Path::new(path).exists() {
            eprintln!("Skipping integration test: '{}' not found.", path);
            return;
        }

        // Action: Load via the Factory Method
        println!("Loading configuration from {}...", path);
        let result = GatewayConf::load_from_yaml(path);

        // Assert: It must parse successfully
        match &result {
            Ok(conf) => {
                println!("  Configuration loaded successfully!");
                println!("   Listeners: {}", conf.server.listeners.len());
                println!("   Upstreams: {}", conf.upstreams.len());
                println!("   Routes:    {}", conf.routes.len());
            },
            Err(e) => {
                panic!("  Failed to parse kitchen sink YAML: {}", e);
            }
        }

        // Basic Sanity Checks (Verification without deep inspection)
        let conf = result.unwrap();
        println!("GatewayConf {:?}", conf);
        assert_eq!(conf.server.listeners.len(), 3, "Should have 3 listeners (HTTP, HTTPS, mTLS)");
        assert_eq!(conf.upstreams.len(), 4, "Should have 4 upstreams (Legacy, API, Inventory, Logger)");
        assert_eq!(conf.routes.len(), 5, "Should have 5 routes defined");

        // ========================================================================
        // 1. SERVER CONFIGURATION
        // ========================================================================
        let server = &conf.server;

        // Listeners
        assert_eq!(server.listeners[0].address, "0.0.0.0:8080");
        assert!(server.listeners[0].tls.is_none());

        assert_eq!(server.listeners[1].address, "0.0.0.0:8443");
        let tls_server = server.listeners[1].tls.as_ref().unwrap();
        assert_eq!(tls_server.cert_path, "/etc/gateway/certs/server.crt");
        assert!(tls_server.mtls_ca_cert.is_none());

        assert_eq!(server.listeners[2].address, "0.0.0.0:9443");
        let tls_mtls = server.listeners[2].tls.as_ref().unwrap();
        assert_eq!(tls_mtls.mtls_ca_cert.as_deref(), Some("/etc/gateway/certs/ca.pem"));

        // Process Settings
        assert_eq!(server.daemon, true);
        assert_eq!(server.pid_file.as_deref(), Some("/var/run/pingora.pid"));
        assert_eq!(server.user.as_deref(), Some("www-data"));
        assert_eq!(server.worker_threads, Some(8));
        assert_eq!(server.watch_config, true);

        // Protocols & Safety
        assert_eq!(server.enable_h2, true);
        assert_eq!(server.enable_h2c, false);
        assert_eq!(server.client_max_body_size, Some(10485760));
        assert_eq!(server.graceful_shutdown_timeout, Some(Duration::from_secs(30)));

        // ========================================================================
        // 2. OBSERVABILITY
        // ========================================================================
        let obs = conf.observability.as_ref().unwrap();
        assert_eq!(obs.prometheus_addr.as_deref(), Some("0.0.0.0:9090"));
        assert_eq!(obs.otlp_endpoint.as_deref(), Some("http://jaeger:14268"));

        // ========================================================================
        // 3. UPSTREAMS (A & B)
        // ========================================================================

        // --- Upstream A: Static / Legacy ---
        let up_legacy = conf.upstreams.iter().find(|u| u.id == "legacy-cluster").unwrap();

        // Flattened Source Check (Static)
        match &up_legacy.source {
            UpstreamSource::Static { backends } => {
                assert_eq!(backends.len(), 2);
                assert_eq!(backends[0].address, "10.0.0.1:80");
                assert_eq!(backends[0].weight, 10);
                assert_eq!(backends[1].address, "10.0.0.2:80");
                assert_eq!(backends[1].weight, 1);
            },
            _ => panic!("Expected Static source for legacy-cluster"),
        }

        // Selection & Health
        assert!(matches!(up_legacy.selection, LoadBalancerSelection::RoundRobin));

        match up_legacy.health_check.as_ref().unwrap() {
            HealthCheckConf::Tcp(common) => {
                assert_eq!(common.interval, Duration::from_secs(5));
                assert_eq!(common.consecutive_failure, 3);
            },
            _ => panic!("Expected TCP health check"),
        }

        // Backup Backends
        assert_eq!(up_legacy.backup_backends[0].address, "10.0.0.99:80");

        // Options
        assert_eq!(up_legacy.options.connect_timeout, Duration::from_millis(500));
        assert_eq!(up_legacy.options.connection_pool_size, 128);
        assert_eq!(up_legacy.options.tls, false);


        // --- Upstream B: DNS / API ---
        let up_api = conf.upstreams.iter().find(|u| u.id == "api-service").unwrap();

        // Flattened Source Check (DNS)
        match &up_api.source {
            UpstreamSource::Dns { hostname, refresh_interval } => {
                assert_eq!(hostname, "api.internal.svc");
                assert_eq!(refresh_interval, &Duration::from_secs(30));
            },
            _ => panic!("Expected DNS source for api-service"),
        }

        // Hash Source (The Struct Variant Fix)
        assert!(matches!(up_api.selection, LoadBalancerSelection::Consistent));
        match &up_api.hash_source {
            HashSource::Cookie { name } => assert_eq!(name, "session_id"),
            _ => panic!("Expected Cookie hash source"),
        }

        // HTTP Health Check
        match up_api.health_check.as_ref().unwrap() {
            HealthCheckConf::Http { common, path, expected_status } => {
                assert_eq!(path, "/health");
                assert_eq!(*expected_status, 200);
                assert_eq!(common.interval, Duration::from_secs(10));
            },
            _ => panic!("Expected HTTP health check"),
        }

        // Advanced Options
        assert_eq!(up_api.options.tls, true);
        assert_eq!(up_api.options.sni.as_deref(), Some("api.internal.svc"));
        assert_eq!(up_api.options.enable_h2, true);

        // ========================================================================
        // 3. UPSTREAMS (C & D)
        // ========================================================================

        // --- Upstream C: File / Inventory ---
        let up_inv = conf.upstreams.iter().find(|u| u.id == "inventory-service").unwrap();

        // Flattened Source Check (File)
        match &up_inv.source {
            UpstreamSource::File { path, format, refresh_interval } => {
                assert_eq!(path, "/etc/gateway/upstreams/inventory.json");
                assert!(matches!(format, FileFormat::Json));
                assert_eq!(refresh_interval, &Duration::from_secs(15));
            },
            _ => panic!("Expected File source for inventory-service"),
        }

        // Hash Source (Unit Variant)
        // Verify that 'type: client_ip' parsed correctly into the enum
        assert!(matches!(up_inv.hash_source, HashSource::ClientIp));

        // Custom Health Check (Shell Command)
        match up_inv.health_check.as_ref().unwrap() {
            HealthCheckConf::Custom { common, command } => {
                assert_eq!(command, "/usr/local/bin/check_inventory.sh");
                assert_eq!(common.timeout, Duration::from_secs(5));
            },
            _ => panic!("Expected Custom health check"),
        }

        // --- Upstream D: Unix Domain Socket ---
        let up_uds = conf.upstreams.iter().find(|u| u.id == "local-logger").unwrap();

        // Flattened Source Check (UDS)
        match &up_uds.source {
            UpstreamSource::Uds { path } => {
                assert_eq!(path, "/tmp/logger.sock");
            },
            _ => panic!("Expected Uds source for local-logger"),
        }

        // ========================================================================
        // 4. ROUTES (1 & 2)
        // ========================================================================

        // --- Route 1: Secure API (Regex + Security) ---
        let r1 = conf.routes.iter().find(|r| r.upstream_id == "api-service").unwrap();

        // Path Matching
        assert_eq!(r1.path, "^/api/v[0-9]+/secure");
        assert!(matches!(r1.path_type, PathType::Regex));

        // ACL (Firewall)
        let acl = r1.access_control.as_ref().unwrap();
        assert!(acl.allow.contains(&"10.0.0.0/8".to_string()));
        assert!(acl.deny.contains(&"0.0.0.0/0".to_string()));

        // WAF (Body Deny Patterns)
        let waf = r1.body_deny_patterns.as_ref().unwrap();
        assert!(waf.contains(&"(?i)union select".to_string()));

        // Inflight Limit
        assert_eq!(r1.inflight_limit, Some(100));

        // Auth Request (Nginx style)
        match r1.auth.as_ref().unwrap() {
            AuthConf::Request { auth_uri, headers_to_copy } => {
                assert_eq!(auth_uri, "http://auth-service.internal/verify");
                assert_eq!(headers_to_copy[0], "X-User-ID");
            },
            _ => panic!("Expected Request auth type"),
        }


        // --- Route 2: Public Web (Prefix + Optimization) ---
        let r2 = conf.routes.iter().find(|r| r.path == "/").unwrap();

        assert!(matches!(r2.path_type, PathType::Prefix));
        assert_eq!(r2.upstream_id, "legacy-cluster");

        // SNI / Hostname Matching
        let hosts = r2.hostnames.as_ref().unwrap();
        assert!(hosts.contains(&"www.example.com".to_string()));

        // Headers
        assert_eq!(r2.headers.add_req_headers.get("X-Gateway").unwrap(), "Pingora-Rust");
        assert!(r2.headers.remove_resp_headers.contains(&"Server".to_string()));
        assert_eq!(r2.headers.preserve_host_header, true);

        // Compression
        assert_eq!(r2.compression, true);

        // Rate Limiting
        let rl = r2.rate_limit.as_ref().unwrap();
        assert_eq!(rl.requests_per_sec, 50);
        assert_eq!(rl.burst, 10);
        assert!(matches!(rl.key, RateLimitKey::Ip));

        // Custom Error Pages
        let errs = r2.error_pages.as_ref().unwrap();
        assert_eq!(errs.get(&404).unwrap(), "/var/www/errors/404.html");

        // ========================================================================
        // 4. ROUTES (3, 4, & 5)
        // ========================================================================

        // --- Route 3: Advanced Caching ---
        let r3 = conf.routes.iter().find(|r| r.path == "/static").unwrap();

        let cache = r3.cache.as_ref().unwrap();
        assert_eq!(cache.enabled, true);
        assert_eq!(cache.default_ttl, Duration::from_secs(3600)); // 1h

        // Shim Verification: "500ms" string -> Option<Duration>
        assert_eq!(cache.lock_timeout, Some(Duration::from_millis(500)));
        assert_eq!(cache.stale_while_revalidate, Some(Duration::from_secs(60)));
        assert_eq!(cache.enable_purge, true);


        // --- Route 4: Real-time & Tunneling ---
        let r4 = conf.routes.iter().find(|r| r.path == "/ws/chat").unwrap();

        assert!(matches!(r4.path_type, PathType::Exact));
        assert_eq!(r4.websocket, true);
        assert_eq!(r4.proxy_connect, false);

        // Query Param Routing
        let query = r4.query_matches.as_ref().unwrap();
        assert_eq!(query.get("version").unwrap(), "2");

        // Query Stripping
        let strip = r4.strip_query_params.as_ref().unwrap();
        assert!(strip.contains(&"api_key".to_string()));


        // --- Route 5: Basic Auth Admin ---
        let r5 = conf.routes.iter().find(|r| r.path == "/admin").unwrap();

        match r5.auth.as_ref().unwrap() {
            AuthConf::Basic { realm, users } => {
                assert_eq!(realm, "Admin Area");
                assert_eq!(users.get("admin").unwrap(), "secret_password_123");
            },
            _ => panic!("Expected Basic auth type"),
        }
    }
}
