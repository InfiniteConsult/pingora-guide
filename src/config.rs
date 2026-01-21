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
pub enum HashSource {
    #[default]
    None,
    ClientIp,
    Uri,
    Header(String),
    Cookie(String),
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
}

fn default_timeout() -> Option<Duration> { Some(Duration::from_secs(60)) }

impl ClusterOptions {
    pub fn new(
        connect_timeout: Duration,
        read_timeout: Duration,
        write_timeout: Duration,
        idle_timeout: Option<Duration>,
        enable_h2: bool,
        verify_hostname: bool,
    ) -> Self {
        Self {
            connect_timeout,
            read_timeout,
            write_timeout,
            idle_timeout,
            enable_h2,
            verify_hostname
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
enum PathType {
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


fn default_status_200() -> u16 { 200 }

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
            let yaml = r#"client_ip"#;
            match serde_yaml::from_str::<HashSource>(yaml) {
                Ok(source) => { assert_eq!(source, HashSource::ClientIp) },
                Err(_) => { panic!("Error should not be returned") }
            }
        }

        #[test]
        fn deserialization_parses_complex_variant() {
            let yaml = r#"!header x-user-id"#;
            match serde_yaml::from_str::<HashSource>(yaml) {
                Ok(source) => {
                    assert_eq!(source, HashSource::Header("x-user-id".to_string()));

                },
                Err(e) => {
                    println!("{:?}", e);
                    panic!("Error should not be returned")
                }
            }
        }

        #[test]
        fn equality_check_works() {
            let a1 = HashSource::Header("a".to_string());
            let a2 = HashSource::Header("a".to_string());
            let b1 = HashSource::Header("b".to_string());

            assert_eq!(a1, a2);
            assert_ne!(b1, a1);
        }
    }
}