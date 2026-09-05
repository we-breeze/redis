use brz_net::QuotaBalancerOptions;
use std::time::Duration;

/// Transport settings used while building every shard.
#[derive(Clone)]
pub struct RedisServiceOptions {
    pub(super) password: Option<String>,
    pub(super) master_timeout: Duration,
    pub(super) slave_timeout: Duration,
    pub(super) connect_timeout: Duration,
    pub(super) dns_refresh_interval: Duration,
    pub(super) replica_balance: QuotaBalancerOptions,
}

impl Default for RedisServiceOptions {
    fn default() -> Self {
        Self {
            password: None,
            master_timeout: Duration::from_millis(200),
            slave_timeout: Duration::from_millis(200),
            connect_timeout: Duration::from_secs(2),
            dns_refresh_interval: Duration::from_secs(30),
            replica_balance: QuotaBalancerOptions::default(),
        }
    }
}

impl RedisServiceOptions {
    /// Sets password authentication for every master and replica connection.
    /// `None` disables AUTH; `Some` is sent unchanged, including an empty password.
    /// Authentication is repeated whenever a connection is established.
    #[must_use]
    pub fn with_password(mut self, password: Option<String>) -> Self {
        self.password = password;
        self
    }

    /// Set the request timeout for both master and slave sessions.
    #[must_use]
    pub fn with_timeout(mut self, value: Duration) -> Self {
        self.master_timeout = value;
        self.slave_timeout = value;
        self
    }

    #[must_use]
    pub fn with_master_timeout(mut self, value: Duration) -> Self {
        self.master_timeout = value;
        self
    }

    #[must_use]
    pub fn with_slave_timeout(mut self, value: Duration) -> Self {
        self.slave_timeout = value;
        self
    }

    #[must_use]
    pub fn with_connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = value;
        self
    }

    #[must_use]
    pub fn with_dns_refresh_interval(mut self, value: Duration) -> Self {
        self.dns_refresh_interval = value;
        self
    }

    #[must_use]
    pub fn with_replica_quota(mut self, value: Duration) -> Self {
        self.replica_balance.quota = value;
        self
    }

    #[must_use]
    pub fn with_failure_penalty(mut self, value: Duration) -> Self {
        self.replica_balance.failure_penalty = value;
        self
    }
}

impl std::fmt::Debug for RedisServiceOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisServiceOptions")
            .field("password", &self.password.as_ref().map(|_| "[REDACTED]"))
            .field("master_timeout", &self.master_timeout)
            .field("slave_timeout", &self.slave_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("dns_refresh_interval", &self.dns_refresh_interval)
            .field("replica_balance", &self.replica_balance)
            .finish()
    }
}
