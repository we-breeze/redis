//! Dedicated subscription sessions reuse the service's logical master routing.
use super::*;
use crate::RedisSubscription;

impl RedisService {
    /// Subscribes to one binary-safe channel using a dedicated master session.
    ///
    /// Returns after the matching `SUBSCRIBE` confirmation. Messages that arrive
    /// with that confirmation remain queued for `RedisSubscription::next_message`.
    /// Ordinary commands continue using the existing service connections.
    ///
    /// The channel selects the same shard as `Redis::publish`. Authentication,
    /// database and transport deadlines come from this service's configuration.
    /// Receiving messages has no command deadline. Disconnection terminates the
    /// subscription rather than transparently reconnecting across a message gap.
    pub async fn subscribe<C>(&self, channel: C) -> RedisResult<RedisSubscription>
    where
        C: EncodeRedisArg + Send,
    {
        let encoded = crate::arg::encode_arg_contiguous(&channel)?;
        let channel = bytes::Bytes::copy_from_slice(encoded.as_ref());
        let topology = self.inner.topology.load_full();
        let index = topology
            .dns_sources
            .router
            .route(channel.as_ref(), topology.resolved.shards.len());
        let master = &topology.resolved.shards[index].master;
        let endpoint = master
            .addresses
            .iter()
            .find(|address| {
                topology.nodes.iter().any(|(key, node)| {
                    key.endpoint == **address
                        && key.db == master.config.db
                        && key.auth == master.config.auth
                        && node.is_connected()
                })
            })
            .or_else(|| master.addresses.first())
            .copied()
            .ok_or_else(|| {
                RedisError::new(
                    ErrorKind::NoConnection,
                    "Redis subscription master has no address",
                )
            })?;
        let options = NodeOptions {
            request_timeout: self.inner.options.master_timeout,
            connect_timeout: self.inner.options.connect_timeout,
            ..NodeOptions::default()
        };
        #[cfg(feature = "metrics")]
        let labels = {
            let endpoint: Arc<str> = Arc::from(master.config.profile_name());
            brz_metrics::ResourceLabels::new(
                self.inner
                    .options
                    .resource_name
                    .clone()
                    .unwrap_or_else(|| Arc::clone(&endpoint)),
                endpoint,
            )
        };
        #[cfg(feature = "metrics")]
        let observer = Some(Arc::new(crate::profile_metrics::ConnectionObserver::new(
            &labels.clone().with_role(brz_metrics::ConnectionRole::Write),
        )) as Arc<dyn brz_net::NodeConnectionObserver>);
        #[cfg(not(feature = "metrics"))]
        let observer = None;
        RedisSubscription::connect(
            endpoint,
            master.config.auth.clone(),
            master.config.db,
            channel,
            options,
            observer,
            #[cfg(feature = "metrics")]
            EndpointProfileMetric::new(&labels),
        )
        .await
    }
}
