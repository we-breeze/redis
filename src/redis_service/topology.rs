use super::*;

pub(super) async fn build_dns_sources(
    shards: Vec<(String, Vec<String>)>,
    routing: &ShardRouting,
    options: &RedisServiceOptions,
) -> RedisResult<Arc<RedisDnsSources>> {
    validate_routing(routing, shards.len())?;
    let mut master_labels = HashSet::with_capacity(shards.len());
    let mut builds = Vec::with_capacity(shards.len());
    let mut names = Vec::with_capacity(shards.len());

    for (master_endpoint, slave_endpoints) in shards {
        let mut master = configured_server(&master_endpoint)?;
        master.auth = options.password.clone();
        let master_label = master.label();
        if !master_labels.insert(master_label.clone()) {
            return Err(client_error(format!(
                "duplicate shard master backend {master_label}"
            )));
        }
        let slaves = slave_endpoints
            .iter()
            .map(|endpoint| {
                let mut config = configured_server(endpoint)?;
                config.auth = options.password.clone();
                Ok(config)
            })
            .collect::<RedisResult<Vec<_>>>()?;
        validate_slave_configs(&slaves)?;
        names.push(master_label);
        builds.push(build_shard_sources(master, slaves, options));
    }

    let built = try_join_all(builds).await?;
    let mut registrations = Vec::new();
    let mut shards = Vec::with_capacity(built.len());
    for (shard, mut shard_registrations) in built {
        shards.push(shard);
        registrations.append(&mut shard_registrations);
    }
    let router = RedisRouter {
        sharding: Sharding::new(routing.hash_algorithm(), routing.distribution(), &names),
    };
    Ok(Arc::new(RedisDnsSources {
        shards,
        router,
        _dns_registrations: registrations,
    }))
}

pub(super) async fn build_shard_sources(
    master: RedisEndpointConfig,
    slaves: Vec<RedisEndpointConfig>,
    options: &RedisServiceOptions,
) -> RedisResult<(RedisShardSources, Vec<DnsSource>)> {
    let (master, slaves) = tokio::try_join!(
        build_endpoint_source(master, options.dns_refresh_interval),
        try_join_all(
            slaves
                .into_iter()
                .map(|config| build_endpoint_source(config, options.dns_refresh_interval))
        ),
    )?;

    let (master, master_registration) = master;
    let mut registrations = Vec::with_capacity(slaves.len() + 1);
    registrations.push(master_registration);
    let mut slave_sources = Vec::with_capacity(slaves.len());
    for (source, registration) in slaves {
        slave_sources.push(source);
        registrations.push(registration);
    }
    Ok((
        RedisShardSources {
            master,
            slaves: slave_sources,
        },
        registrations,
    ))
}

pub(super) async fn build_endpoint_source(
    config: RedisEndpointConfig,
    refresh_interval: Duration,
) -> RedisResult<(RedisEndpointSource, DnsSource)> {
    let authority = format!("{}:{}", config.host, config.port);
    let registration = DnsSource::new([authority], DnsOptions { refresh_interval })
        .await
        .map_err(map_net_error)?;
    let endpoints = registration.endpoint_set().clone();
    Ok((RedisEndpointSource { config, endpoints }, registration))
}

pub(super) async fn build_topology(
    dns_sources: Arc<RedisDnsSources>,
    options: &RedisServiceOptions,
    previous: Option<&RedisTopology>,
) -> RedisResult<RedisTopology> {
    let resolved = dns_sources.resolve();
    let built = try_join_all(
        resolved
            .shards
            .iter()
            .map(|shard| build_resolved_shard(shard, options, previous)),
    )
    .await?;
    let mut nodes = HashMap::new();
    let mut shards = Vec::with_capacity(built.len());
    for (shard, shard_nodes) in built {
        shards.push(shard);
        nodes.extend(shard_nodes);
    }

    Ok(RedisTopology {
        shards: Sharded::new(dns_sources.router.clone(), shards).map_err(map_net_error)?,
        dns_sources,
        resolved,
        nodes,
    })
}

pub(super) async fn build_resolved_shard(
    shard: &ResolvedShard,
    options: &RedisServiceOptions,
    previous: Option<&RedisTopology>,
) -> RedisResult<(RedisShard, HashMap<NodeKey, Node<RedisProtocol>>)> {
    let (master, slaves) = tokio::try_join!(
        build_replicas(
            std::slice::from_ref(&shard.master),
            options.master_timeout,
            options,
            previous,
        ),
        build_replicas(&shard.slaves, options.slave_timeout, options, previous),
    )?;
    let mut nodes = master.1;
    nodes.extend(slaves.1);
    Ok((
        RedisShard {
            master: master.0,
            slaves: slaves.0,
        },
        nodes,
    ))
}

pub(super) async fn build_replicas(
    configs: &[ResolvedEndpoint],
    request_timeout: Duration,
    options: &RedisServiceOptions,
    previous: Option<&RedisTopology>,
) -> RedisResult<(
    ReplicaSet<RedisReplica>,
    HashMap<NodeKey, Node<RedisProtocol>>,
)> {
    let mut endpoints = HashSet::new();
    let mut node_cache = HashMap::new();
    let mut nodes = Vec::new();
    for resolved in configs {
        #[cfg(feature = "metrics")]
        let profile_metric = Arc::new(EndpointProfileMetric::new(resolved.config.profile_name()));
        for &address in resolved.addresses.iter() {
            let key = NodeKey {
                endpoint: address,
                db: resolved.config.db,
                auth: resolved.config.auth.clone(),
                request_timeout,
            };
            if !endpoints.insert(key.clone()) {
                continue;
            }
            let node = if let Some(node) = previous.and_then(|topology| topology.nodes.get(&key)) {
                node.clone()
            } else {
                Node::new(
                    address,
                    RedisProtocol::new(resolved.config.auth.clone(), resolved.config.db),
                    NodeOptions {
                        request_timeout,
                        connect_timeout: options.connect_timeout,
                        ..NodeOptions::default()
                    },
                )
                .map_err(map_net_error)?
            };
            node_cache.insert(key, node.clone());
            #[cfg(feature = "metrics")]
            nodes.push(RedisReplica::new(node, Arc::clone(&profile_metric)));
            #[cfg(not(feature = "metrics"))]
            nodes.push(RedisReplica::new(node));
        }
    }

    wait_until_connected(&nodes, options.connect_timeout).await?;
    let replicas =
        ReplicaSet::with_options(nodes, options.replica_balance).map_err(map_net_error)?;
    Ok((replicas, node_cache))
}

pub(super) async fn wait_until_connected(
    nodes: &[RedisReplica],
    connect_timeout: Duration,
) -> RedisResult<()> {
    let deadline = Instant::now()
        + connect_timeout
            .saturating_mul(2)
            .saturating_add(Duration::from_millis(100));
    while nodes.iter().any(|node| !node.is_connected()) {
        if Instant::now() >= deadline {
            let endpoints = nodes
                .iter()
                .filter(|node| !node.is_connected())
                .map(|node| node.endpoint().to_string())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(RedisError::new(
                ErrorKind::NoConnection,
                format!("Redis sessions did not become ready: {endpoints}"),
            ));
        }
        sleep(Duration::from_millis(1)).await;
    }
    Ok(())
}

pub(super) fn configured_server(endpoint: &str) -> RedisResult<RedisEndpointConfig> {
    let endpoint = endpoint.trim();
    let parts = endpoint.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&parts.len()) || parts[0].is_empty() {
        return Err(client_error(format!(
            "invalid Redis endpoint {endpoint:?}, expected host:port[:db]"
        )));
    }
    let port = parts[1]
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| client_error(format!("invalid port in Redis endpoint {endpoint:?}")))?;
    let db = parts.get(2).map_or(Ok(0), |db| {
        db.parse::<i64>()
            .map_err(|_| client_error(format!("invalid db in Redis endpoint {endpoint:?}")))
    })?;
    Ok(RedisEndpointConfig {
        host: parts[0].to_owned(),
        port,
        db,
        auth: None,
    })
}

pub(super) fn validate_slave_configs(configs: &[RedisEndpointConfig]) -> RedisResult<()> {
    if configs.is_empty() {
        return Err(client_error("at least one slave backend is required"));
    }
    let mut labels = HashSet::with_capacity(configs.len());
    for config in configs {
        let label = config.label();
        if !labels.insert(label.clone()) {
            return Err(client_error(format!("duplicate slave backend {label}")));
        }
    }
    Ok(())
}

pub(super) fn validate_options(options: &RedisServiceOptions) -> RedisResult<()> {
    if options.master_timeout.is_zero()
        || options.slave_timeout.is_zero()
        || options.connect_timeout.is_zero()
        || options.dns_refresh_interval.is_zero()
    {
        return Err(client_error(
            "Redis transport and DNS intervals must be non-zero",
        ));
    }
    if options.replica_balance.quota.is_zero() || options.replica_balance.failure_penalty.is_zero()
    {
        return Err(client_error(
            "Redis replica quota settings must be non-zero",
        ));
    }
    Ok(())
}

pub(super) fn map_net_error(error: NetError) -> RedisError {
    RedisError::new(ErrorKind::ClientError, error.to_string())
}

pub(super) fn validate_routing(routing: &ShardRouting, shard_count: usize) -> RedisResult<()> {
    if shard_count == 0 {
        return Err(client_error("at least one Redis shard is required"));
    }
    match routing.hash_algorithm().to_ascii_lowercase().as_str() {
        "crc32"
        | "crc32-short"
        | "crc32-smartnum"
        | "crc32-mixnum"
        | "crc32-num"
        | "crc32local"
        | "crc32local-smartnum"
        | "raw"
        | "bkdr"
        | "crc64"
        | "fnv1_32"
        | "fnv1a_64" => {}
        algorithm => {
            return Err(client_error(format!(
                "unsupported Redis hash algorithm {algorithm:?}"
            )));
        }
    }

    let distribution = routing.distribution().to_ascii_lowercase();
    if let Some(slot) = distribution.strip_prefix("range-") {
        let slot = slot
            .parse::<u64>()
            .map_err(|_| client_error(format!("invalid range distribution {distribution:?}")))?;
        if slot < shard_count as u64 {
            return Err(client_error(format!(
                "range slot count {slot} is smaller than shard count {shard_count}"
            )));
        }
        if !shard_count.is_power_of_two() {
            return Err(client_error(
                "range distribution requires a power-of-two shard count",
            ));
        }
    } else if !matches!(
        distribution.as_str(),
        "range" | "modula" | "absmodula" | "ketama" | "ketama_origin" | "secmod"
    ) {
        return Err(client_error(format!(
            "unsupported Redis distribution {distribution:?}"
        )));
    }
    Ok(())
}

pub(super) fn client_error(message: impl Into<String>) -> RedisError {
    RedisError::new(ErrorKind::ClientError, message)
}
