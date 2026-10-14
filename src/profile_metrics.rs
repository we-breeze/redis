//! Feature-gated cumulative metrics for direct Redis attempts.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use brz_metrics::{MetricAccess, MetricType, ResourceLabels, ResourceMetric, TimingMetric};

/// One configured logical `host:port` metric shared by all of its resolved
/// physical replicas.
#[derive(Clone, Copy)]
pub(crate) struct EndpointProfileMetric {
    read: TimingMetric,
    write: TimingMetric,
}

impl EndpointProfileMetric {
    pub(crate) fn new(labels: &ResourceLabels) -> Self {
        let kind = MetricType::Redis(ResourceMetric::Req);
        Self {
            read: TimingMetric::new_resource(kind, &labels.clone().with_access(MetricAccess::Read)),
            write: TimingMetric::new_resource(
                kind,
                &labels.clone().with_access(MetricAccess::Write),
            ),
        }
    }

    #[inline]
    pub(crate) fn attempt(&self, readonly: bool) -> ProfileAttempt {
        ProfileAttempt {
            metric: if readonly { self.read } else { self.write },
            started: Instant::now(),
            finished: false,
        }
    }
}

/// RAII completion state for exactly one physical Redis attempt.
///
/// Dropping an admitted response before observing its completion is counted as
/// a failure, matching the replica quota guard's conservative cancellation
/// semantics.
pub(crate) struct ProfileAttempt {
    metric: TimingMetric,
    started: Instant,
    finished: bool,
}

impl ProfileAttempt {
    #[inline]
    pub(crate) fn wrap<F>(self, response: F) -> ProfiledResponseFuture<F> {
        ProfiledResponseFuture {
            response,
            attempt: Some(self),
        }
    }

    #[inline]
    pub(crate) fn finish(&mut self, success: bool) {
        self.metric.record(self.started.elapsed(), success);
        self.finished = true;
    }
}

impl Drop for ProfileAttempt {
    fn drop(&mut self) {
        if !self.finished {
            self.metric.record(self.started.elapsed(), false);
        }
    }
}

impl<F> ProfiledResponseFuture<F> {
    pub(crate) fn unobserved(response: F) -> Self {
        Self {
            response,
            attempt: None,
        }
    }
}

pub(crate) struct ProfiledResponseFuture<F> {
    response: F,
    attempt: Option<ProfileAttempt>,
}

trait CompletionResult {
    fn succeeded(&self) -> bool;
}

impl CompletionResult
    for Result<crate::net_transport::RedisResponse, brz_net::SessionError<crate::RedisError>>
{
    #[inline]
    fn succeeded(&self) -> bool {
        self.as_ref().is_ok_and(|response| {
            !matches!(
                response,
                crate::net_transport::RedisResponse::ServerError(_)
            )
        })
    }
}

impl<F> Future for ProfiledResponseFuture<F>
where
    F: Future + Unpin,
    F::Output: CompletionResult,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.response).poll(context) {
            Poll::Ready(result) => {
                if let Some(attempt) = &mut self.attempt {
                    attempt.finish(result.succeeded());
                }
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "metrics")]
pub(crate) struct ConnectionObserver {
    metric: brz_metrics::ConnMetric,
    active: std::sync::Mutex<Option<brz_metrics::ConnectionGuard>>,
}

#[cfg(feature = "metrics")]
impl ConnectionObserver {
    pub(crate) fn new(labels: &ResourceLabels) -> Self {
        Self {
            metric: brz_metrics::ConnMetric::new_resource(
                brz_metrics::MetricType::Redis(brz_metrics::ResourceMetric::Conn),
                labels,
            ),
            active: std::sync::Mutex::new(None),
        }
    }
}

#[cfg(feature = "metrics")]
impl brz_net::NodeConnectionObserver for ConnectionObserver {
    fn on_connection_state_change(&self, connected: bool) {
        let mut active = self
            .active
            .lock()
            .expect("connection metrics mutex poisoned");
        if connected {
            if active.is_none() {
                *active = Some(self.metric.connection());
            }
        } else {
            active.take();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_endpoint_attempts_share_one_registered_metric() {
        let name = format!("profile-metric-test-{}", std::process::id());
        let endpoint =
            EndpointProfileMetric::new(&ResourceLabels::new(name.clone(), "localhost:6379"));

        endpoint.attempt(true).finish(true);
        endpoint.attempt(true).finish(false);

        let snapshot = endpoint.read.snapshot();
        assert_eq!(endpoint.write.snapshot().total, 0);
        assert_eq!(
            (snapshot.total, snapshot.success, snapshot.failure),
            (2, 1, 1)
        );
    }
}
