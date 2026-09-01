//! Feature-gated ProfileUtil-compatible metrics for direct Redis attempts.

use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::Instant;

use brz_metrics::Metric;

/// One configured logical `host:port` metric shared by all of its resolved
/// physical replicas.
pub(crate) struct EndpointProfileMetric {
    name: Box<str>,
    metric: OnceLock<Metric>,
}

impl EndpointProfileMetric {
    pub(crate) fn new(name: String) -> Self {
        Self {
            name: name.into_boxed_str(),
            metric: OnceLock::new(),
        }
    }

    /// Creates one physical attempt. Registration remains lazy so an endpoint
    /// that is configured but never selected does not create a profile row.
    #[inline]
    pub(crate) fn attempt(&self) -> ProfileAttempt {
        self.attempt_started_at(Instant::now())
    }

    #[inline]
    pub(crate) fn attempt_started_at(&self, started: Instant) -> ProfileAttempt {
        ProfileAttempt {
            metric: *self.metric.get_or_init(|| Metric::redis(&self.name)),
            started,
            finished: false,
        }
    }

    pub(crate) fn record_batch_failure(&self, count: usize, started: Instant) {
        if count == 0 {
            return;
        }
        let metric = *self.metric.get_or_init(|| Metric::redis(&self.name));
        let elapsed = started.elapsed();
        for _ in 0..count {
            metric.record(elapsed, false);
        }
    }
}

/// RAII completion state for exactly one physical Redis attempt.
///
/// Dropping an admitted response before observing its completion is counted as
/// a failure, matching the replica quota guard's conservative cancellation
/// semantics.
pub(crate) struct ProfileAttempt {
    metric: Metric,
    started: Instant,
    finished: bool,
}

impl ProfileAttempt {
    #[inline]
    pub(crate) fn wrap<F>(self, response: F) -> ProfiledResponseFuture<F> {
        ProfiledResponseFuture {
            response,
            attempt: self,
        }
    }

    #[inline]
    fn finish(&mut self, success: bool) {
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

pub(crate) struct ProfiledResponseFuture<F> {
    response: F,
    attempt: ProfileAttempt,
}

trait CompletionResult {
    fn succeeded(&self) -> bool;
}

impl<T, E> CompletionResult for Result<T, E> {
    #[inline]
    fn succeeded(&self) -> bool {
        self.is_ok()
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
                self.attempt.finish(result.succeeded());
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_endpoint_attempts_share_one_registered_metric() {
        let name = format!("profile-metric-test-{}", std::process::id());
        let endpoint = EndpointProfileMetric::new(name.clone());

        endpoint.attempt().finish(true);
        endpoint.attempt().finish(false);

        let mut snapshot = None;
        brz_metrics::visit(|candidate_name, metric_type, candidate| {
            if candidate_name == name && metric_type == "REDIS" {
                snapshot = Some(candidate);
            }
        });
        let snapshot = snapshot.expect("attempts must lazily register the endpoint metric");
        assert_eq!(
            (snapshot.total, snapshot.success, snapshot.failure),
            (2, 1, 1)
        );
    }
}
