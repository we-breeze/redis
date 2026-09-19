use std::borrow::Cow;
use std::time::{Duration, Instant};

const SLOW_REQUEST_THRESHOLD: Duration = Duration::from_millis(200);

pub(crate) struct Observation {
    started: Instant,
    readonly: bool,
    detail: Cow<'static, str>,
    finished: bool,
}

impl Observation {
    pub(crate) fn new(readonly: bool, detail: impl Into<Cow<'static, str>>) -> Self {
        Self {
            started: Instant::now(),
            readonly,
            detail: detail.into(),
            finished: false,
        }
    }

    pub(crate) fn finish(mut self, success: bool) {
        self.log(success);
        self.finished = true;
    }

    fn log(&self, success: bool) {
        let elapsed = self.started.elapsed();
        if elapsed >= SLOW_REQUEST_THRESHOLD {
            tracing::warn!(
                target: "breeze.slow",
                "redis {} {}ms {} {}",
                if self.readonly { "read" } else { "write" },
                elapsed.as_millis(),
                success,
                self.detail,
            );
        }
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        if !self.finished {
            self.log(false);
        }
    }
}
