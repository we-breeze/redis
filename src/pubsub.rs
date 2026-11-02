//! A finite-lifetime, dedicated Redis Pub/Sub session without reconnects.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use brz_net::{Node, NodeConnectionObserver, NodeOptions, SessionError};
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::net_transport::map_session_error;
use crate::{ErrorKind, RedisError, RedisResult};
use protocol::{Control, PubSubProtocol};

mod protocol;

#[cfg(test)]
mod tests;

const MESSAGE_CAPACITY: usize = 256;

/// One binary-safe publication received on the subscribed channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisMessage {
    /// The exact subscribed channel, without UTF-8 conversion.
    pub channel: Bytes,
    /// The complete publication payload, without UTF-8 conversion.
    pub payload: Bytes,
}

/// One ordinary channel subscription with its own physical connection.
///
/// Messages are buffered in a bounded 256-message queue. Queue overflow closes
/// the connection and is reported after its already queued prefix. EOF and
/// protocol failures are errors, never transparent reconnects. Cancelling a
/// pending [`Self::next_message`] preserves all partially received frames.
pub struct RedisSubscription {
    node: Node<PubSubProtocol>,
    messages: mpsc::Receiver<RedisMessage>,
    inbox: Arc<Inbox>,
    closed: bool,
    #[cfg(feature = "metrics")]
    profile_metric: crate::profile_metrics::EndpointProfileMetric,
}

impl std::fmt::Debug for RedisSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisSubscription")
            .field("node", &self.node)
            .field("buffered_messages", &self.messages.len())
            .field("closed", &self.closed)
            .finish()
    }
}

#[derive(Default)]
struct Inbox {
    explicitly_closed: AtomicBool,
    terminal: Mutex<Option<RedisError>>,
}

impl Inbox {
    fn take_error(&self) -> Option<RedisError> {
        self.terminal.lock().expect("Pub/Sub terminal state").take()
    }

    fn disconnected(&self, error: &SessionError<RedisError>) {
        if self.explicitly_closed.load(Ordering::Acquire) {
            return;
        }
        let error = match error {
            SessionError::Protocol(error) => RedisError::new(error.kind(), error.to_string()),
            _ => map_session_error(error.clone()),
        };
        let mut terminal = self.terminal.lock().expect("Pub/Sub terminal state");
        if terminal.is_none() {
            *terminal = Some(error);
        }
    }
}

impl RedisSubscription {
    pub(crate) async fn connect(
        endpoint: SocketAddr,
        auth: Option<String>,
        db: i64,
        channel: Bytes,
        options: NodeOptions,
        observer: Option<Arc<dyn NodeConnectionObserver>>,
        #[cfg(feature = "metrics")] profile_metric: crate::profile_metrics::EndpointProfileMetric,
    ) -> RedisResult<Self> {
        let (sender, messages) = mpsc::channel(MESSAGE_CAPACITY);
        let inbox = Arc::new(Inbox::default());
        let protocol = PubSubProtocol::new(auth, db, channel, sender, inbox.clone());
        let node = match observer {
            Some(observer) => Node::new_observed(endpoint, protocol, options, observer),
            None => Node::new(endpoint, protocol, options),
        }
        .map_err(|error| RedisError::new(ErrorKind::ClientError, error.to_string()))?;
        // Own the close guard before the first suspension, including handshake
        // and SUBSCRIBE ACK waits. Cancelling creation closes this exact socket.
        let subscription = Self {
            node,
            messages,
            inbox,
            closed: false,
            #[cfg(feature = "metrics")]
            profile_metric,
        };
        let budget = options
            .connect_timeout
            .checked_add(options.request_timeout)
            .unwrap_or(options.connect_timeout);
        let ready = tokio::time::timeout(budget, subscription.wait_connected()).await;
        match ready {
            Ok(ready) => ready?,
            Err(_) => {
                return Err(RedisError::new(
                    ErrorKind::Timeout,
                    "Pub/Sub setup timed out",
                ));
            }
        }
        subscription.control(Control::Subscribe).await?;
        Ok(subscription)
    }

    async fn wait_connected(&self) -> RedisResult<()> {
        loop {
            if self.node.is_connected() {
                return Ok(());
            }
            tokio::select! {
                _ = self.node.closed() => {
                    return Err(self.inbox.take_error().unwrap_or_else(|| {
                        RedisError::new(ErrorKind::Io, "Pub/Sub session closed during setup")
                    }));
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
            }
        }
    }

    async fn control(&self, command: Control) -> RedisResult<()> {
        #[cfg(feature = "metrics")]
        let mut attempt = None;
        let response = self
            .node
            .request_with(|| {
                #[cfg(feature = "metrics")]
                {
                    attempt = Some(self.profile_metric.attempt(false));
                }
                command
            })
            .map_err(map_session_error)?;
        let result = response.await;
        #[cfg(feature = "metrics")]
        if let Some(attempt) = &mut attempt {
            attempt.finish(result.is_ok());
        }
        result.map_err(map_session_error)
    }

    /// Receive the next publication, or `None` after explicit close or a
    /// previously reported terminal error. An idle wait has no request timeout.
    pub async fn next_message(&mut self) -> RedisResult<Option<RedisMessage>> {
        if self.closed || self.inbox.explicitly_closed.load(Ordering::Acquire) {
            return Ok(None);
        }
        if let Some(message) = self.messages.recv().await {
            return Ok(Some(message));
        }
        self.closed = true;
        self.node.close();
        Err(self.inbox.take_error().unwrap_or_else(|| {
            RedisError::new(ErrorKind::Io, "Pub/Sub session ended unexpectedly")
        }))
    }

    /// Unsubscribe the exact channel, validate its zero-count ACK, then close
    /// the dedicated session. `close` and `Drop` do not send this command.
    pub async fn unsubscribe(&mut self) -> RedisResult<()> {
        if self.closed || self.inbox.explicitly_closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let result = self.control(Control::Unsubscribe).await;
        self.close().await?;
        result
    }

    /// Close the physical session without sending UNSUBSCRIBE. Repeated closes
    /// are harmless, and return only after the transport has released the socket.
    pub async fn close(&mut self) -> RedisResult<()> {
        self.inbox.explicitly_closed.store(true, Ordering::Release);
        self.closed = true;
        self.messages.close();
        while self.messages.try_recv().is_ok() {}
        self.node.close();
        self.node.closed().await;
        self.inbox.take_error();
        Ok(())
    }
}

impl Drop for RedisSubscription {
    fn drop(&mut self) {
        self.inbox.explicitly_closed.store(true, Ordering::Release);
        self.node.close();
    }
}
