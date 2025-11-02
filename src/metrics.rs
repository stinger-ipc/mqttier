//! Metrics tracking for MQTTier client operations
//!
//! This module provides comprehensive metrics tracking for MQTT operations when the
//! `metrics` feature is enabled. All metrics use atomic operations for thread-safe updates.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    // Connection metrics
    pub connection_attempts: u64,
    pub successful_connections: u64,
    pub failed_connections: u64,
    pub reconnection_count: u64,
    pub is_connected: bool,
    pub connection_start_timestamp: Option<u64>,
    pub last_connection_duration_ms: Option<u64>,

    // Message metrics - Published
    pub messages_published_qos0: u64,
    pub messages_published_qos1: u64,
    pub messages_published_qos2: u64,
    pub publish_failures: u64,
    pub total_bytes_sent: u64,

    // Message metrics - Received
    pub messages_received_qos0: u64,
    pub messages_received_qos1: u64,
    pub messages_received_qos2: u64,
    pub total_bytes_received: u64,

    // Performance metrics
    pub avg_publish_latency_us: u64,
    pub max_publish_latency_us: u64,
    pub min_publish_latency_us: u64,

    // Reliability metrics
    pub puback_received: u64,
    pub pubcomp_received: u64,
    pub publish_timeouts: u64,
    pub last_error_timestamp: Option<u64>,

    // Subscription metrics
    pub active_subscriptions: u32,
    pub subscription_requests: u64,
    pub subscription_failures: u64,
}

impl MetricsSnapshot {
    /// Get total messages published across all QoS levels
    pub fn total_messages_published(&self) -> u64 {
        self.messages_published_qos0 + self.messages_published_qos1 + self.messages_published_qos2
    }

    /// Get total messages received across all QoS levels
    pub fn total_messages_received(&self) -> u64 {
        self.messages_received_qos0 + self.messages_received_qos1 + self.messages_received_qos2
    }

    /// Get current connection uptime in milliseconds, if connected
    pub fn connection_uptime_ms(&self) -> Option<u64> {
        if self.is_connected {
            self.connection_start_timestamp.map(|start| {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as u64;
                now.saturating_sub(start)
            })
        } else {
            None
        }
    }

    /// Get average message size for sent messages in bytes
    pub fn avg_sent_message_size(&self) -> Option<u64> {
        let total_msgs = self.total_messages_published();
        if total_msgs > 0 {
            Some(self.total_bytes_sent / total_msgs)
        } else {
            None
        }
    }

    /// Get average message size for received messages in bytes
    pub fn avg_received_message_size(&self) -> Option<u64> {
        let total_msgs = self.total_messages_received();
        if total_msgs > 0 {
            Some(self.total_bytes_received / total_msgs)
        } else {
            None
        }
    }

    /// Get publish success rate (0.0 to 1.0)
    pub fn publish_success_rate(&self) -> Option<f64> {
        let total_attempts = self.total_messages_published() + self.publish_failures;
        if total_attempts > 0 {
            Some(self.total_messages_published() as f64 / total_attempts as f64)
        } else {
            None
        }
    }
}

/// Metrics collector for MQTTier client operations
#[derive(Debug, Default)]
pub struct Metrics {
    // Connection metrics
    connection_attempts: AtomicU64,
    successful_connections: AtomicU64,
    failed_connections: AtomicU64,
    reconnection_count: AtomicU64,
    is_connected: AtomicBool,
    connection_start_timestamp: AtomicU64, // Unix timestamp in milliseconds
    last_connection_duration_ms: AtomicU64,

    // Message metrics - Published
    messages_published_qos0: AtomicU64,
    messages_published_qos1: AtomicU64,
    messages_published_qos2: AtomicU64,
    publish_failures: AtomicU64,
    total_bytes_sent: AtomicU64,

    // Message metrics - Received
    messages_received_qos0: AtomicU64,
    messages_received_qos1: AtomicU64,
    messages_received_qos2: AtomicU64,
    total_bytes_received: AtomicU64,

    // Performance metrics
    total_publish_latency_us: AtomicU64, // For calculating average
    publish_latency_samples: AtomicU64,  // Number of latency samples
    max_publish_latency_us: AtomicU64,
    min_publish_latency_us: AtomicU64,

    // Reliability metrics
    puback_received: AtomicU64,
    pubcomp_received: AtomicU64,
    publish_timeouts: AtomicU64,
    last_error_timestamp: AtomicU64,

    // Subscription metrics
    active_subscriptions: AtomicU32,
    subscription_requests: AtomicU64,
    subscription_failures: AtomicU64,
}

impl Metrics {
    /// Create a new Metrics instance
    pub fn new() -> Self {
        Self {
            min_publish_latency_us: AtomicU64::new(u64::MAX),
            ..Default::default()
        }
    }

    // Connection metric methods

    pub fn increment_connection_attempts(&self) {
        self.connection_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_successful_connection(&self) {
        self.successful_connections.fetch_add(1, Ordering::Relaxed);
        self.is_connected.store(true, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;
        self.connection_start_timestamp
            .store(now, Ordering::Relaxed);
    }

    pub fn record_failed_connection(&self) {
        self.failed_connections.fetch_add(1, Ordering::Relaxed);
        self.is_connected.store(false, Ordering::Relaxed);
        self.record_error();
    }

    pub fn record_disconnection(&self) {
        if self.is_connected.load(Ordering::Relaxed) {
            let start = self.connection_start_timestamp.load(Ordering::Relaxed);
            if start > 0 {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::from_secs(0))
                    .as_millis() as u64;
                let duration = now.saturating_sub(start);
                self.last_connection_duration_ms
                    .store(duration, Ordering::Relaxed);
            }
            self.is_connected.store(false, Ordering::Relaxed);
        }
    }

    pub fn increment_reconnection_count(&self) {
        self.reconnection_count.fetch_add(1, Ordering::Relaxed);
    }

    // Publish metric methods

    pub fn record_message_published(&self, qos: u8, payload_size: usize) {
        match qos {
            0 => self.messages_published_qos0.fetch_add(1, Ordering::Relaxed),
            1 => self.messages_published_qos1.fetch_add(1, Ordering::Relaxed),
            2 => self.messages_published_qos2.fetch_add(1, Ordering::Relaxed),
            _ => return,
        };
        self.total_bytes_sent
            .fetch_add(payload_size as u64, Ordering::Relaxed);
    }

    pub fn increment_publish_failures(&self) {
        self.publish_failures.fetch_add(1, Ordering::Relaxed);
        self.record_error();
    }

    pub fn record_publish_latency(&self, latency_us: u64) {
        self.total_publish_latency_us
            .fetch_add(latency_us, Ordering::Relaxed);
        self.publish_latency_samples.fetch_add(1, Ordering::Relaxed);

        // Update max
        self.max_publish_latency_us
            .fetch_max(latency_us, Ordering::Relaxed);

        // Update min
        let mut current_min = self.min_publish_latency_us.load(Ordering::Relaxed);
        while latency_us < current_min {
            match self.min_publish_latency_us.compare_exchange_weak(
                current_min,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_min) => current_min = new_min,
            }
        }
    }

    // Receive metric methods

    pub fn record_message_received(&self, qos: u8, payload_size: usize) {
        match qos {
            0 => self.messages_received_qos0.fetch_add(1, Ordering::Relaxed),
            1 => self.messages_received_qos1.fetch_add(1, Ordering::Relaxed),
            2 => self.messages_received_qos2.fetch_add(1, Ordering::Relaxed),
            _ => return,
        };
        self.total_bytes_received
            .fetch_add(payload_size as u64, Ordering::Relaxed);
    }

    // Reliability metric methods

    pub fn increment_puback_received(&self) {
        self.puback_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_pubcomp_received(&self) {
        self.pubcomp_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_publish_timeouts(&self) {
        self.publish_timeouts.fetch_add(1, Ordering::Relaxed);
        self.record_error();
    }

    pub fn record_error(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_millis() as u64;
        self.last_error_timestamp.store(now, Ordering::Relaxed);
    }

    // Subscription metric methods

    pub fn increment_active_subscriptions(&self) {
        self.active_subscriptions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement_active_subscriptions(&self) {
        self.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn increment_subscription_requests(&self) {
        self.subscription_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_subscription_failures(&self) {
        self.subscription_failures.fetch_add(1, Ordering::Relaxed);
        self.record_error();
    }

    /// Get a snapshot of all current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let latency_samples = self.publish_latency_samples.load(Ordering::Relaxed);
        let avg_latency = if latency_samples > 0 {
            self.total_publish_latency_us.load(Ordering::Relaxed) / latency_samples
        } else {
            0
        };

        let min_latency = self.min_publish_latency_us.load(Ordering::Relaxed);
        let min_latency = if min_latency == u64::MAX {
            0
        } else {
            min_latency
        };

        let connection_start = self.connection_start_timestamp.load(Ordering::Relaxed);
        let last_error = self.last_error_timestamp.load(Ordering::Relaxed);
        let last_conn_duration = self.last_connection_duration_ms.load(Ordering::Relaxed);

        MetricsSnapshot {
            connection_attempts: self.connection_attempts.load(Ordering::Relaxed),
            successful_connections: self.successful_connections.load(Ordering::Relaxed),
            failed_connections: self.failed_connections.load(Ordering::Relaxed),
            reconnection_count: self.reconnection_count.load(Ordering::Relaxed),
            is_connected: self.is_connected.load(Ordering::Relaxed),
            connection_start_timestamp: if connection_start > 0 {
                Some(connection_start)
            } else {
                None
            },
            last_connection_duration_ms: if last_conn_duration > 0 {
                Some(last_conn_duration)
            } else {
                None
            },

            messages_published_qos0: self.messages_published_qos0.load(Ordering::Relaxed),
            messages_published_qos1: self.messages_published_qos1.load(Ordering::Relaxed),
            messages_published_qos2: self.messages_published_qos2.load(Ordering::Relaxed),
            publish_failures: self.publish_failures.load(Ordering::Relaxed),
            total_bytes_sent: self.total_bytes_sent.load(Ordering::Relaxed),

            messages_received_qos0: self.messages_received_qos0.load(Ordering::Relaxed),
            messages_received_qos1: self.messages_received_qos1.load(Ordering::Relaxed),
            messages_received_qos2: self.messages_received_qos2.load(Ordering::Relaxed),
            total_bytes_received: self.total_bytes_received.load(Ordering::Relaxed),

            // pending_publish_count removed
            avg_publish_latency_us: avg_latency,
            max_publish_latency_us: self.max_publish_latency_us.load(Ordering::Relaxed),
            min_publish_latency_us: min_latency,

            puback_received: self.puback_received.load(Ordering::Relaxed),
            pubcomp_received: self.pubcomp_received.load(Ordering::Relaxed),
            publish_timeouts: self.publish_timeouts.load(Ordering::Relaxed),
            last_error_timestamp: if last_error > 0 {
                Some(last_error)
            } else {
                None
            },

            active_subscriptions: self.active_subscriptions.load(Ordering::Relaxed),
            subscription_requests: self.subscription_requests.load(Ordering::Relaxed),
            subscription_failures: self.subscription_failures.load(Ordering::Relaxed),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.connection_attempts.store(0, Ordering::Relaxed);
        self.successful_connections.store(0, Ordering::Relaxed);
        self.failed_connections.store(0, Ordering::Relaxed);
        self.reconnection_count.store(0, Ordering::Relaxed);
        self.is_connected.store(false, Ordering::Relaxed);
        self.connection_start_timestamp.store(0, Ordering::Relaxed);
        self.last_connection_duration_ms.store(0, Ordering::Relaxed);

        self.messages_published_qos0.store(0, Ordering::Relaxed);
        self.messages_published_qos1.store(0, Ordering::Relaxed);
        self.messages_published_qos2.store(0, Ordering::Relaxed);
        self.publish_failures.store(0, Ordering::Relaxed);
        self.total_bytes_sent.store(0, Ordering::Relaxed);

        self.messages_received_qos0.store(0, Ordering::Relaxed);
        self.messages_received_qos1.store(0, Ordering::Relaxed);
        self.messages_received_qos2.store(0, Ordering::Relaxed);
        self.total_bytes_received.store(0, Ordering::Relaxed);

        self.total_publish_latency_us.store(0, Ordering::Relaxed);
        self.publish_latency_samples.store(0, Ordering::Relaxed);
        self.max_publish_latency_us.store(0, Ordering::Relaxed);
        self.min_publish_latency_us
            .store(u64::MAX, Ordering::Relaxed);

        self.puback_received.store(0, Ordering::Relaxed);
        self.pubcomp_received.store(0, Ordering::Relaxed);
        self.publish_timeouts.store(0, Ordering::Relaxed);
        self.last_error_timestamp.store(0, Ordering::Relaxed);

        self.active_subscriptions.store(0, Ordering::Relaxed);
        self.subscription_requests.store(0, Ordering::Relaxed);
        self.subscription_failures.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connection_attempts, 0);
        assert_eq!(snapshot.messages_published_qos0, 0);
    }

    #[test]
    fn test_connection_metrics() {
        let metrics = Metrics::new();

        metrics.increment_connection_attempts();
        metrics.record_successful_connection();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connection_attempts, 1);
        assert_eq!(snapshot.successful_connections, 1);
        assert!(snapshot.is_connected);
        assert!(snapshot.connection_start_timestamp.is_some());
    }

    #[test]
    fn test_publish_metrics() {
        let metrics = Metrics::new();

        metrics.record_message_published(0, 100);
        metrics.record_message_published(1, 200);
        metrics.record_message_published(2, 300);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.messages_published_qos0, 1);
        assert_eq!(snapshot.messages_published_qos1, 1);
        assert_eq!(snapshot.messages_published_qos2, 1);
        assert_eq!(snapshot.total_bytes_sent, 600);
        assert_eq!(snapshot.total_messages_published(), 3);
    }

    #[test]
    fn test_latency_metrics() {
        let metrics = Metrics::new();

        metrics.record_publish_latency(100);
        metrics.record_publish_latency(200);
        metrics.record_publish_latency(50);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.avg_publish_latency_us, 116); // (100 + 200 + 50) / 3
        assert_eq!(snapshot.max_publish_latency_us, 200);
        assert_eq!(snapshot.min_publish_latency_us, 50);
    }

    #[test]
    fn test_snapshot_calculations() {
        let metrics = Metrics::new();

        metrics.record_message_published(0, 100);
        metrics.record_message_published(1, 200);
        metrics.increment_publish_failures();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_messages_published(), 2);
        assert_eq!(snapshot.avg_sent_message_size(), Some(150)); // (100 + 200) / 2
        assert_eq!(snapshot.publish_success_rate(), Some(2.0 / 3.0));
    }

    #[test]
    fn test_reset() {
        let metrics = Metrics::new();

        metrics.increment_connection_attempts();
        metrics.record_message_published(0, 100);

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connection_attempts, 0);
        assert_eq!(snapshot.messages_published_qos0, 0);
    }
}
