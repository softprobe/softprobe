use super::*;
use super::buffer::FlushCallback;
use std::sync::Arc;
use tokio::sync::Mutex;

// Test helper to track flushes
struct FlushTracker {
    flushed_batches: Arc<Mutex<Vec<usize>>>, // Track flushed span counts
}

impl FlushTracker {
    fn new() -> Self {
        Self {
            flushed_batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn create_callback(&self) -> Arc<FlushCallback<SpanData>> {
        let tracker = self.flushed_batches.clone();
        Arc::new(move |spans: Vec<SpanData>| -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
            let tracker = tracker.clone();
            Box::pin(async move {
                tracker.lock().await.push(spans.len());
                Ok(())
            })
        })
    }

    async fn get_flush_counts(&self) -> Vec<usize> {
        self.flushed_batches.lock().await.clone()
    }
}

fn create_test_span(span_id: &str, trace_id: &str, session_id: Option<&str>) -> SpanData {
    let mut attributes = HashMap::new();
    if let Some(sid) = session_id {
        attributes.insert("sp.session.id".to_string(), sid.to_string());
    }

    SpanData {
        trace_id: trace_id.to_string(),
        span_id: span_id.to_string(),
        parent_span_id: None,
        app_id: "test_app".to_string(),
        organization_id: None,
        tenant_id: None,
        message_type: "test_message".to_string(),
        span_kind: Some("INTERNAL".to_string()),
        timestamp: chrono::Utc::now(),
        end_timestamp: None,
        attributes,
        events: vec![],
        status_code: None,
        status_message: None,
    }
}

#[tokio::test]
async fn test_buffer_flushes_on_span_count_limit() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 10_000_000, // 10MB
        max_buffer_spans: 5,           // Flush at 5 spans
        flush_interval_seconds: 3600,  // 1 hour (won't trigger)
    };

    let buffer = SimpleSpanBuffer::new(config, None, tracker.create_callback());

    // Add 5 spans - should trigger flush
    let mut spans = vec![];
    for i in 0..5 {
        spans.push(create_test_span(&format!("span_{}", i), "trace_1", Some("session_1")));
    }

    buffer.add_spans(spans, 1000).await.unwrap();

    // Give flush task time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let flush_counts = tracker.get_flush_counts().await;
    assert_eq!(flush_counts.len(), 1, "Should have flushed once");
    assert_eq!(flush_counts[0], 5, "Should have flushed 5 spans");
}

#[tokio::test]
async fn test_buffer_flushes_on_size_limit() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 2000,        // 2KB - will be exceeded
        max_buffer_spans: 1000,        // High count limit
        flush_interval_seconds: 3600,  // 1 hour (won't trigger)
    };

    let buffer = SimpleSpanBuffer::new(config, None, tracker.create_callback());

    // Add spans with large body size
    let spans = vec![
        create_test_span("span_1", "trace_1", Some("session_1")),
        create_test_span("span_2", "trace_1", Some("session_1")),
    ];

    buffer.add_spans(spans, 2500).await.unwrap(); // Body size exceeds limit

    // Give flush task time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let flush_counts = tracker.get_flush_counts().await;
    assert_eq!(flush_counts.len(), 1, "Should have flushed once due to size limit");
    assert_eq!(flush_counts[0], 2, "Should have flushed 2 spans");
}

#[tokio::test]
async fn test_buffer_flushes_on_time_interval() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 10_000_000,  // 10MB
        max_buffer_spans: 1000,        // High count limit
        flush_interval_seconds: 1,     // 1 second
    };

    let buffer = SimpleSpanBuffer::new(config, None, tracker.create_callback());

    // Add a few spans
    let spans = vec![
        create_test_span("span_1", "trace_1", Some("session_1")),
        create_test_span("span_2", "trace_1", Some("session_1")),
    ];

    buffer.add_spans(spans, 500).await.unwrap();

    // Wait for time-based flush to trigger
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let flush_counts = tracker.get_flush_counts().await;
    assert_eq!(flush_counts.len(), 1, "Should have flushed once due to time interval");
    assert_eq!(flush_counts[0], 2, "Should have flushed 2 spans");
}

#[tokio::test]
async fn test_spans_sorted_by_session_and_trace() {
    let flushed_spans: Arc<Mutex<Vec<SpanData>>> = Arc::new(Mutex::new(Vec::new()));
    let flushed_spans_clone = flushed_spans.clone();

    let callback = Arc::new(move |spans: Vec<SpanData>| -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
        let flushed = flushed_spans_clone.clone();
        Box::pin(async move {
            flushed.lock().await.extend(spans);
            Ok(())
        })
    });

    let config = SpanBufferConfig {
        max_buffer_bytes: 10_000_000,
        max_buffer_spans: 10,
        flush_interval_seconds: 3600,
    };

    let buffer = SimpleSpanBuffer::new(config, None, callback);

    // Add spans in mixed order
    let spans = vec![
        create_test_span("span_1", "trace_2", Some("session_2")),
        create_test_span("span_2", "trace_1", Some("session_1")),
        create_test_span("span_3", "trace_2", Some("session_1")),
        create_test_span("span_4", "trace_1", Some("session_2")),
        create_test_span("span_5", "trace_1", Some("session_1")),
    ];

    buffer.add_spans(spans, 1000).await.unwrap();

    // Force flush
    buffer.force_flush().await.unwrap();

    // Wait for flush to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let flushed = flushed_spans.lock().await;
    assert_eq!(flushed.len(), 5, "Should have flushed all 5 spans");

    // Verify sorting: session_id -> trace_id -> timestamp
    // Expected order:
    // session_1/trace_1, session_1/trace_1, session_1/trace_2, session_2/trace_1, session_2/trace_2
    assert_eq!(flushed[0].span_id, "span_2"); // session_1/trace_1
    assert_eq!(flushed[1].span_id, "span_5"); // session_1/trace_1
    assert_eq!(flushed[2].span_id, "span_3"); // session_1/trace_2
    assert_eq!(flushed[3].span_id, "span_4"); // session_2/trace_1
    assert_eq!(flushed[4].span_id, "span_1"); // session_2/trace_2
}

#[tokio::test]
async fn test_buffer_stats() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 10_000_000,
        max_buffer_spans: 1000,
        flush_interval_seconds: 3600,
    };

    let buffer = SimpleSpanBuffer::new(config, None, tracker.create_callback());

    // Initially empty
    let stats = buffer.stats().await;
    assert_eq!(stats.buffered_spans, 0);
    assert_eq!(stats.buffered_bytes, 0);

    // Add some spans
    let spans = vec![
        create_test_span("span_1", "trace_1", Some("session_1")),
        create_test_span("span_2", "trace_1", Some("session_1")),
    ];

    buffer.add_spans(spans, 1500).await.unwrap();

    let stats = buffer.stats().await;
    assert_eq!(stats.buffered_spans, 2);
    assert_eq!(stats.buffered_bytes, 1500);
}

/// Test concurrent additions to detect race conditions
#[tokio::test]
async fn test_concurrent_adds_no_lost_spans() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 1_000_000,
        max_buffer_spans: 1, // Flush after each span
        flush_interval_seconds: 3600,
    };

    let buffer = Arc::new(SimpleSpanBuffer::new(config, None, tracker.create_callback()));

    // Spawn 10 concurrent tasks, each adding 1 span
    let mut handles = vec![];
    for i in 0..10 {
        let buffer_clone = buffer.clone();
        let handle = tokio::spawn(async move {
            let span = create_test_span(
                &format!("span_{}", i),
                &format!("trace_{}", i),
                Some(&format!("session_{}", i)),
            );
            buffer_clone.add_spans(vec![span], 100).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Wait a bit for any pending flushes
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Force final flush to ensure all spans are written
    buffer.force_flush().await.unwrap();

    // Verify all 10 spans were flushed
    let flush_counts = tracker.get_flush_counts().await;
    let total_flushed: usize = flush_counts.iter().sum();
    assert_eq!(
        total_flushed, 10,
        "Expected 10 spans to be flushed, but got {}. Flush batches: {:?}",
        total_flushed, flush_counts
    );
}

/// Test concurrent additions with different flush scenarios
#[tokio::test]
async fn test_concurrent_adds_batch_sizes() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 1_000_000,
        max_buffer_spans: 5, // Flush after 5 spans
        flush_interval_seconds: 3600,
    };

    let buffer = Arc::new(SimpleSpanBuffer::new(config, None, tracker.create_callback()));

    // Spawn 20 concurrent tasks (should result in 4 flushes of 5 spans each)
    let mut handles = vec![];
    for i in 0..20 {
        let buffer_clone = buffer.clone();
        let handle = tokio::spawn(async move {
            let span = create_test_span(
                &format!("span_{}", i),
                &format!("trace_{}", i),
                Some(&format!("session_{}", i)),
            );
            buffer_clone.add_spans(vec![span], 100).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Wait a bit for any pending flushes
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force final flush
    buffer.force_flush().await.unwrap();

    // Verify all 20 spans were flushed
    let flush_counts = tracker.get_flush_counts().await;
    let total_flushed: usize = flush_counts.iter().sum();
    assert_eq!(
        total_flushed, 20,
        "Expected 20 spans to be flushed, but got {}. Flush batches: {:?}",
        total_flushed, flush_counts
    );
}

/// Test rapid sequential adds (stress test for race conditions)
#[tokio::test]
async fn test_rapid_sequential_adds() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 1_000_000,
        max_buffer_spans: 1, // Flush immediately
        flush_interval_seconds: 3600,
    };

    let buffer = SimpleSpanBuffer::new(config, None, tracker.create_callback());

    // Rapidly add 50 spans sequentially
    for i in 0..50 {
        let span = create_test_span(
            &format!("span_{}", i),
            &format!("trace_{}", i),
            Some(&format!("session_{}", i)),
        );
        buffer.add_spans(vec![span], 100).await.unwrap();
    }

    // Wait for any pending async operations
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Force final flush
    buffer.force_flush().await.unwrap();

    // Verify all 50 spans were flushed
    let flush_counts = tracker.get_flush_counts().await;
    let total_flushed: usize = flush_counts.iter().sum();
    assert_eq!(
        total_flushed, 50,
        "Expected 50 spans to be flushed, but got {}. Flush batches: {:?}",
        total_flushed, flush_counts
    );
}

/// Test interleaved concurrent and sequential operations
#[tokio::test]
async fn test_mixed_concurrent_sequential_adds() {
    let tracker = FlushTracker::new();
    let config = SpanBufferConfig {
        max_buffer_bytes: 1_000_000,
        max_buffer_spans: 3, // Flush after 3 spans
        flush_interval_seconds: 3600,
    };

    let buffer = Arc::new(SimpleSpanBuffer::new(config, None, tracker.create_callback()));

    // Start with some sequential adds
    for i in 0..5 {
        let span = create_test_span(
            &format!("seq_span_{}", i),
            &format!("trace_{}", i),
            Some(&format!("session_{}", i)),
        );
        buffer.add_spans(vec![span], 100).await.unwrap();
    }

    // Now do concurrent adds
    let mut handles = vec![];
    for i in 0..10 {
        let buffer_clone = buffer.clone();
        let handle = tokio::spawn(async move {
            let span = create_test_span(
                &format!("concurrent_span_{}", i),
                &format!("trace_{}", i + 100),
                Some(&format!("session_{}", i + 100)),
            );
            buffer_clone.add_spans(vec![span], 100).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for concurrent tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // More sequential adds
    for i in 0..5 {
        let span = create_test_span(
            &format!("final_span_{}", i),
            &format!("trace_{}", i + 200),
            Some(&format!("session_{}", i + 200)),
        );
        buffer.add_spans(vec![span], 100).await.unwrap();
    }

    // Wait and force flush
    tokio::time::sleep(Duration::from_millis(200)).await;
    buffer.force_flush().await.unwrap();

    // Verify all 20 spans (5 + 10 + 5) were flushed
    let flush_counts = tracker.get_flush_counts().await;
    let total_flushed: usize = flush_counts.iter().sum();
    assert_eq!(
        total_flushed, 20,
        "Expected 20 spans to be flushed, but got {}. Flush batches: {:?}",
        total_flushed, flush_counts
    );
}
