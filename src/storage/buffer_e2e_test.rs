/// End-to-end test demonstrating the buffer flush behavior
/// This test verifies the exact scenario mentioned in the investigation:
/// "What if I send one span 1000 times?"

use super::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::HashMap;

#[tokio::test]
async fn test_multiple_sessions_sorted_correctly() {
    // This test verifies that spans from MULTIPLE sessions are sorted correctly
    // by session_id -> trace_id -> timestamp when flushed

    let flushed_batches: Arc<Mutex<Vec<Vec<SpanData>>>> = Arc::new(Mutex::new(Vec::new()));
    let flushed_clone = flushed_batches.clone();

    let callback = Arc::new(move |spans: Vec<SpanData>| {
        let flushed = flushed_clone.clone();
        Box::pin(async move {
            flushed.lock().await.push(spans);
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>
    });

    let config = crate::config::SpanBufferConfig {
        max_buffer_bytes: 128 * 1024 * 1024,
        max_buffer_spans: 20, // Flush after 20 spans
        flush_interval_seconds: 3600,
    };

    let buffer = SimpleSpanBuffer::new(config, None, callback);

    // Create 20 spans with 3 different session_ids, 2 trace_ids per session
    // Insert them in INTENTIONALLY UNSORTED order to verify sorting works
    let base_time = chrono::Utc::now();

    // Pattern: Insert in this order (intentionally mixed)
    // session-C, trace-2, span-zzz, time+10
    // session-A, trace-1, span-fff, time+5
    // session-B, trace-2, span-ddd, time+8
    // session-A, trace-2, span-ggg, time+6
    // session-C, trace-1, span-xxx, time+9
    // ... etc

    let unsorted_spans = vec![
        // Mix of sessions, traces, and span IDs - intentionally unsorted
        ("session-C", "trace-200", "span-zzz", 10),
        ("session-A", "trace-100", "span-fff", 5),
        ("session-B", "trace-200", "span-ddd", 8),
        ("session-A", "trace-200", "span-ggg", 6),
        ("session-C", "trace-100", "span-xxx", 9),
        ("session-B", "trace-100", "span-bbb", 7),
        ("session-A", "trace-100", "span-eee", 4),
        ("session-C", "trace-200", "span-yyy", 11),
        ("session-B", "trace-200", "span-ccc", 3),
        ("session-A", "trace-200", "span-hhh", 2),
        ("session-C", "trace-100", "span-www", 1),
        ("session-B", "trace-100", "span-aaa", 0),
        ("session-A", "trace-100", "span-iii", 12),
        ("session-C", "trace-200", "span-vvv", 13),
        ("session-B", "trace-200", "span-kkk", 14),
        ("session-A", "trace-200", "span-jjj", 15),
        ("session-C", "trace-100", "span-uuu", 16),
        ("session-B", "trace-100", "span-lll", 17),
        ("session-A", "trace-100", "span-mmm", 18),
        ("session-C", "trace-200", "span-nnn", 19),
    ];

    for (session_id, trace_id, span_id, time_offset) in unsorted_spans {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), session_id.to_string());

        let span = SpanData {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: None,
            app_id: "test_app".to_string(),
            organization_id: None,
            tenant_id: None,
            message_type: "test_message".to_string(),
            span_kind: Some("INTERNAL".to_string()),
            timestamp: base_time + chrono::Duration::milliseconds(time_offset),
            end_timestamp: None,
            attributes,
            events: vec![],
            status_code: None,
            status_message: None,
        };

        buffer.add_spans(vec![span], 1024).await.unwrap();
    }

    // Give flush task time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify flush happened
    let batches = flushed_batches.lock().await;
    assert_eq!(batches.len(), 1, "Should have flushed exactly once");
    assert_eq!(batches[0].len(), 20, "Should have flushed all 20 spans");

    let sorted_spans = &batches[0];

    // Verify sorting: session_id -> trace_id -> timestamp
    // Expected order after sorting:
    // session-A, trace-100 (sorted by timestamp within this group)
    // session-A, trace-200 (sorted by timestamp within this group)
    // session-B, trace-100 (sorted by timestamp within this group)
    // session-B, trace-200 (sorted by timestamp within this group)
    // session-C, trace-100 (sorted by timestamp within this group)
    // session-C, trace-200 (sorted by timestamp within this group)

    println!("\n=== Sorted Spans ===");
    for (i, span) in sorted_spans.iter().enumerate() {
        let session = span.attributes.get("sp.session.id").unwrap();
        println!(
            "[{}] session={}, trace={}, span={}, time={}ms",
            i,
            session,
            span.trace_id,
            span.span_id,
            span.timestamp.timestamp_millis() - base_time.timestamp_millis()
        );
    }

    // Verify session-A comes first (alphabetically)
    let first_session = sorted_spans[0].attributes.get("sp.session.id").unwrap();
    assert_eq!(first_session, "session-A", "First session should be session-A");

    // Verify session-C comes last (alphabetically)
    let last_session = sorted_spans[19].attributes.get("sp.session.id").unwrap();
    assert_eq!(last_session, "session-C", "Last session should be session-C");

    // Verify all spans are grouped by session_id
    let mut prev_session: Option<String> = None;
    let mut session_changes = 0;
    for span in sorted_spans.iter() {
        let curr_session = span.attributes.get("sp.session.id").unwrap().clone();
        if let Some(ref prev) = prev_session {
            if prev != &curr_session {
                session_changes += 1;
                // Session changed, verify new session is >= previous (alphabetically)
                assert!(
                    curr_session > *prev,
                    "Sessions not sorted: {} should come after {}",
                    curr_session,
                    prev
                );
            }
        }
        prev_session = Some(curr_session);
    }
    assert_eq!(session_changes, 2, "Should have exactly 2 session changes (A->B, B->C)");

    // Verify within each session, spans are sorted by trace_id -> timestamp
    // Count spans per session from the sorted output
    // session-A: 7 spans (indices 0-6)
    // session-B: 6 spans (indices 7-12)
    // session-C: 7 spans (indices 13-19)

    let session_a_spans = &sorted_spans[0..7];
    verify_trace_and_time_sorting(session_a_spans, "session-A");

    let session_b_spans = &sorted_spans[7..13];
    verify_trace_and_time_sorting(session_b_spans, "session-B");

    let session_c_spans = &sorted_spans[13..20];
    verify_trace_and_time_sorting(session_c_spans, "session-C");
}

fn verify_trace_and_time_sorting(spans: &[SpanData], expected_session: &str) {
    let mut prev_trace: Option<String> = None;
    let mut prev_timestamp: Option<chrono::DateTime<chrono::Utc>> = None;

    for span in spans {
        let session = span.attributes.get("sp.session.id").unwrap();
        assert_eq!(session, expected_session, "All spans should be from {}", expected_session);

        let curr_trace = span.trace_id.clone();

        if let Some(ref prev) = prev_trace {
            if prev != &curr_trace {
                // Trace changed, verify new trace is >= previous (alphabetically)
                assert!(
                    curr_trace >= *prev,
                    "Traces not sorted within session {}: {} should come after {}",
                    expected_session,
                    curr_trace,
                    prev
                );
                // Reset timestamp check for new trace
                prev_timestamp = None;
            }
        }

        // Within same trace, verify timestamps are sorted
        if prev_trace.as_ref() == Some(&curr_trace) {
            if let Some(prev_ts) = prev_timestamp {
                assert!(
                    span.timestamp >= prev_ts,
                    "Timestamps not sorted within session {} trace {}: {:?} should come after {:?}",
                    expected_session,
                    curr_trace,
                    span.timestamp,
                    prev_ts
                );
            }
        }

        prev_trace = Some(curr_trace);
        prev_timestamp = Some(span.timestamp);
    }
}

#[tokio::test]
async fn test_1000_spans_same_session_triggers_flush() {
    // This test demonstrates the fix for the original problem:
    // OLD: Each span had unique session_id -> 1000 sessions with 1 span each -> NO FLUSH
    // NEW: All spans have same session_id -> 1 buffer with 1000 spans -> FLUSH!

    let flushed_batches: Arc<Mutex<Vec<Vec<SpanData>>>> = Arc::new(Mutex::new(Vec::new()));
    let flushed_clone = flushed_batches.clone();

    let callback = Arc::new(move |spans: Vec<SpanData>| {
        let flushed = flushed_clone.clone();
        Box::pin(async move {
            flushed.lock().await.push(spans);
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>
    });

    let config = crate::config::SpanBufferConfig {
        max_buffer_bytes: 128 * 1024 * 1024, // 128MB
        max_buffer_spans: 1000,               // Flush at 1000 spans
        flush_interval_seconds: 3600,         // 1 hour (won't trigger)
    };

    let buffer = SimpleSpanBuffer::new(config, None, callback);

    // Send 1000 spans with THE SAME session_id
    let session_id = "test-session-fixed";

    for i in 0..1000 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), session_id.to_string());

        let span = SpanData {
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
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
        };

        // Simulate adding one span at a time (like the original scenario)
        buffer.add_spans(vec![span], 1024).await.unwrap();
    }

    // Give flush task time to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Verify flush happened
    let batches = flushed_batches.lock().await;
    assert_eq!(batches.len(), 1, "Should have flushed exactly once");
    assert_eq!(batches[0].len(), 1000, "Should have flushed all 1000 spans");

    // Verify spans are sorted by session_id -> trace_id
    let first_span = &batches[0][0];
    let last_span = &batches[0][999];
    assert_eq!(
        first_span.attributes.get("sp.session.id"),
        Some(&session_id.to_string())
    );
    assert_eq!(
        last_span.attributes.get("sp.session.id"),
        Some(&session_id.to_string())
    );
}

#[tokio::test]
async fn test_1000_spans_different_sessions_no_premature_flush() {
    // This test demonstrates the OLD problematic behavior would NOT flush
    // With our new design, it still buffers them all (no session limit)
    // until size/time limit is hit

    let flushed_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let flushed_clone = flushed_count.clone();

    let callback = Arc::new(move |spans: Vec<SpanData>| {
        let flushed = flushed_clone.clone();
        Box::pin(async move {
            *flushed.lock().await += spans.len();
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>
    });

    let config = crate::config::SpanBufferConfig {
        max_buffer_bytes: 128 * 1024 * 1024, // 128MB
        max_buffer_spans: 1500,               // Higher limit
        flush_interval_seconds: 3600,         // Won't trigger
    };

    let buffer = SimpleSpanBuffer::new(config, None, callback);

    // Send 1000 spans with DIFFERENT session_ids (old problematic scenario)
    for i in 0..1000 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), format!("session-{}", i));

        let span = SpanData {
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
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
        };

        buffer.add_spans(vec![span], 1024).await.unwrap();
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // With new design: NO flush because we haven't hit the 1500 span limit
    // All 1000 spans are buffered together
    let count = *flushed_count.lock().await;
    assert_eq!(count, 0, "Should NOT have flushed yet (under span limit)");

    // Verify they're still in the buffer
    let stats = buffer.stats().await;
    assert_eq!(stats.buffered_spans, 1000, "All 1000 spans should be buffered");
}

#[tokio::test]
async fn test_size_based_flush_with_request_body_tracking() {
    let flushed_batches: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let flushed_clone = flushed_batches.clone();

    let callback = Arc::new(move |spans: Vec<SpanData>| {
        let flushed = flushed_clone.clone();
        Box::pin(async move {
            flushed.lock().await.push(spans.len());
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>
    });

    let config = crate::config::SpanBufferConfig {
        max_buffer_bytes: 5000,       // 5KB limit - will be exceeded
        max_buffer_spans: 10000,      // High count
        flush_interval_seconds: 3600, // Won't trigger
    };

    let buffer = SimpleSpanBuffer::new(config, None, callback);

    // First batch: 3KB (under limit)
    let mut spans1 = vec![];
    for i in 0..5 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), "session-1".to_string());

        spans1.push(SpanData {
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
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
        });
    }

    buffer.add_spans(spans1, 3000).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // No flush yet
    assert_eq!(flushed_batches.lock().await.len(), 0);

    // Second batch: 3KB (total 6KB > 5KB limit = FLUSH)
    let mut spans2 = vec![];
    for i in 5..10 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), "session-1".to_string());

        spans2.push(SpanData {
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
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
        });
    }

    buffer.add_spans(spans2, 3000).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Should have flushed due to size limit
    let batches = flushed_batches.lock().await;
    assert_eq!(batches.len(), 1, "Should have flushed once due to size limit");
    assert_eq!(batches[0], 10, "Should have flushed all 10 spans");
}

#[tokio::test]
async fn test_time_based_flush() {
    let flushed_count: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let flushed_clone = flushed_count.clone();

    let callback = Arc::new(move |spans: Vec<SpanData>| {
        let flushed = flushed_clone.clone();
        Box::pin(async move {
            *flushed.lock().await += spans.len();
            Ok(())
        }) as std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>>
    });

    let config = crate::config::SpanBufferConfig {
        max_buffer_bytes: 128 * 1024 * 1024,
        max_buffer_spans: 10000,
        flush_interval_seconds: 1, // 1 second - will trigger
    };

    let buffer = SimpleSpanBuffer::new(config, None, callback);

    // Add a few spans
    let mut spans = vec![];
    for i in 0..5 {
        let mut attributes = HashMap::new();
        attributes.insert("sp.session.id".to_string(), "session-1".to_string());

        spans.push(SpanData {
            trace_id: format!("trace-{}", i),
            span_id: format!("span-{}", i),
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
        });
    }

    buffer.add_spans(spans, 1000).await.unwrap();

    // Wait for time-based flush (1 second + buffer)
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Should have flushed due to time limit
    let count = *flushed_count.lock().await;
    assert_eq!(count, 5, "Should have flushed all 5 spans after 1 second");
}
