use std::time::Instant;

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    operation: String,
    duration_ms: u128,
    rows_processed: usize,
    bytes_processed: Option<usize>,
    files_scanned: Option<usize>,
    partitions_scanned: Option<usize>,
    throughput_rows_per_sec: f64,
    throughput_mb_per_sec: Option<f64>,
}

impl PerformanceMetrics {
    pub fn new(operation: &str) -> Self {
        Self {
            operation: operation.to_string(),
            duration_ms: 0,
            rows_processed: 0,
            bytes_processed: None,
            files_scanned: None,
            partitions_scanned: None,
            throughput_rows_per_sec: 0.0,
            throughput_mb_per_sec: None,
        }
    }

    pub fn with_duration(mut self, duration: std::time::Duration) -> Self {
        self.duration_ms = duration.as_millis();
        self.calculate_throughput();
        self
    }

    pub fn with_rows(mut self, rows: usize) -> Self {
        self.rows_processed = rows;
        self.calculate_throughput();
        self
    }

    fn calculate_throughput(&mut self) {
        if self.duration_ms > 0 {
            let duration_sec = self.duration_ms as f64 / 1000.0;
            self.throughput_rows_per_sec = self.rows_processed as f64 / duration_sec;

            if let Some(bytes) = self.bytes_processed {
                let mb = bytes as f64 / (1024.0 * 1024.0);
                self.throughput_mb_per_sec = Some(mb / duration_sec);
            }
        }
    }

    pub fn print_report(&self) {
        println!("\n📊 Performance Metrics Report: {}", self.operation);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("⏱️  Duration:              {} ms", self.duration_ms);
        println!("📝 Rows Processed:        {}", self.rows_processed);

        if let Some(bytes) = self.bytes_processed {
            println!(
                "💾 Bytes Processed:       {} ({:.2} MB)",
                bytes,
                bytes as f64 / (1024.0 * 1024.0)
            );
        }

        if let Some(files) = self.files_scanned {
            println!("📁 Files Scanned:         {}", files);
        }

        if let Some(partitions) = self.partitions_scanned {
            println!("🗂️  Partitions Scanned:    {}", partitions);
        }

        println!("⚡ Throughput (rows/sec): {:.2}", self.throughput_rows_per_sec);

        if let Some(mb_per_sec) = self.throughput_mb_per_sec {
            println!("⚡ Throughput (MB/sec):   {:.2}", mb_per_sec);
        }

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    pub fn assert_performance_target(&self, max_duration_ms: u128, target_name: &str) {
        if self.duration_ms > max_duration_ms {
            println!(
                "⚠️  WARNING: {} exceeded target of {} ms (actual: {} ms)",
                target_name, max_duration_ms, self.duration_ms
            );
        } else {
            println!(
                "✅ {} within target of {} ms (actual: {} ms)",
                target_name, max_duration_ms, self.duration_ms
            );
        }
    }
}

pub struct Timer {
    start: Instant,
    operation: String,
}

impl Timer {
    pub fn start(operation: &str) -> Self {
        println!("⏱️  Starting: {}", operation);
        Self {
            start: Instant::now(),
            operation: operation.to_string(),
        }
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    pub fn stop(&self) -> std::time::Duration {
        let duration = self.elapsed();
        println!("⏹️  Completed: {} in {:?}", self.operation, duration);
        duration
    }
}

