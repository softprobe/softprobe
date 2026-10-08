#!/usr/bin/env python3
"""
SoftProbe OTLP Backend Deployment Test Script

This script tests the deployed OTLP backend (GCP or local) by:
1. Generating a unique session ID to avoid data conflicts
2. Sending OTLP trace data with configurable span count
3. Waiting for buffer flush (respects environment-specific timeouts)
4. Querying the session data via the query API
5. Validating that all spans were ingested correctly

Usage:
    # Test GCP deployment (default: small dataset)
    python test_deployment.py --env gcp

    # Test local deployment
    python test_deployment.py --env local

    # Test with large dataset to trigger buffer flush
    python test_deployment.py --env gcp --span-count 10000

    # Test with stress dataset (>10K spans to test multiple flushes)
    python test_deployment.py --env local --span-count 20000

Environment Variables:
    OTLP_ENDPOINT - Override the OTLP endpoint URL
    OTLP_TIMEOUT  - Override the wait timeout (seconds)
"""

import argparse
import gzip
import json
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests

# Environment configurations
ENVIRONMENTS = {
    "gcp": {
        "name": "GCP Production",
        "otlp_endpoint": "https://i.softprobe.ai",
        "buffer_timeout": 60,  # 60s production timeout
        "buffer_size_spans": 10000,  # 10K spans trigger
        "buffer_size_bytes": 128 * 1024 * 1024,  # 128MB
        "description": "Production deployment with 60s buffer, 128MB, 10K spans"
    },
    "local": {
        "name": "Local Development",
        "otlp_endpoint": "http://localhost:8090",
        "buffer_timeout": 1,  # 1s local timeout
        "buffer_size_spans": 1,  # 1 span trigger (test config)
        "buffer_size_bytes": 128 * 1024 * 1024,
        "description": "Local MinIO deployment with fast flush for testing"
    }
}


class Colors:
    """Terminal colors for output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


class OTLPDeploymentTester:
    """Tests OTLP backend deployment"""

    def __init__(self, env: str, endpoint: Optional[str] = None, timeout: Optional[int] = None, session_count: int = 1):
        self.env_config = ENVIRONMENTS[env]
        self.endpoint = endpoint or self.env_config["otlp_endpoint"]
        self.timeout = timeout or self.env_config["buffer_timeout"]
        self.session_count = session_count
        
        # Generate session IDs
        if session_count > 1:
            self.session_ids = [f"test-{env}-{uuid.uuid4()}" for _ in range(session_count)]
            self.session_id = self.session_ids[0]  # Primary session ID for verification
        else:
            self.session_id = f"test-{env}-{uuid.uuid4()}"
            self.session_ids = [self.session_id]

        print(f"{Colors.HEADER}=== SoftProbe OTLP Backend Deployment Test ==={Colors.ENDC}")
        print(f"{Colors.OKBLUE}Environment: {self.env_config['name']}{Colors.ENDC}")
        print(f"{Colors.OKBLUE}Endpoint: {self.endpoint}{Colors.ENDC}")
        if session_count > 1:
            print(f"{Colors.OKBLUE}Session IDs: {session_count} sessions (verifying first: {self.session_id}){Colors.ENDC}")
        else:
            print(f"{Colors.OKBLUE}Session ID: {self.session_id}{Colors.ENDC}")
        print(f"{Colors.OKBLUE}Buffer timeout: {self.timeout}s{Colors.ENDC}")
        print(f"{Colors.OKBLUE}Description: {self.env_config['description']}{Colors.ENDC}")
        print()

    def test_health(self) -> bool:
        """Test health endpoint"""
        print(f"{Colors.OKCYAN}[1/5] Testing health endpoint...{Colors.ENDC}")
        try:
            response = requests.get(f"{self.endpoint}/health", timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"{Colors.OKGREEN}✓ Health check passed: {data}{Colors.ENDC}")
                return True
            else:
                print(f"{Colors.FAIL}✗ Health check failed: HTTP {response.status_code}{Colors.ENDC}")
                return False
        except Exception as e:
            print(f"{Colors.FAIL}✗ Health check failed: {e}{Colors.ENDC}")
            return False

    def _generate_realistic_padding(self, span_index: int, target_bytes: int) -> Dict:
        """Generate realistic, dynamic JSON padding that doesn't compress well (optimized)"""
        import random
        import hashlib
        
        # Use span_index as seed for deterministic but varied content
        rng = random.Random(span_index)
        
        # Generate deterministic IDs based on span_index to avoid expensive UUID calls
        def make_id(prefix: str, idx: int) -> str:
            """Create deterministic ID from span_index and idx"""
            data = f"{prefix}-{span_index}-{idx}".encode()
            return hashlib.md5(data).hexdigest()[:16]
        
        # Create varied JSON structures: nested objects, arrays, mixed data types
        padding_data = {
            "metadata": {
                "requestId": f"req-{span_index}-{make_id('req', 0)}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "version": f"1.{rng.randint(0, 9)}.{rng.randint(0, 99)}",
                "environment": rng.choice(["production", "staging", "development"]),
                "region": rng.choice(["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]),
            },
            "items": [],
            "details": {},
            "logs": []
        }
        
        # Generate items array with varied content (reduced count for performance)
        items_count = 30 + (span_index % 50)  # Vary between 30-80 items (reduced from 50-150)
        for i in range(items_count):
            item = {
                "id": f"item-{span_index}-{i}-{make_id('item', i)}",
                "name": f"Product {i}",
                "price": round(rng.uniform(10.0, 999.99), 2),
                "quantity": rng.randint(1, 100),
                "category": rng.choice(["electronics", "clothing", "food", "books", "toys"]),
                "tags": [f"tag-{j}" for j in range(rng.randint(2, 8))],
                "description": f"Detailed description for item {i} with various characteristics and features that provide context and information",
                "inStock": rng.choice([True, False]),
                "rating": round(rng.uniform(1.0, 5.0), 1),
            }
            padding_data["items"].append(item)
        
        # Generate details object with nested structures
        for key in ["shipping", "payment", "customer", "analytics"]:
            padding_data["details"][key] = {
                "method": rng.choice(["standard", "express", "overnight"]),
                "cost": round(rng.uniform(5.0, 50.0), 2),
                "tracking": f"TRK-{make_id('track', hash(key) % 1000).upper()}",
                "estimatedDays": rng.randint(1, 7),
                "options": {f"opt-{j}": f"value-{j}-{rng.randint(1000, 9999)}" for j in range(5)}
            }
        
        # Generate log entries with varied text content (reduced count)
        log_count = 15 + (span_index % 20)  # Vary between 15-35 (reduced from 20-50)
        for i in range(log_count):
            log_entry = {
                "level": rng.choice(["INFO", "DEBUG", "WARN", "ERROR"]),
                "message": f"Processing step {i} completed with status {rng.choice(['success', 'pending', 'retry'])}",
                "timestamp": (datetime.now(timezone.utc).timestamp() + i * 0.1),
                "context": {
                    "user.id": f"user-{rng.randint(1000, 9999)}",
                    "sessionId": f"sess-{make_id('sess', i)}",
                    "ipAddress": f"{rng.randint(1, 255)}.{rng.randint(1, 255)}.{rng.randint(1, 255)}.{rng.randint(1, 255)}",
                    "userAgent": rng.choice([
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                    ])
                },
                "data": {f"field-{j}": f"value-{rng.randint(10000, 99999)}" for j in range(3)}
            }
            padding_data["logs"].append(log_entry)
        
        # Add additional random fields to increase size and variation (reduced count)
        padding_data["extras"] = {
            f"extra-{i}": {
                "value": rng.randint(100000, 999999),
                "text": f"Some descriptive text for extra field {i} with additional context and details",
                "nested": {
                    "a": rng.randint(1, 100),
                    "b": f"string-{rng.randint(1000, 9999)}",
                    "c": rng.choice([True, False, None])
                }
            }
            for i in range(20 + (span_index % 15))  # Reduced from 30-50
        }
        
        # Serialize to check size
        padding_json = json.dumps(padding_data)
        current_size = len(padding_json.encode('utf-8'))
        
        # If still too small, add more content iteratively (use deterministic IDs)
        while current_size < target_bytes:
            remaining = target_bytes - current_size
            # Add more items to reach target size
            additional_items = max(1, remaining // 250)  # Slightly larger estimate per item
            start_idx = len(padding_data["items"])
            for i in range(additional_items):
                item = {
                    "id": f"item-extra-{span_index}-{start_idx + i}-{make_id('extra', start_idx + i)}",
                    "name": f"Additional Product {start_idx + i}",
                    "price": round(rng.uniform(10.0, 999.99), 2),
                    "description": f"Extended description with more details about product {start_idx + i} and its various features",
                    "specs": {f"spec-{j}": f"value-{rng.randint(10000, 99999)}" for j in range(5)}
                }
                padding_data["items"].append(item)
            
            padding_json = json.dumps(padding_data)
            current_size = len(padding_json.encode('utf-8'))
            
            # Safety check to avoid infinite loop
            if current_size >= target_bytes * 0.95:  # Close enough
                break
        
        return padding_data

    def _create_large_body(self, base_data: Dict, target_size_kb: int = 100, span_index: int = 0) -> str:
        """Create a JSON body of approximately target_size_kb KB with realistic padding"""
        target_size = target_size_kb * 1024  # Convert KB to bytes
        
        # Start with base data
        current_json = json.dumps(base_data)
        current_size = len(current_json.encode('utf-8'))
        
        # Generate realistic padding that varies per span
        if current_size < target_size:
            padding_needed = target_size - current_size
            # Generate realistic padding data
            padding_data = self._generate_realistic_padding(span_index, padding_needed)
            base_data["payload"] = padding_data
            current_json = json.dumps(base_data)
            current_size = len(current_json.encode('utf-8'))
            
            # Fine-tune if needed (should be close already)
            if current_size < target_size * 0.95:
                # Add a small text field to reach target
                remaining = target_size - current_size
                base_data["additionalData"] = "x" * max(0, remaining - 50)
                current_json = json.dumps(base_data)
        
        return current_json

    def create_otlp_span(self, span_index: int, session_id: Optional[str] = None) -> Dict:
        """Create a single OTLP span for testing with SoftProbe-specific attributes"""
        trace_id_bytes = list(uuid.uuid4().bytes)
        span_id_bytes = list(uuid.uuid4().bytes[:8])
        now_nanos = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
        
        # Use provided session_id or default to primary session_id
        span_session_id = session_id or self.session_id

        # Generate ~100KB request body with realistic, dynamic padding
        request_base_data = {
            "user.id": f"user_{span_index}",
            "action": "test_action",
            "index": span_index,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        request_body = self._create_large_body(request_base_data, target_size_kb=100, span_index=span_index)

        # Generate ~100KB response body with realistic, dynamic padding
        response_base_data = {
            "success": True,
            "result": {
                "id": f"result_{span_index}",
                "status": "completed",
                "index": span_index
            },
            "message": "Test operation successful"
        }
        response_body = self._create_large_body(response_base_data, target_size_kb=100, span_index=span_index + 1000000)  # Different seed for response

        return {
            "traceId": "".join(f"{b:02x}" for b in trace_id_bytes),
            "spanId": "".join(f"{b:02x}" for b in span_id_bytes),
            "parentSpanId": "",
            "name": f"POST /api/test/{span_index}",
            "kind": 2,  # SPAN_KIND_SERVER
            "startTimeUnixNano": str(now_nanos),
            "endTimeUnixNano": str(now_nanos + 50_000_000),  # +50ms
            "attributes": [
                # SoftProbe session identifier
                {
                    "key": "sp.session.id",
                    "value": {"stringValue": span_session_id}
                },
                {
                    "key": "test.span.index",
                    "value": {"intValue": str(span_index)}
                },
                # HTTP request metadata
                {
                    "key": "http.method",
                    "value": {"stringValue": "POST"}
                },
                {
                    "key": "http.url",
                    "value": {"stringValue": f"https://api.example.com/api/test/{span_index}"}
                },
                {
                    "key": "http.target",
                    "value": {"stringValue": f"/api/test/{span_index}"}
                },
                {
                    "key": "http.status_code",
                    "value": {"intValue": "200"}
                },
                # HTTP request headers
                {
                    "key": "http.request.header.content-type",
                    "value": {"stringValue": "application/json"}
                },
                {
                    "key": "http.request.header.user-agent",
                    "value": {"stringValue": "SoftProbe-Test-Client/1.0"}
                },
                {
                    "key": "http.request.header.authorization",
                    "value": {"stringValue": "Bearer test-token-abc123"}
                },
                {
                    "key": "http.request.header.x-request-id",
                    "value": {"stringValue": f"req-{span_index}"}
                },
                # HTTP response headers
                {
                    "key": "http.response.header.content-type",
                    "value": {"stringValue": "application/json"}
                },
                {
                    "key": "http.response.header.server",
                    "value": {"stringValue": "SoftProbe-API/1.0"}
                },
                {
                    "key": "http.response.header.x-response-time",
                    "value": {"stringValue": "45ms"}
                }
            ],
            "events": [
                # HTTP request event with body
                {
                    "timeUnixNano": str(now_nanos + 1_000_000),  # +1ms after start
                    "name": "sp.body",
                    "attributes": [
                        {
                            "key": "sp.body.type",
                            "value": {"stringValue": "request"}
                        },
                        {
                            "key": "sp.body.content",
                            "value": {"stringValue": request_body}
                        },
                        {
                            "key": "sp.body.size",
                            "value": {"intValue": str(len(request_body))}
                        }
                    ]
                },
                # HTTP response event with body
                {
                    "timeUnixNano": str(now_nanos + 45_000_000),  # +45ms (near end)
                    "name": "sp.body",
                    "attributes": [
                        {
                            "key": "sp.body.type",
                            "value": {"stringValue": "response"}
                        },
                        {
                            "key": "sp.body.content",
                            "value": {"stringValue": response_body}
                        },
                        {
                            "key": "sp.body.size",
                            "value": {"intValue": str(len(response_body))}
                        },
                        {
                            "key": "http.status_code",
                            "value": {"intValue": "200"}
                        }
                    ]
                }
            ],
            "status": {
                "code": 1  # STATUS_CODE_OK
            }
        }

    def _create_otlp_request(self, spans: List[Dict]) -> Dict:
        """Create OTLP request structure for given spans"""
        return {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "sp.app.id",
                                "value": {"stringValue": "test_deployment_app"}
                            },
                            {
                                "key": "sp.organization.id",
                                "value": {"stringValue": "org_deployment_test"}
                            },
                            {
                                "key": "sp.tenant.id",
                                "value": {"stringValue": "tenant_test"}
                            },
                            {
                                "key": "service.name",
                                "value": {"stringValue": "deployment_test_service"}
                            }
                        ]
                    },
                    "scopeSpans": [
                        {
                            "spans": spans
                        }
                    ]
                }
            ]
        }

    def _send_batch(self, batch_spans: List[Dict], batch_index: int) -> tuple[int, bool, Optional[str]]:
        """Send a single batch of spans with gzip compression"""
        otlp_request = self._create_otlp_request(batch_spans)
        try:
            # Serialize JSON and compress with gzip
            json_data = json.dumps(otlp_request).encode('utf-8')
            compressed_data = gzip.compress(json_data)
            
            response = requests.post(
                f"{self.endpoint}/v1/traces",
                headers={
                    "Content-Type": "application/json",
                    "Content-Encoding": "gzip"
                },
                data=compressed_data,
                timeout=60  # Increased timeout for large payloads
            )
            if response.status_code == 200:
                return (batch_index, True, None)
            else:
                return (batch_index, False, f"HTTP {response.status_code}: {response.text[:200]}")
        except requests.exceptions.Timeout:
            return (batch_index, False, "Request timeout after 60s")
        except Exception as e:
            return (batch_index, False, f"Exception: {str(e)[:200]}")

    def _generate_and_send_batch(self, batch_start: int, batch_size: int, batch_index: int) -> tuple[int, bool, Optional[str]]:
        """Generate spans on the fly for a batch and send it"""
        # Generate spans for this batch on the fly
        batch_spans = []
        for i in range(batch_start, min(batch_start + batch_size, self._total_span_count)):
            # Determine session ID for this span
            if self.session_count > 1:
                session_id = self.session_ids[i % self.session_count]
                span = self.create_otlp_span(i, session_id)
            else:
                span = self.create_otlp_span(i)
            batch_spans.append(span)
        
        # Send the batch
        return self._send_batch(batch_spans, batch_index)

    def _worker_thread(self, batch_indices: List[int], batch_size: int, worker_id: int) -> List[tuple[int, bool, Optional[str]]]:
        """Worker thread that processes multiple batches sequentially"""
        results = []
        total_batches = len(batch_indices)
        for idx, batch_idx in enumerate(batch_indices):
            if idx % 50 == 0 or idx == total_batches - 1:  # Log every 50 batches or last batch
                print(f"  Worker {worker_id}: Processing batch {batch_idx} ({idx + 1}/{total_batches})", flush=True)
            batch_start = batch_idx * batch_size
            result = self._generate_and_send_batch(batch_start, batch_size, batch_idx)
            results.append(result)
            if not result[1]:  # If failed
                print(f"  Worker {worker_id}: Batch {batch_idx} failed: {result[2]}", flush=True)
        return results

    def ingest_spans(self, span_count: int) -> bool:
        """Ingest test spans via OTLP endpoint with parallel batching for large counts"""
        print(f"{Colors.OKCYAN}[2/5] Ingesting {span_count} spans...{Colors.ENDC}")
        
        if self.session_count > 1:
            print(f"  Using {self.session_count} session IDs for stress testing")

        # Store total span count for worker threads
        self._total_span_count = span_count

        # Stress feature: if span_count > 5, split into batches and send in parallel
        if span_count > 20:
            batch_size = 20
            num_batches = (span_count + batch_size - 1) // batch_size  # Ceiling division
            num_workers = min(10, num_batches)  # Fixed number of worker threads
            
            print(f"  Using parallel stress mode: {num_batches} batches of up to {batch_size} spans each")
            print(f"  Using {num_workers} worker threads (generating spans on the fly to save memory)")
            5591491
            try:
                start_time = time.time()
                success_count = 0
                failed_batches = []
                
                # Distribute batches across worker threads
                batches_per_worker = (num_batches + num_workers - 1) // num_workers
                batch_assignments = []
                for worker_idx in range(num_workers):
                    worker_batches = []
                    for i in range(batches_per_worker):
                        batch_idx = worker_idx * batches_per_worker + i
                        if batch_idx < num_batches:
                            worker_batches.append(batch_idx)
                    if worker_batches:
                        batch_assignments.append(worker_batches)
                
                # Send batches in parallel using ThreadPoolExecutor with fixed number of workers
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    # Submit work to each worker thread
                    futures = [
                        executor.submit(self._worker_thread, batch_indices, batch_size, worker_id)
                        for worker_id, batch_indices in enumerate(batch_assignments)
                    ]
                    
                    # Collect results from all workers
                    for future in as_completed(futures):
                        worker_results = future.result()
                        for batch_idx, success, error in worker_results:
                            if success:
                                success_count += 1
                            else:
                                failed_batches.append((batch_idx, error))
                
                elapsed = time.time() - start_time
                
                if success_count == num_batches:
                    print(f"{Colors.OKGREEN}✓ Ingested {span_count} spans in {num_batches} parallel batches ({elapsed:.2f}s){Colors.ENDC}")
                    print(f"  All {num_batches} batches succeeded")
                    return True
                else:
                    print(f"{Colors.FAIL}✗ Ingestion partially failed: {success_count}/{num_batches} batches succeeded{Colors.ENDC}")
                    for batch_idx, error in failed_batches:
                        print(f"  Batch {batch_idx} failed: {error}")
                    return False
            except Exception as e:
                print(f"{Colors.FAIL}✗ Parallel ingestion failed: {e}{Colors.ENDC}")
                return False
        else:
            # Original behavior for span_count <= 5: single request
            # Generate spans on the fly even for small batches
            spans = []
            for i in range(span_count):
                if self.session_count > 1:
                    session_id = self.session_ids[i % self.session_count]
                    spans.append(self.create_otlp_span(i, session_id))
                else:
                    spans.append(self.create_otlp_span(i))
            
            otlp_request = self._create_otlp_request(spans)
            
            try:
                start_time = time.time()
                # Serialize JSON and compress with gzip
                json_data = json.dumps(otlp_request).encode('utf-8')
                compressed_data = gzip.compress(json_data)
                
                response = requests.post(
                    f"{self.endpoint}/v1/traces",
                    headers={
                        "Content-Type": "application/json",
                        "Content-Encoding": "gzip"
                    },
                    data=compressed_data,
                    timeout=30
                )
                elapsed = time.time() - start_time

                if response.status_code == 200:
                    data = response.json()
                    print(f"{Colors.OKGREEN}✓ Ingested {span_count} spans in {elapsed:.2f}s{Colors.ENDC}")
                    print(f"  Response: {data}")
                    return True
                else:
                    print(f"{Colors.FAIL}✗ Ingestion failed: HTTP {response.status_code}{Colors.ENDC}")
                    print(f"  Response: {response.text}")
                    return False
            except Exception as e:
                print(f"{Colors.FAIL}✗ Ingestion failed: {e}{Colors.ENDC}")
                return False

    def wait_for_flush(self, span_count: int):
        """Wait for buffer flush based on environment and span count"""
        buffer_spans = self.env_config["buffer_size_spans"]

        if span_count >= buffer_spans:
            # Spans exceed buffer size, flush should be immediate
            wait_time = 5  # Small buffer for async operations
            print(f"{Colors.OKCYAN}[3/5] Span count ({span_count}) >= buffer size ({buffer_spans}), waiting {wait_time}s for immediate flush...{Colors.ENDC}")
        else:
            # Need to wait for timeout-based flush
            wait_time = self.timeout + 10  # Buffer timeout + margin
            print(f"{Colors.OKCYAN}[3/5] Span count ({span_count}) < buffer size ({buffer_spans}), waiting {wait_time}s for timeout flush...{Colors.ENDC}")

        print(f"  Waiting {wait_time}s for buffer flush...")
        time.sleep(wait_time)
        print(f"{Colors.OKGREEN}✓ Wait complete{Colors.ENDC}")

    def query_session(self, max_retries: int = 5) -> Optional[Dict]:
        """Query spans by session ID"""
        print(f"{Colors.OKCYAN}[4/5] Querying session data...{Colors.ENDC}")

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(
                    f"{self.endpoint}/v1/query/session/{self.session_id}",
                    timeout=30
                )

                if response.status_code == 200:
                    data = response.json()
                    span_count = data.get("total_count", 0)
                    print(f"{Colors.OKGREEN}✓ Query successful (attempt {attempt}/{max_retries}): Found {span_count} spans{Colors.ENDC}")
                    return data
                elif response.status_code == 404:
                    print(f"{Colors.WARNING}⚠ Session not found yet (attempt {attempt}/{max_retries}), retrying...{Colors.ENDC}")
                    if attempt < max_retries:
                        time.sleep(5)
                        continue
                else:
                    print(f"{Colors.FAIL}✗ Query failed: HTTP {response.status_code}{Colors.ENDC}")
                    print(f"  Response: {response.text}")
                    return None
            except Exception as e:
                print(f"{Colors.FAIL}✗ Query failed (attempt {attempt}/{max_retries}): {e}{Colors.ENDC}")
                if attempt < max_retries:
                    time.sleep(5)
                    continue
                return None

        print(f"{Colors.FAIL}✗ Query failed after {max_retries} attempts{Colors.ENDC}")
        return None

    def validate_results(self, query_result: Dict, expected_count: int) -> bool:
        """Validate query results"""
        print(f"{Colors.OKCYAN}[5/5] Validating results...{Colors.ENDC}")

        actual_count = query_result.get("total_count", 0)
        spans = query_result.get("spans", [])

        # Check span count (allow small variance for multi-session scenarios)
        # When using multiple sessions, distribution might not be perfectly even
        count_diff = abs(actual_count - expected_count)
        if count_diff > 1:  # Allow 1 span difference for rounding
            print(f"{Colors.FAIL}✗ Span count mismatch: expected {expected_count} (±1), got {actual_count}{Colors.ENDC}")
            return False

        if len(spans) != actual_count:
            print(f"{Colors.FAIL}✗ Spans array length mismatch: expected {actual_count}, got {len(spans)}{Colors.ENDC}")
            return False

        # Validate session ID in results
        if query_result.get("session_id") != self.session_id:
            print(f"{Colors.FAIL}✗ Session ID mismatch{Colors.ENDC}")
            return False

        # Validate spans have required fields
        for i, span in enumerate(spans):
            required_fields = ["trace_id", "span_id", "app_id", "message_type", "timestamp"]
            for field in required_fields:
                if field not in span:
                    print(f"{Colors.FAIL}✗ Span {i} missing required field: {field}{Colors.ENDC}")
                    return False

        print(f"{Colors.OKGREEN}✓ Validation passed:{Colors.ENDC}")
        print(f"  - Span count: {actual_count} / {expected_count}")
        print(f"  - Session ID: {self.session_id}")
        print(f"  - All spans have required fields")
        return True

    def run_test(self, span_count: int = 10) -> bool:
        """Run complete deployment test"""
        session_info = f" across {self.session_count} sessions" if self.session_count > 1 else ""
        print(f"{Colors.BOLD}Starting deployment test with {span_count} spans{session_info}{Colors.ENDC}")
        print()

        # Step 1: Health check
        if not self.test_health():
            return False

        # Step 2: Ingest spans
        if not self.ingest_spans(span_count):
            return False

        # Step 3: Wait for flush
        self.wait_for_flush(span_count)

        # Step 4: Query session
        query_result = self.query_session()
        if not query_result:
            return False

        # Step 5: Validate results (only verify first session if multiple sessions)
        # Calculate expected span count for the first session
        if self.session_count > 1:
            # Spans are distributed evenly, so first session gets approximately span_count / session_count
            expected_count = (span_count + self.session_count - 1) // self.session_count  # Ceiling division
            print(f"  Note: Verifying only first session (expected ~{expected_count} spans)")
        else:
            expected_count = span_count
        
        if not self.validate_results(query_result, expected_count):
            return False

        print()
        print(f"{Colors.BOLD}{Colors.OKGREEN}{'=' * 60}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKGREEN}✓ ALL TESTS PASSED{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKGREEN}{'=' * 60}{Colors.ENDC}")
        print()
        print(f"Summary:")
        print(f"  Environment: {self.env_config['name']}")
        print(f"  Endpoint: {self.endpoint}")
        if self.session_count > 1:
            print(f"  Session IDs: {self.session_count} sessions (verified first: {self.session_id})")
            print(f"  Total spans ingested: {span_count}")
            print(f"  Spans retrieved (first session): {query_result.get('total_count', 0)}")
        else:
            print(f"  Session ID: {self.session_id}")
            print(f"  Spans ingested: {span_count}")
            print(f"  Spans retrieved: {span_count}")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Test SoftProbe OTLP backend deployment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--env",
        choices=["gcp", "local"],
        default="local",
        help="Target environment (default: local)"
    )
    parser.add_argument(
        "--endpoint",
        help="Override OTLP endpoint URL"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="Override buffer timeout (seconds)"
    )
    parser.add_argument(
        "--span-count",
        type=int,
        default=2,
        help="Number of spans to ingest (default: 10, use 10000+ for buffer flush test)"
    )
    parser.add_argument(
        "--session-count",
        type=int,
        default=1,
        help="Number of session IDs to use for stress testing (default: 1, spans will be distributed across sessions)"
    )

    args = parser.parse_args()

    # Create tester
    tester = OTLPDeploymentTester(args.env, args.endpoint, args.timeout, args.session_count)

    # Run test
    success = tester.run_test(args.span_count)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
