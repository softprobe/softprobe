# Legacy `verify_e2e.sh` (removed)

The old script assumed **Lakekeeper** on `:8181`, **Grafana**, service name **`otlp-backend`**, and paths under `warehouse/iceberg/default/`. The current stack is **DuckLake + MinIO** (see `docker-compose.yml` and `tests/config/test.yaml`).

**Replacement:** run automated checks that exercise ingest, flush, and SQL over `union_spans` / `union_logs`:

```bash
cd softprobe-runtime
make test-e2e
# or
cargo test --features integration-e2e --test tests integration::storage_contract_validation -- --test-threads=1
```
