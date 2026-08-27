# Compatibility conformance corpus

`tests/compat/manifests/cases.v0.yaml` is the data-driven case index for the
Prometheus, Loki, and Tempo compatibility lanes. The manifest remains the
source of case selection; protocol implementations and upstream code are not
copied into this corpus.

## Fixture provenance

The shared fixtures under `tests/compat/fixtures/` are repository-maintained
synthetic protocol envelopes. The manifest records the corpus source, license
statement, attribution policy, and the source commit used for this metadata.
Any upstream-derived Prometheus corpus material is separate, carries its own
attribution in `tests/compat/prometheus/promqltest/ATTRIBUTION.md`, and is not
copied into the signal-neutral fixtures.

When adding a fixture, record its provenance and license in the fixture corpus
documentation or the fixture's own attribution file. Do not copy upstream
implementation or AGPL-licensed code into the compatibility corpus.

## Unsupported-feature allowlist

The manifest's top-level `unsupported_feature_allowlist` is keyed by
`capability.id`. Each value is a list of feature names that are explicitly
unsupported for that capability and may be represented by the protocol's
documented unsupported-feature response. The existing conformance validator
consumes this mapping and rejects unknown capability keys or malformed entries.

The optional per-case `unsupported_feature_allowlist` entries add a normalized
diff path for an already-declared capability/feature pair. Those entries are
consumed by the current diff-approval path; the manifest metadata block records
their field and entry shape for review. The metadata block itself is review
metadata, not a second validator input.

An allowlist is not a general diff waiver: an unapproved normalized response
difference remains a product regression. Add a feature only when the capability
and protocol documentation identify it as unsupported.

## Reference pins

`docs/compat/references.v0.yaml` is the canonical pin file. The manifest
records the supported Prometheus, Loki, and Tempo image/tag set under
`metadata.reference_pins` for review visibility; changes must keep those values
aligned with the canonical pin file and each case's `reference` field.

## Tenant-isolation evidence

Each selected release case inherits the protocol entry in
`metadata.tenant_isolation`. The entry points to the shared authenticated tenant
helper at `tests/compat/support/auth.rs` and to the existing protocol contract
suite that exercises tenant-scoped behavior. The conformance validator rejects
missing helpers, missing protocol contract files, or commands that are not the
declared `cargo test --lib compat::<protocol>` suite. Reports and case
provenance retain this reference so release evidence can be traced to the
shared contract without copying its setup or assertions into the differential
harness.

## Drift review

Review compatibility drift as a data change with this sequence:

1. Update the case or fixture metadata and its provenance/attribution.
2. Run the filtered mock conformance command for affected cases.
3. Run the protocol differential target with the pinned reference image.
4. Run the reference-pin check and review raw plus normalized evidence.
5. Treat any normalized difference not covered by the documented unsupported
   feature allowlist as a product regression.

The manifest's `metadata.drift_review.required_checks` records this policy so
CI and reviewers can audit the expected checks without duplicating per-case
instructions.
