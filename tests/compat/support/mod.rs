//! Shared helpers for compatibility contract tests.

pub mod auth;
pub mod loki;
pub mod prometheus;
pub mod prometheus_oracle;
pub mod promqltest;
pub mod tempo;

pub mod conformance {
    use serde_json::{Map, Value};
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::{Path, PathBuf};

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct CompatCaseDescriptor {
        pub protocol: String,
        pub case_id: String,
        pub source_id: String,
        pub fixture_id: String,
        pub method: String,
        pub path: String,
        pub params: BTreeMap<String, String>,
        pub differential: bool,
    }

    impl CompatCaseDescriptor {
        #[allow(clippy::too_many_arguments)]
        pub fn new(
            protocol: &str,
            case_id: &str,
            source_id: &str,
            fixture_id: &str,
            path: &str,
            params: BTreeMap<String, String>,
            differential: bool,
        ) -> Self {
            Self {
                protocol: protocol.to_string(),
                case_id: case_id.to_string(),
                source_id: source_id.to_string(),
                fixture_id: fixture_id.to_string(),
                method: "GET".to_string(),
                path: path.to_string(),
                params,
                differential,
            }
        }
    }

    /// Load per-case canonical descriptors straight from the conformance
    /// manifest so runner receipts can carry manifest-static requests even
    /// when execution uses shifted fixtures. Returns (id, runner_id, descriptor).
    pub fn load_manifest_descriptors(
        path: &str,
    ) -> Result<Vec<(String, Option<String>, CompatCaseDescriptor)>, String> {
        let doc: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(path).map_err(|error| format!("read {path}: {error}"))?,
        )
        .map_err(|error| format!("parse {path}: {error}"))?;
        let mut out = Vec::new();
        for entry in doc
            .get("cases")
            .and_then(|cases| cases.as_array())
            .ok_or_else(|| format!("{path} has no cases array"))?
        {
            let id = entry
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("{path}: case without id"))?
                .to_string();
            let runner = entry
                .get("runner_case_id")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let endpoint = entry
                .get("endpoint")
                .ok_or_else(|| format!("{id}: no endpoint"))?;
            let method = endpoint
                .get("method")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("{id}: no method"))?
                .to_string();
            let path_field = endpoint
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| format!("{id}: no path"))?
                .to_string();
            let params: BTreeMap<String, String> = entry
                .pointer("/request/params")
                .and_then(|v| v.as_object())
                .map(|map| {
                    map.iter()
                        .filter_map(|(key, value)| {
                            value.as_str().map(|value| (key.clone(), value.to_string()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let fixture_id = entry
                .pointer("/fixture/id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let protocol = entry
                .get("protocol")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let descriptor = CompatCaseDescriptor::new(
                &protocol,
                &id,
                &id,
                &fixture_id,
                &path_field,
                params,
                true,
            );
            out.push((
                entry
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                runner,
                descriptor,
            ));
        }
        Ok(out)
    }

    pub fn parse_case_selection(
        protocol: &str,
        case_ids: Option<&str>,
        case_id: Option<&str>,
    ) -> Result<Option<BTreeSet<String>>, String> {
        let mut selected = BTreeSet::new();
        if let Some(case_ids) = case_ids.filter(|value| !value.trim().is_empty()) {
            for case_id in case_ids.split(',').map(str::trim) {
                if case_id.is_empty() {
                    return Err(format!(
                        "COMPAT_CASE_IDS contains an empty {protocol} case ID"
                    ));
                }
                if !selected.insert(case_id.to_string()) {
                    return Err(format!("duplicate selected {protocol} case ID: {case_id}"));
                }
            }
        }
        if let Some(case_id) = case_id
            .map(str::trim)
            .filter(|value| !value.is_empty() && *value != "__suite__")
        {
            if !selected.insert(case_id.to_string()) {
                return Err(format!("duplicate selected {protocol} case ID: {case_id}"));
            }
        }
        Ok((!selected.is_empty()).then_some(selected))
    }

    pub fn select_differential_cases<'a, T, F>(
        protocol: &str,
        cases: &'a [T],
        selection: Option<&BTreeSet<String>>,
        case_id: F,
    ) -> Result<Vec<&'a T>, String>
    where
        F: Fn(&T) -> Option<String>,
    {
        let available = cases
            .iter()
            .filter_map(|case| case_id(case))
            .collect::<Vec<_>>();
        let Some(selection) = selection else {
            return Ok(cases
                .iter()
                .filter(|case| case_id(case).is_some())
                .collect());
        };
        let available_set = available
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let missing = selection
            .iter()
            .filter(|id| !available_set.contains(id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "selected {protocol} cases were not differential fixture cases: {}",
                missing.join(", ")
            ));
        }
        let selected = cases
            .iter()
            .filter(|case| case_id(case).is_some_and(|id| selection.contains(&id)))
            .collect::<Vec<_>>();
        if selected.len() != selection.len() {
            return Err(format!(
                "selected {protocol} case IDs were not all resolved"
            ));
        }
        Ok(selected)
    }

    #[derive(Debug)]
    pub struct SelectedCase<'a, T> {
        pub case: &'a T,
        pub descriptor: CompatCaseDescriptor,
    }

    pub fn select_cases<'a, T, D, F>(
        protocol: &str,
        cases: &'a [T],
        selection: Option<&BTreeSet<String>>,
        is_differential: D,
        descriptor_for_case: F,
    ) -> Result<Vec<SelectedCase<'a, T>>, String>
    where
        D: Fn(&T) -> bool,
        F: Fn(&T) -> Result<CompatCaseDescriptor, String>,
    {
        let described = cases
            .iter()
            .filter(|case| is_differential(case))
            .map(|case| {
                descriptor_for_case(case).map(|descriptor| SelectedCase { case, descriptor })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let differential = described.iter().collect::<Vec<_>>();
        let Some(selection) = selection else {
            return Ok(differential
                .into_iter()
                .map(|selected| SelectedCase {
                    case: selected.case,
                    descriptor: selected.descriptor.clone(),
                })
                .collect());
        };
        let available = differential
            .iter()
            .flat_map(|selected| {
                [
                    selected.descriptor.case_id.as_str(),
                    selected.descriptor.source_id.as_str(),
                ]
            })
            .collect::<BTreeSet<_>>();
        let missing = selection
            .iter()
            .filter(|id| !available.contains(id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(format!(
                "selected {protocol} cases were not differential fixture cases: {}",
                missing.join(", ")
            ));
        }
        let selected = differential
            .into_iter()
            .filter(|selected| {
                selection.contains(&selected.descriptor.case_id)
                    || selection.contains(&selected.descriptor.source_id)
            })
            .map(|selected| SelectedCase {
                case: selected.case,
                descriptor: selected.descriptor.clone(),
            })
            .collect::<Vec<_>>();
        if selected.len() != selection.len() {
            return Err(format!(
                "selected {protocol} case IDs were not all resolved"
            ));
        }
        Ok(selected)
    }

    fn canonicalize(value: &Value) -> Value {
        match value {
            Value::Object(object) => {
                let mut sorted = Map::new();
                let mut keys = object.keys().collect::<Vec<_>>();
                keys.sort();
                for key in keys {
                    sorted.insert(key.clone(), canonicalize(&object[key]));
                }
                Value::Object(sorted)
            }
            Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
            value => value.clone(),
        }
    }

    pub fn canonical_request(method: &str, path: &str, params: &BTreeMap<String, String>) -> Value {
        canonicalize(&serde_json::json!({
            "method": method,
            "path": path,
            "params": params,
        }))
    }

    pub fn canonical_request_text(request: &Value) -> String {
        serde_json::to_string(&canonicalize(request)).expect("canonical request JSON")
    }

    pub fn sha256_hex(input: &[u8]) -> String {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut message = input.to_vec();
        let bit_len = (message.len() as u64) * 8;
        message.push(0x80);
        while message.len() % 64 != 56 {
            message.push(0);
        }
        message.extend_from_slice(&bit_len.to_be_bytes());
        let mut h = [
            0x6a09e667u32,
            0xbb67ae85,
            0x3c6ef372,
            0xa54ff53a,
            0x510e527f,
            0x9b05688c,
            0x1f83d9ab,
            0x5be0cd19,
        ];
        for chunk in message.chunks_exact(64) {
            let mut w = [0u32; 64];
            for (index, bytes) in chunk.chunks_exact(4).take(16).enumerate() {
                w[index] = u32::from_be_bytes(bytes.try_into().unwrap());
            }
            for index in 16..64 {
                let s0 = w[index - 15].rotate_right(7)
                    ^ w[index - 15].rotate_right(18)
                    ^ (w[index - 15] >> 3);
                let s1 = w[index - 2].rotate_right(17)
                    ^ w[index - 2].rotate_right(19)
                    ^ (w[index - 2] >> 10);
                w[index] = w[index - 16]
                    .wrapping_add(s0)
                    .wrapping_add(w[index - 7])
                    .wrapping_add(s1);
            }
            let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) =
                (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
            for index in 0..64 {
                let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
                let ch = (e & f) ^ ((!e) & g);
                let temp1 = hh
                    .wrapping_add(s1)
                    .wrapping_add(ch)
                    .wrapping_add(K[index])
                    .wrapping_add(w[index]);
                let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
                let maj = (a & b) ^ (a & c) ^ (b & c);
                let temp2 = s0.wrapping_add(maj);
                (hh, g, f, e, d, c, b, a) = (
                    g,
                    f,
                    e,
                    d.wrapping_add(temp1),
                    c,
                    b,
                    a,
                    temp1.wrapping_add(temp2),
                );
            }
            for (index, value) in [a, b, c, d, e, f, g, hh].into_iter().enumerate() {
                h[index] = h[index].wrapping_add(value);
            }
        }
        h.iter().map(|word| format!("{word:08x}")).collect()
    }

    pub struct CompatExecutionRecorder {
        path: PathBuf,
        protocol: String,
        run_id: String,
        selected: Vec<CompatCaseDescriptor>,
        executed: BTreeSet<String>,
        cases: Vec<Value>,
    }

    impl CompatExecutionRecorder {
        pub fn new(
            protocol: &str,
            selected: &[CompatCaseDescriptor],
            run_id: Option<&str>,
        ) -> std::io::Result<Self> {
            let root = std::env::var_os("SOFTPROBE_COMPAT_ARTIFACT_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("target/compat-artifacts"));
            Self::new_at(protocol, selected, run_id, &root)
        }

        pub fn new_at(
            protocol: &str,
            selected: &[CompatCaseDescriptor],
            run_id: Option<&str>,
            root: &Path,
        ) -> std::io::Result<Self> {
            std::fs::create_dir_all(root)?;
            let run_id = run_id
                .map(str::to_string)
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    std::env::var("RUN_ID")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                })
                .or_else(|| {
                    std::env::var("COMPAT_RUN_ID")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                })
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
            let cases = selected
                .iter()
                .map(|descriptor| {
                    let request =
                        canonical_request(&descriptor.method, &descriptor.path, &descriptor.params);
                    let text = canonical_request_text(&request);
                    let fingerprint = sha256_hex(text.as_bytes());
                    serde_json::json!({
                        "case_id": descriptor.case_id,
                        "source_id": descriptor.source_id,
                        "fixture_id": descriptor.fixture_id,
                        "endpoint": {"method": descriptor.method, "path": descriptor.path},
                        "canonical_request": request,
                        "canonical_text": text,
                        "canonical_request_json": text,
                        "fingerprint": fingerprint,
                        "request_fingerprint": fingerprint,
                        "fingerprints": {"canonical_request": fingerprint},
                        "fingerprint_algorithm": "SHA-256",
                        "status": "selected",
                        "reason": "not_attempted",
                    })
                })
                .collect();
            Ok(Self {
                path: root.join("execution-receipt.json"),
                protocol: protocol.to_string(),
                run_id,
                selected: selected.to_vec(),
                executed: BTreeSet::new(),
                cases,
            })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn record_case(
            &mut self,
            descriptor: &CompatCaseDescriptor,
            status: &str,
            reason: &str,
        ) -> std::io::Result<()> {
            if !self
                .selected
                .iter()
                .any(|selected| selected.case_id == descriptor.case_id)
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("receipt case was not selected: {}", descriptor.case_id),
                ));
            }
            if !self.executed.insert(descriptor.case_id.clone()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("receipt case was recorded twice: {}", descriptor.case_id),
                ));
            }
            if let Some(case) = self
                .cases
                .iter_mut()
                .find(|case| case["case_id"] == descriptor.case_id)
            {
                case["status"] = Value::String(status.to_string());
                case["outcome"] = Value::String(status.to_string());
                case["reason"] = Value::String(reason.to_string());
            }
            self.write("running", "in_progress", "")
        }

        pub fn finish(&self, status: &str, reason: &str) -> Result<(), String> {
            self.write(status, status, reason)
                .map_err(|error| error.to_string())?;
            validate_execution_receipt_for_run(&self.path, &self.protocol, status, &self.run_id)?;
            Ok(())
        }

        fn write(&self, status: &str, outcome: &str, reason: &str) -> std::io::Result<()> {
            let selected_case_ids = self
                .selected
                .iter()
                .map(|descriptor| descriptor.case_id.clone())
                .collect::<Vec<_>>();
            let selected_fixture_ids = self
                .selected
                .iter()
                .map(|descriptor| descriptor.fixture_id.clone())
                .collect::<Vec<_>>();
            let executed_case_ids = self
                .selected
                .iter()
                .filter(|descriptor| self.executed.contains(&descriptor.case_id))
                .map(|descriptor| descriptor.case_id.clone())
                .collect::<Vec<_>>();
            let executed_fixture_ids = self
                .selected
                .iter()
                .filter(|descriptor| self.executed.contains(&descriptor.case_id))
                .map(|descriptor| descriptor.fixture_id.clone())
                .collect::<Vec<_>>();
            let receipt = serde_json::json!({
                "run_id": self.run_id,
                "protocol": self.protocol,
                "selected_case_ids": selected_case_ids,
                "executed_case_ids": executed_case_ids,
                "selected_fixture_ids": selected_fixture_ids,
                "executed_fixture_ids": executed_fixture_ids,
                "status": status,
                "outcome": outcome,
                "reason": reason,
                "cases": self.cases,
            });
            std::fs::write(&self.path, serde_json::to_vec_pretty(&receipt).unwrap())
        }

        pub fn read(path: &Path) -> Result<Value, String> {
            serde_json::from_slice(
                &std::fs::read(path).map_err(|error| format!("read {path:?}: {error}"))?,
            )
            .map_err(|error| format!("parse {path:?}: {error}"))
        }
    }

    pub fn validate_execution_receipt(
        path: &Path,
        protocol: &str,
        expected_status: &str,
    ) -> Result<(), String> {
        validate_execution_receipt_for_run(path, protocol, expected_status, "")
    }

    pub fn validate_execution_receipt_for_run(
        path: &Path,
        protocol: &str,
        expected_status: &str,
        expected_run_id: &str,
    ) -> Result<(), String> {
        let receipt = CompatExecutionRecorder::read(path)?;
        let run_id = receipt["run_id"]
            .as_str()
            .filter(|run_id| !run_id.trim().is_empty())
            .ok_or_else(|| format!("missing run ID at {path:?}"))?;
        if !expected_run_id.is_empty() && run_id != expected_run_id {
            return Err(format!("receipt run ID mismatch at {path:?}: {receipt}"));
        }
        if receipt["protocol"] != protocol || receipt["status"] != expected_status {
            return Err(format!("invalid execution receipt at {path:?}: {receipt}"));
        }
        let selected = receipt["selected_case_ids"]
            .as_array()
            .ok_or_else(|| format!("missing selected case IDs at {path:?}"))?;
        let executed = receipt["executed_case_ids"]
            .as_array()
            .ok_or_else(|| format!("missing executed case IDs at {path:?}"))?;
        let selected_fixtures = receipt["selected_fixture_ids"]
            .as_array()
            .ok_or_else(|| format!("missing selected fixture IDs at {path:?}"))?;
        let executed_fixtures = receipt["executed_fixture_ids"]
            .as_array()
            .ok_or_else(|| format!("missing executed fixture IDs at {path:?}"))?;
        if selected.len() != selected_fixtures.len() {
            return Err(format!("selected case/fixture IDs differ at {path:?}"));
        }
        if selected
            .iter()
            .filter_map(Value::as_str)
            .collect::<BTreeSet<_>>()
            .len()
            != selected.len()
        {
            return Err(format!("duplicate selected case IDs at {path:?}"));
        }
        if !executed.iter().all(|case_id| selected.contains(case_id)) {
            return Err(format!("executed IDs are not selected IDs at {path:?}"));
        }
        let expected_executed_fixtures = executed
            .iter()
            .map(|case_id| {
                selected
                    .iter()
                    .position(|selected_id| selected_id == case_id)
                    .and_then(|index| selected_fixtures.get(index))
                    .cloned()
                    .ok_or_else(|| format!("missing fixture for executed case at {path:?}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if *executed_fixtures != expected_executed_fixtures {
            return Err(format!("executed case/fixture IDs differ at {path:?}"));
        }
        if expected_status == "pass" && selected != executed {
            return Err(format!(
                "successful receipt did not execute every selected case at {path:?}"
            ));
        }
        let cases = receipt["cases"]
            .as_array()
            .ok_or_else(|| format!("missing receipt cases at {path:?}"))?;
        if cases.len() != selected.len() {
            return Err(format!("receipt case count mismatch at {path:?}"));
        }
        for (index, case) in cases.iter().enumerate() {
            if case["case_id"] != selected[index] || case["fixture_id"] != selected_fixtures[index]
            {
                return Err(format!(
                    "receipt case provenance mismatch at {path:?}: {case}"
                ));
            }
            let request = &case["canonical_request"];
            let text = canonical_request_text(request);
            let expected = sha256_hex(text.as_bytes());
            if case["canonical_text"] != text
                || case["canonical_request_json"] != text
                || case["fingerprint"] != expected
                || case["request_fingerprint"] != expected
                || case["fingerprints"]["canonical_request"] != expected
                || case["fingerprint_algorithm"] != "SHA-256"
            {
                return Err(format!("invalid request fingerprint at {path:?}: {case}"));
            }
        }
        Ok(())
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct ManifestCase {
        pub id: String,
        #[serde(default)]
        pub runner_case_id: Option<String>,
        pub protocol: String,
        pub endpoint: ManifestEndpoint,
        pub request: ManifestRequest,
        pub fixture: ManifestFixture,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct ManifestEndpoint {
        pub method: String,
        pub path: String,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct ManifestRequest {
        #[serde(default)]
        pub params: BTreeMap<String, serde_yaml::Value>,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    pub struct ManifestFixture {
        pub id: String,
    }

    #[derive(Debug, serde::Deserialize)]
    struct Manifest {
        cases: Vec<ManifestCase>,
    }

    fn load_manifest() -> Result<Manifest, String> {
        serde_yaml::from_str(include_str!(
            "../../../tests/compat/manifests/cases.v0.yaml"
        ))
        .map_err(|error| format!("parse compatibility manifest: {error}"))
    }

    fn selection_is_requested() -> bool {
        ["COMPAT_CASE_ID", "COMPAT_CASE_IDS"].iter().any(|name| {
            std::env::var(name)
                .ok()
                .is_some_and(|value| !value.trim().is_empty())
        })
    }

    fn selection_contains(case_id: &str) -> bool {
        ["COMPAT_CASE_ID", "COMPAT_CASE_IDS"].iter().any(|name| {
            std::env::var(name).ok().is_some_and(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .any(|selected| selected == case_id)
            })
        })
    }

    fn descriptor_from_manifest_case(
        case: &ManifestCase,
        source_case_id: &str,
        path: &str,
        params: BTreeMap<String, String>,
        differential: bool,
    ) -> CompatCaseDescriptor {
        CompatCaseDescriptor::new(
            &case.protocol,
            &case.id,
            source_case_id,
            &case.fixture.id,
            path,
            params,
            differential,
        )
    }

    pub fn manifest_case_for(
        protocol: &str,
        method: &str,
        path: &str,
        params: &BTreeMap<String, String>,
    ) -> Result<ManifestCase, String> {
        let manifest = load_manifest()?;
        let matches = manifest
            .cases
            .into_iter()
            .filter(|case| {
                case.protocol == protocol
                    && case.endpoint.method == method
                    && case.endpoint.path == path
                    && manifest_params(&case.request) == *params
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [case] => Ok(case.clone()),
            [] => Err(format!(
                "no manifest case for {protocol} {method} {path} with params {params:?}"
            )),
            _ => Err(format!(
                "multiple manifest cases for {protocol} {method} {path} with params {params:?}"
            )),
        }
    }

    pub fn descriptor_for_request(
        protocol: &str,
        source_case_id: &str,
        method: &str,
        path: &str,
        params: BTreeMap<String, String>,
        differential: bool,
    ) -> Result<CompatCaseDescriptor, String> {
        let manifest = manifest_case_for(protocol, method, path, &params)?;
        Ok(descriptor_from_manifest_case(
            &manifest,
            source_case_id,
            path,
            params,
            differential,
        ))
    }

    pub fn descriptor_for_case(
        protocol: &str,
        source_case_id: &str,
        method: &str,
        path: &str,
        params: BTreeMap<String, String>,
        differential: bool,
    ) -> Result<CompatCaseDescriptor, String> {
        let manifest = load_manifest()?;
        let source_matches = manifest
            .cases
            .iter()
            .filter(|case| {
                case.protocol == protocol
                    && (case.id == source_case_id
                        || case.runner_case_id.as_deref() == Some(source_case_id))
                    && case.endpoint.method == method
                    && case.endpoint.path == path
            })
            .collect::<Vec<_>>();

        if selection_is_requested() && selection_contains(source_case_id) {
            return match source_matches.as_slice() {
                [case] => Ok(descriptor_from_manifest_case(
                    case,
                    source_case_id,
                    path,
                    params,
                    differential,
                )),
                [] => Err(format!(
                    "no selected manifest case for {protocol} source case {source_case_id}"
                )),
                _ => Err(format!(
                    "multiple selected manifest cases for {protocol} source case {source_case_id}"
                )),
            };
        }

        let request_matches = manifest
            .cases
            .iter()
            .filter(|case| {
                case.protocol == protocol
                    && case.endpoint.method == method
                    && case.endpoint.path == path
                    && manifest_params(&case.request) == params
            })
            .collect::<Vec<_>>();
        match request_matches.as_slice() {
            [case] => Ok(descriptor_from_manifest_case(
                case,
                source_case_id,
                path,
                params,
                differential,
            )),
            [_, ..] => Err(format!(
                "multiple manifest cases for {protocol} {method} {path} with params {params:?}"
            )),
            [] => match source_matches.as_slice() {
                [case] => Ok(descriptor_from_manifest_case(
                    case,
                    source_case_id,
                    path,
                    params,
                    differential,
                )),
                [] => Ok(CompatCaseDescriptor::new(
                    protocol,
                    source_case_id,
                    source_case_id,
                    &format!("{protocol}:{source_case_id}"),
                    path,
                    params,
                    differential,
                )),
                _ => Err(format!(
                    "multiple manifest cases for {protocol} source case {source_case_id}"
                )),
            },
        }
    }

    fn manifest_params(request: &ManifestRequest) -> BTreeMap<String, String> {
        request
            .params
            .iter()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| serde_json::to_string(value).expect("manifest value"));
                (key.clone(), value)
            })
            .collect()
    }
}

pub mod lifecycle {
    use serde::Serialize;
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use crate::compat_support::prometheus::encode_query_owned;

    const POLL_INTERVAL: Duration = Duration::from_millis(250);

    pub struct ContainerGuard {
        name: String,
    }

    impl ContainerGuard {
        pub fn name(&self) -> &str {
            &self.name
        }
    }

    impl Drop for ContainerGuard {
        fn drop(&mut self) {
            let _ = Command::new("docker")
                .args(["rm", "-f", &self.name])
                .status();
        }
    }

    #[allow(dead_code)]
    pub struct ReferenceService {
        guard: ContainerGuard,
        ports: BTreeMap<String, String>,
        pub base: String,
    }

    #[allow(dead_code)]
    impl ReferenceService {
        pub fn port(&self, container_port: &str) -> &str {
            self.ports
                .get(container_port)
                .map(String::as_str)
                .unwrap_or_else(|| panic!("reference service did not expose {container_port}"))
        }

        pub fn wait_queryable(&self, url: &str, timeout: Duration) {
            wait_queryable(url, timeout);
        }
    }

    pub fn container_name(prefix: &str) -> String {
        format!(
            "{prefix}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        )
    }

    fn run_container(name: &str, args: &[String]) -> ContainerGuard {
        let _ = Command::new("docker").args(["rm", "-f", name]).status();
        // Keep the container after exit (--rm would discard logs we need
        // when readiness fails); ContainerGuard removes it explicitly.
        let run = Command::new("docker")
            .args(["run", "-d", "--name", name])
            .args(args)
            .status()
            .unwrap_or_else(|error| panic!("docker run {name}: {error}"));
        assert!(run.success(), "failed to start oracle container {name}");
        // Expose for readiness-timeout diagnostics.
        std::env::set_var("COMPAT_ORACLE_CONTAINER", &name);
        ContainerGuard {
            name: name.to_string(),
        }
    }

    /// Dump `docker logs` for a reference container (used on readiness
    /// timeouts so CI shows why the oracle never came up).
    pub fn print_container_logs(name: &str) {
        let _ = Command::new("docker")
            .args(["logs", "--tail", "80", name])
            .status();
    }

    fn docker_port(name: &str, container_port: &str) -> String {
        let output = Command::new("docker")
            .args(["port", name, container_port])
            .output()
            .unwrap_or_else(|error| panic!("docker port {name}:{container_port}: {error}"));
        assert!(
            output.status.success(),
            "docker port failed for {name}:{container_port}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .and_then(|line| line.rsplit(':').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| panic!("unexpected docker port output: {output:?}"))
    }

    pub fn require_docker(message: &str) {
        let ok = Command::new("docker")
            .args(["info"])
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false);
        assert!(ok, "{message}");
    }

    pub fn start_reference_service(
        prefix: &str,
        image: &str,
        options: &[String],
        command: &[String],
        http_container_port: &str,
        extra_container_ports: &[&str],
        readiness_path: &str,
        readiness_timeout: Duration,
        docker_message: &str,
    ) -> ReferenceService {
        require_docker(docker_message);
        let name = container_name(prefix);
        let mut args = options.to_vec();
        args.push(image.to_string());
        args.extend(command.iter().cloned());
        let guard = run_container(&name, &args);
        let mut ports = BTreeMap::new();
        ports.insert(
            http_container_port.to_string(),
            docker_port(guard.name(), http_container_port),
        );
        for container_port in extra_container_ports {
            ports.insert(
                (*container_port).to_string(),
                docker_port(guard.name(), container_port),
            );
        }
        let base = format!("http://127.0.0.1:{}", ports[http_container_port]);
        wait_http_ok(&format!("{base}{readiness_path}"), readiness_timeout);
        ReferenceService { guard, ports, base }
    }

    pub fn wait_http_ok(url: &str, timeout: Duration) {
        wait_http_status(url, timeout, |status| (200..300).contains(&status));
    }

    pub fn wait_queryable(url: &str, timeout: Duration) {
        wait_http_status(url, timeout, |status| status == 200);
    }

    fn wait_http_status(url: &str, timeout: Duration, accepts: impl Fn(u16) -> bool) {
        wait_http_status_with_probe(url, timeout, POLL_INTERVAL, accepts, curl_probe)
            .unwrap_or_else(|error| {
                // Best-effort: surface oracle container logs on readiness
                // timeouts so CI can show the crash reason.
                for arg in std::env::args().collect::<Vec<_>>() {
                    let _ = arg; // no-op; container name passed via env below
                }
                if let Ok(name) = std::env::var("COMPAT_ORACLE_CONTAINER") {
                    print_container_logs(&name);
                }
                panic!("{error}")
            });
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct PollObservation {
        status: Option<u16>,
        detail: String,
    }

    fn curl_probe(url: &str) -> PollObservation {
        let probe = Command::new("curl")
            .args(["-sS", "-o", "/dev/null", "-w", "%{http_code}", url])
            .output();
        match probe {
            Ok(output) => {
                let status = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .parse::<u16>()
                    .ok();
                let detail = match status {
                    Some(status) => format!(
                        "status={status}, stderr={}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ),
                    None => format!(
                        "invalid status, stdout={}, stderr={}",
                        String::from_utf8_lossy(&output.stdout).trim(),
                        String::from_utf8_lossy(&output.stderr).trim()
                    ),
                };
                PollObservation { status, detail }
            }
            Err(error) => PollObservation {
                status: None,
                detail: format!("probe error={error}"),
            },
        }
    }

    fn wait_http_status_with_probe(
        url: &str,
        timeout: Duration,
        poll_interval: Duration,
        accepts: impl Fn(u16) -> bool,
        mut probe: impl FnMut(&str) -> PollObservation,
    ) -> Result<(), String> {
        let start = Instant::now();
        loop {
            let observation = probe(url);
            let last_observed = observation.detail;
            if observation.status.is_some_and(|status| accepts(status)) {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(format!(
                    "timeout waiting for {url} after {timeout:?}; last observed {}",
                    last_observed
                ));
            }
            std::thread::sleep(poll_interval);
        }
    }

    pub fn write_failure_artifacts(
        protocol: &str,
        case_id: &str,
        path: &str,
        params: &BTreeMap<String, String>,
        lake_raw: Option<&Value>,
        oracle_raw: Option<&Value>,
        normalize: fn(Value) -> Value,
        raw_env: &str,
        normalized_env: &str,
    ) -> std::io::Result<PathBuf> {
        for (kind, component) in [("protocol", protocol), ("case_id", case_id)] {
            if !valid_component(component) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid {kind} artifact path component: {component:?}"),
                ));
            }
        }
        let root = std::env::var_os("SOFTPROBE_COMPAT_ARTIFACT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("target/compat-artifacts"));
        let dir = root.join(protocol).join(case_id);
        std::fs::create_dir_all(&dir)?;
        let request = serde_json::json!({"path": path, "params": params});
        write_json(&dir.join("request.raw.json"), &request)?;
        let query_params = params
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        let normalized_request =
            serde_json::json!({"path": path, "query": encode_query_owned(&query_params)});
        write_json(&dir.join("request.normalized.json"), &normalized_request)?;
        write_json(&dir.join("lake.raw.json"), &lake_raw)?;
        write_json(&dir.join("oracle.raw.json"), &oracle_raw)?;
        if let Some(body) = lake_raw {
            write_json(&dir.join("lake.normalized.json"), &normalize(body.clone()))?;
        }
        if let Some(body) = oracle_raw {
            write_json(
                &dir.join("oracle.normalized.json"),
                &normalize(body.clone()),
            )?;
        }

        if let Some(path) = std::env::var_os(raw_env) {
            write_json(
                Path::new(&path),
                &serde_json::json!({
                    "case_id": case_id,
                    "request": request,
                    "lake": lake_raw,
                    "oracle": oracle_raw,
                }),
            )?;
        }
        if let Some(path) = std::env::var_os(normalized_env) {
            write_json(
                Path::new(&path),
                &serde_json::json!({
                    "case_id": case_id,
                    "request": normalized_request,
                    "lake": lake_raw.map(|body| normalize(body.clone())),
                    "oracle": oracle_raw.map(|body| normalize(body.clone())),
                }),
            )?;
        }
        Ok(dir)
    }

    fn write_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(
            path,
            serde_json::to_vec_pretty(value).expect("artifact json"),
        )
    }

    fn valid_component(value: &str) -> bool {
        !value.is_empty()
            && value != "."
            && value != ".."
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::cell::RefCell;

        fn normalize(mut value: Value) -> Value {
            value["normalized"] = Value::Bool(true);
            value
        }

        #[test]
        fn wait_http_accepts_only_success_statuses() {
            let statuses = RefCell::new(vec![
                PollObservation {
                    status: Some(404),
                    detail: "status=404".into(),
                },
                PollObservation {
                    status: Some(204),
                    detail: "status=204".into(),
                },
            ]);
            let result = wait_http_status_with_probe(
                "test://readiness",
                Duration::from_millis(10),
                Duration::ZERO,
                |status| (200..300).contains(&status),
                |_| statuses.borrow_mut().remove(0),
            );
            assert_eq!(result, Ok(()));
        }

        #[test]
        fn wait_http_timeout_includes_last_observed_context() {
            let result = wait_http_status_with_probe(
                "test://query",
                Duration::ZERO,
                Duration::ZERO,
                |status| status == 200,
                |_| PollObservation {
                    status: Some(503),
                    detail: "status=503, body=warming".into(),
                },
            );
            let error = result.expect_err("timeout");
            assert!(error.contains("test://query"));
            assert!(error.contains("status=503, body=warming"));
        }

        #[test]
        fn artifact_paths_and_normalizer_are_shared_and_parameterized() {
            let root = tempfile::tempdir().expect("artifact root");
            let raw_path = root.path().join("raw.json");
            let normalized_path = root.path().join("normalized.json");
            let raw_env = "SOFTPROBE_TESTKIT_RAW_ARTIFACT";
            let normalized_env = "SOFTPROBE_TESTKIT_NORMALIZED_ARTIFACT";
            std::env::set_var(raw_env, &raw_path);
            std::env::set_var(normalized_env, &normalized_path);
            let previous_root = std::env::var_os("SOFTPROBE_COMPAT_ARTIFACT_DIR");
            std::env::set_var("SOFTPROBE_COMPAT_ARTIFACT_DIR", root.path());

            let mut params = BTreeMap::new();
            params.insert("query".into(), "{service=\"api\"}".into());
            let dir = write_failure_artifacts(
                "tempo",
                "stable-case-1",
                "/api/search",
                &params,
                Some(&serde_json::json!({"value": 1})),
                Some(&serde_json::json!({"value": 2})),
                normalize,
                raw_env,
                normalized_env,
            )
            .expect("artifacts");

            assert_eq!(dir, root.path().join("tempo/stable-case-1"));
            for file in [
                "request.raw.json",
                "request.normalized.json",
                "lake.raw.json",
                "oracle.raw.json",
                "lake.normalized.json",
                "oracle.normalized.json",
            ] {
                assert!(dir.join(file).is_file(), "missing {file}");
            }
            assert!(raw_path.is_file());
            assert!(normalized_path.is_file());
            let normalized: Value = serde_json::from_slice(
                &std::fs::read(&normalized_path).expect("normalized artifact"),
            )
            .expect("normalized json");
            assert_eq!(normalized["lake"]["normalized"], true);

            std::env::remove_var(raw_env);
            std::env::remove_var(normalized_env);
            match previous_root {
                Some(value) => std::env::set_var("SOFTPROBE_COMPAT_ARTIFACT_DIR", value),
                None => std::env::remove_var("SOFTPROBE_COMPAT_ARTIFACT_DIR"),
            }
        }

        #[test]
        fn artifact_components_reject_path_traversal() {
            assert!(valid_component("tempo"));
            assert!(valid_component("stable-case_1.2"));
            assert!(!valid_component("../escape"));
            assert!(!valid_component(""));
            assert!(!valid_component("case/name"));
        }

        #[test]
        fn artifact_writer_rejects_path_traversal() {
            let params = BTreeMap::new();
            let error = write_failure_artifacts(
                "tempo",
                "../escape",
                "/api/search",
                &params,
                None,
                None,
                normalize,
                "SOFTPROBE_TESTKIT_UNUSED_RAW",
                "SOFTPROBE_TESTKIT_UNUSED_NORMALIZED",
            )
            .expect_err("path traversal");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
}

use softprobe_runtime::compat::capability::{
    parse_capability_yaml, CapabilityManifest, EMBEDDED_CAPABILITY_V0,
};
use softprobe_runtime::compat::stubs::declared_compat_probe_paths;

pub fn load_embedded_capability() -> CapabilityManifest {
    parse_capability_yaml(EMBEDDED_CAPABILITY_V0).expect("embedded capability.v0")
}

pub fn all_compat_probe_paths() -> &'static [(&'static str, &'static str)] {
    declared_compat_probe_paths()
}

#[test]
fn support_helpers_load_manifest_and_probe_paths() {
    let m = load_embedded_capability();
    assert_eq!(m.version, "compat.v0");
    assert!(!all_compat_probe_paths().is_empty());
}

#[cfg(test)]
mod selector_tests {
    use super::conformance::{
        parse_case_selection, select_cases, select_differential_cases, validate_execution_receipt,
        CompatCaseDescriptor, CompatExecutionRecorder,
    };
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Debug)]
    struct Case {
        id: &'static str,
        differential: bool,
    }

    #[test]
    fn shared_selector_parser_combines_ids_and_ignores_suite_sentinel() {
        let selected = parse_case_selection("shared", Some("case-a, case-b"), Some("__suite__"))
            .expect("selector");

        assert_eq!(
            selected,
            Some(BTreeSet::from(["case-a".into(), "case-b".into()]))
        );
    }

    #[test]
    fn shared_selector_rejects_unknown_and_non_differential_cases() {
        let cases = [
            Case {
                id: "supported",
                differential: true,
            },
            Case {
                id: "contract-only",
                differential: false,
            },
        ];

        for case_id in ["missing", "contract-only"] {
            let selection = BTreeSet::from([case_id.to_string()]);
            let error = select_differential_cases("shared", &cases, Some(&selection), |case| {
                case.differential.then_some(case.id.to_string())
            })
            .expect_err("invalid selection");
            assert!(
                error.contains(case_id),
                "error should name {case_id}: {error}"
            );
        }
    }

    #[test]
    fn shared_selector_preserves_unfiltered_differential_cases() {
        let cases = [
            Case {
                id: "first",
                differential: true,
            },
            Case {
                id: "contract-only",
                differential: false,
            },
            Case {
                id: "second",
                differential: true,
            },
        ];

        let selected = select_differential_cases("shared", &cases, None, |case| {
            case.differential.then_some(case.id.to_string())
        })
        .expect("all differential cases");
        assert_eq!(
            selected.iter().map(|case| case.id).collect::<Vec<_>>(),
            vec!["first", "second"]
        );
    }

    #[test]
    fn shared_manifest_selector_skips_non_differential_cases_before_building_descriptors() {
        let cases = [
            Case {
                id: "supported",
                differential: true,
            },
            Case {
                id: "contract-only",
                differential: false,
            },
        ];
        let build_descriptor = |case: &Case| {
            if !case.differential {
                return Err(format!("descriptor should not be built for {}", case.id));
            }
            Ok(CompatCaseDescriptor::new(
                "shared",
                case.id,
                case.id,
                "fixture-a",
                "/supported",
                BTreeMap::new(),
                true,
            ))
        };

        let selected = select_cases(
            "shared",
            &cases,
            None,
            |case| case.differential,
            build_descriptor,
        )
        .expect("non-differential cases should be skipped");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].descriptor.case_id, "supported");

        let selection = BTreeSet::from(["contract-only".to_string()]);
        let error = select_cases(
            "shared",
            &cases,
            Some(&selection),
            |case| case.differential,
            build_descriptor,
        )
        .expect_err("non-differential selection should be rejected");
        assert!(error.contains("contract-only"), "error: {error}");
    }

    #[test]
    fn shared_execution_receipt_is_written_and_validated() {
        let temp = tempfile::tempdir().expect("receipt temp directory");
        let case = CompatCaseDescriptor::new(
            "shared",
            "case-a",
            "case-a",
            "fixture-a",
            "/case-a",
            BTreeMap::new(),
            true,
        );
        let mut recorder = CompatExecutionRecorder::new_at(
            "shared",
            &[case.clone()],
            Some("test-run"),
            temp.path(),
        )
        .expect("create receipt");
        recorder
            .record_case(&case, "pass", "matched")
            .expect("record receipt");
        recorder.finish("pass", "matched").expect("write receipt");
        validate_execution_receipt(recorder.path(), "shared", "pass").expect("validate receipt");
        let receipt = CompatExecutionRecorder::read(recorder.path()).expect("read receipt");
        assert_eq!(receipt["run_id"], "test-run");
        assert_eq!(receipt["selected_case_ids"], serde_json::json!(["case-a"]));
        assert_eq!(receipt["executed_case_ids"], serde_json::json!(["case-a"]));
        assert_eq!(
            receipt["selected_fixture_ids"],
            serde_json::json!(["fixture-a"])
        );
        assert_eq!(
            receipt["executed_fixture_ids"],
            serde_json::json!(["fixture-a"])
        );
        assert_eq!(
            receipt["cases"][0]["canonical_request"],
            serde_json::json!({
                "method": "GET",
                "params": {},
                "path": "/case-a"
            })
        );
        assert_eq!(receipt["cases"][0]["fingerprint_algorithm"], "SHA-256");
    }

    #[test]
    fn manifest_descriptor_fixture_provenance_is_preserved_in_receipt() {
        let temp = tempfile::tempdir().expect("receipt temp directory");
        let descriptor = super::conformance::descriptor_for_request(
            "loki",
            "loki-label-names-discovery",
            "GET",
            "/loki/api/v1/labels",
            BTreeMap::from([
                ("start".to_string(), "1700000000000000001".to_string()),
                ("end".to_string(), "1700000002000000000".to_string()),
            ]),
            true,
        )
        .expect("manifest descriptor");
        let mut recorder = CompatExecutionRecorder::new_at(
            "loki",
            &[descriptor.clone()],
            Some("manifest-run"),
            temp.path(),
        )
        .expect("create receipt");
        recorder
            .record_case(&descriptor, "pass", "matched")
            .expect("record receipt");
        recorder.finish("pass", "matched").expect("write receipt");

        let receipt = CompatExecutionRecorder::read(recorder.path()).expect("read receipt");
        assert_eq!(descriptor.fixture_id, "loki_success_minimal");
        assert_eq!(
            receipt["selected_fixture_ids"],
            serde_json::json!(["loki_success_minimal"])
        );
        assert_eq!(
            receipt["executed_fixture_ids"],
            serde_json::json!(["loki_success_minimal"])
        );
        assert_eq!(receipt["run_id"], "manifest-run");
    }

    #[test]
    fn selected_missing_manifest_case_is_rejected_even_when_request_matches_another_case() {
        let previous_case_id = std::env::var_os("COMPAT_CASE_ID");
        let previous_case_ids = std::env::var_os("COMPAT_CASE_IDS");
        std::env::remove_var("COMPAT_CASE_IDS");
        std::env::set_var("COMPAT_CASE_ID", "loki-query-backward-limit-order");

        let result = super::conformance::descriptor_for_case(
            "loki",
            "loki-query-backward-limit-order",
            "GET",
            "/loki/api/v1/labels",
            BTreeMap::from([
                ("start".to_string(), "1700000000000000001".to_string()),
                ("end".to_string(), "1700000002000000000".to_string()),
            ]),
            true,
        );

        match previous_case_id {
            Some(value) => std::env::set_var("COMPAT_CASE_ID", value),
            None => std::env::remove_var("COMPAT_CASE_ID"),
        }
        match previous_case_ids {
            Some(value) => std::env::set_var("COMPAT_CASE_IDS", value),
            None => std::env::remove_var("COMPAT_CASE_IDS"),
        }

        let error = result.expect_err("selected missing manifest case");
        assert!(error.contains("loki-query-backward-limit-order"), "{error}");
    }

    #[test]
    fn unfiltered_missing_manifest_case_gets_deterministic_fallback_descriptor() {
        let previous_case_id = std::env::var_os("COMPAT_CASE_ID");
        let previous_case_ids = std::env::var_os("COMPAT_CASE_IDS");
        std::env::remove_var("COMPAT_CASE_ID");
        std::env::remove_var("COMPAT_CASE_IDS");

        let params = BTreeMap::from([
            (
                "query".to_string(),
                "{service_name=\"checkout\"}".to_string(),
            ),
            ("start".to_string(), "1786827600000000001".to_string()),
            ("end".to_string(), "1786827601000000001".to_string()),
            ("direction".to_string(), "backward".to_string()),
            ("limit".to_string(), "1".to_string()),
        ]);
        let descriptor = super::conformance::descriptor_for_case(
            "loki",
            "loki-query-backward-limit-order",
            "GET",
            "/loki/api/v1/query",
            params.clone(),
            true,
        )
        .expect("unfiltered fallback descriptor");

        match previous_case_id {
            Some(value) => std::env::set_var("COMPAT_CASE_ID", value),
            None => std::env::remove_var("COMPAT_CASE_ID"),
        }
        match previous_case_ids {
            Some(value) => std::env::set_var("COMPAT_CASE_IDS", value),
            None => std::env::remove_var("COMPAT_CASE_IDS"),
        }

        assert_eq!(descriptor.protocol, "loki");
        assert_eq!(descriptor.case_id, "loki-query-backward-limit-order");
        assert_eq!(descriptor.source_id, "loki-query-backward-limit-order");
        assert_eq!(
            descriptor.fixture_id,
            "loki:loki-query-backward-limit-order"
        );
        assert_eq!(descriptor.method, "GET");
        assert_eq!(descriptor.path, "/loki/api/v1/query");
        assert_eq!(descriptor.params, params);
    }
}
