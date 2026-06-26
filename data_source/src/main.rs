use anyhow::Context;
use rand::{RngCore, SeedableRng};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use zkvm_common::{ChainHashFn, ChainInput, Event, KEY_BYTES_LEN};

fn proc_status_kb(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(field) {
            // e.g. "VmRSS:\t  12345 kB"
            if let Some(kb) = rest.split_whitespace().next().and_then(|v| v.parse::<u64>().ok()) {
                return Some(kb);
            }
        }
    }
    None
}

fn parse_arg_u64(name: &str, default: u64) -> u64 {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            if let Some(v) = it.next() {
                return v.parse::<u64>().unwrap_or(default);
            }
        }
    }
    default
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

fn env_u64(name: &str, default: u64) -> u64 {
    env_string(name)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    env_string(name)
        .and_then(|v| v.parse::<u8>().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    env_string(name)
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_path(name: &str) -> Option<PathBuf> {
    env_string(name).map(PathBuf::from)
}

struct ZipfSampler {
    cdf: Vec<f64>,
}

impl ZipfSampler {
    fn new(n: usize, s: f64) -> anyhow::Result<Self> {
        anyhow::ensure!(n >= 1, "Zipf n must be >= 1");
        anyhow::ensure!(s > 0.0, "VALUE_ZIPF_S must be > 0 (got {s})");
        let mut cdf: Vec<f64> = Vec::with_capacity(n);
        let mut sum = 0.0f64;
        for k in 1..=n {
            sum += (k as f64).powf(-s);
            cdf.push(sum);
        }
        for x in &mut cdf {
            *x /= sum;
        }
        if let Some(last) = cdf.last_mut() {
            *last = 1.0;
        }
        Ok(Self { cdf })
    }

    fn sample_u64<R: RngCore>(&self, rng: &mut R) -> u64 {
        let u = ((rng.next_u64() >> 11) as f64) / ((1u64 << 53) as f64);
        let idx = match self
            .cdf
            .binary_search_by(|p| p.partial_cmp(&u).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(i) => i,
            Err(i) => i,
        };
        idx as u64
    }
}

fn build_zipf_value(value_mod: u64) -> anyhow::Result<ZipfSampler> {
    anyhow::ensure!(value_mod > 0, "VALUE_MOD must be > 0 for zipf");
    let n = value_mod as usize;
    let max_n = env_u64("VALUE_ZIPF_MAX_N", 2_000_000) as usize;
    anyhow::ensure!(
        n <= max_n,
        "VALUE_MOD={} too large for Zipf precompute; increase VALUE_ZIPF_MAX_N (current {})",
        n,
        max_n
    );
    let s = env_f64("VALUE_ZIPF_S", 1.2);
    ZipfSampler::new(n, s)
}

fn has_flag(name: &str) -> bool {
    std::env::args().any(|a| a == name)
}

fn parse_arg_opt_u64(name: &str) -> Option<u64> {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next().and_then(|v| v.parse::<u64>().ok());
        }
    }
    None
}

fn parse_arg_string(name: &str) -> Option<String> {
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == name {
            return it.next();
        }
    }
    None
}

fn parse_hex_32(s: &str) -> anyhow::Result<[u8; 32]> {
    let s = s.trim();
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).context("hex decode")?;
    anyhow::ensure!(bytes.len() == 32, "expected 32 bytes, got {}", bytes.len());
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// Get current timestamp as u32 (seconds since epoch)
fn now_ts() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

/// Generate timestamp for an event (u32 seconds) - uses current system time
fn ts_for_event(_seq: u64) -> u32 {
    now_ts()
}

const TAG_SHARD_CHAIN: &[u8] = b"ZKTLM_SHARD_CHAIN_V1";

#[allow(dead_code)]
fn update_chain_hash_sha256(prev: [u8; 32], chunk31: &[u8; 31]) -> [u8; 32] {
    let mut sha = Sha256::new();
    sha.update(&prev);
    sha.update(chunk31);
    sha.finalize().into()
}

// 23-byte log entry: 15-byte key + 8-byte value (fits in one SHA-256 block with 32-byte prev_hash)
#[allow(dead_code)]
fn update_chain_hash_sha256_23(prev: [u8; 32], chunk: &[u8; 23]) -> [u8; 32] {
    let mut sha = Sha256::new();
    sha.update(&prev);
    sha.update(chunk);
    sha.finalize().into()
}

/// Compute events commitment: SHA256(all event data concatenated)
/// Each event contributes: key_id (15 bytes) + value (4 bytes) + ts (4 bytes) = 23 bytes
/// Order matches kafka_producer.rs: key_id || value || ts
fn events_commit_sha256(events: &[Event]) -> [u8; 32] {
    let mut sha = Sha256::new();
    for ev in events {
        sha.update(&ev.key_id);                // 15 bytes key
        sha.update(&ev.value.to_be_bytes());   // 4 bytes value
        sha.update(&ev.ts.to_be_bytes());      // 4 bytes timestamp
    }
    sha.finalize().into()
}

/// Batch-level chain hash: SHA256(TAG || chain_prev || events_commit)
/// This amortizes commitment overhead: one 32-byte hash per batch instead of per event.
fn batch_chain_hash_sha256(prev: [u8; 32], events: &[Event]) -> [u8; 32] {
    let commit = events_commit_sha256(events);
    let mut sha = Sha256::new();
    sha.update(TAG_SHARD_CHAIN);
    sha.update(&prev);
    sha.update(&commit);
    sha.finalize().into()
}

/// Batch-level chain hash for 23-byte chunks (streaming benchmark)
fn batch_chain_hash_sha256_23(prev: [u8; 32], chunks: &[[u8; 23]]) -> [u8; 32] {
    // First compute events_commit
    let mut sha = Sha256::new();
    for chunk in chunks {
        sha.update(chunk);
    }
    let commit: [u8; 32] = sha.finalize().into();

    // Then compute batch_hash = SHA256(TAG || prev || commit)
    let mut sha = Sha256::new();
    sha.update(TAG_SHARD_CHAIN);
    sha.update(&prev);
    sha.update(&commit);
    sha.finalize().into()
}

fn parse_parallel_chains_flag() -> bool {
    has_flag("--parallel-chains")
        || env_string("PARALLEL_CHAINS")
            .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true") || v == "yes")
}

fn default_caida_dir() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("testdata/caida_pcap/caida_txt"),
        PathBuf::from("../testdata/caida_pcap/caida_txt"),
    ];
    candidates.into_iter().find(|p| p.is_dir())
}

fn parse_tsv_line(line: &str) -> Option<(u64, u64)> {
    let s = line.trim();
    if s.is_empty() {
        return None;
    }
    let mut it = s.split('\t');
    let t = it.next()?.trim().parse::<u64>().ok()?;
    let v = it.next()?.trim().parse::<u64>().ok()?;
    Some((t, v))
}

fn trim_quotes(s: &str) -> &str {
    let s = s.trim();
    let s = s.strip_prefix('"').unwrap_or(s);
    s.strip_suffix('"').unwrap_or(s)
}

fn metric_id_from_csv_header(line: &str) -> Option<u8> {
    let mut it = line.split(',');
    let first = it.next()?;
    let name = trim_quotes(first).to_ascii_lowercase();
    if name.contains("cpu") {
        Some(1)
    } else if name.contains("mem") {
        Some(2)
    } else {
        None
    }
}

fn parse_csv_line(line: &str, value_scale: f64) -> Option<(u64, u64, u64)> {
    let s = line.trim();
    if s.is_empty() {
        return None;
    }
    let mut it = s.split(',');
    let value_raw = trim_quotes(it.next()?);
    let machine_raw = trim_quotes(it.next()?);
    let end_time_raw = trim_quotes(it.next()?);

    let value_f64 = value_raw.parse::<f64>().ok()?;
    let machine_id = machine_raw.parse::<u64>().ok()?;
    let end_time = end_time_raw.parse::<u64>().ok()?;

    let scaled = (value_f64 * value_scale).round();
    if !scaled.is_finite() || scaled < 0.0 {
        return None;
    }
    let value_u64 = scaled as u64;
    Some((end_time, value_u64, machine_id))
}

fn extract_machine_id_from_filename(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_string_lossy().to_string();
    let name_lc = name.to_ascii_lowercase();
    if let Some(rest) = name_lc.strip_prefix("avg_cpu_machine_id_") {
        return rest.trim_end_matches(".txt").parse::<u64>().ok();
    }
    if let Some(rest) = name_lc.strip_prefix("avg_mem_machine_id_") {
        return rest.trim_end_matches(".txt").parse::<u64>().ok();
    }
    if let Some(rest) = name_lc.strip_prefix("machine_") {
        let (mid, _rest) = rest.split_once("__")?;
        return mid.parse::<u64>().ok();
    }
    None
}

fn metric_id_from_filename(path: &Path) -> u8 {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if name.contains("cpu") {
        1
    } else if name.contains("mem") {
        2
    } else {
        255
    }
}

fn encode_key_id(metric_id: u8, machine_id: u64) -> [u8; KEY_BYTES_LEN] {
    let mut key = [0u8; KEY_BYTES_LEN];
    // 15-byte key layout: [metric_id (1 byte)] [padding (6 bytes)] [machine_id (8 bytes)]
    key[0] = metric_id;
    key[KEY_BYTES_LEN - 8..].copy_from_slice(&machine_id.to_be_bytes());
    key
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum DataFileFormat {
    Tsv,
    Csv,
}

struct TsvFile {
    key_id: [u8; KEY_BYTES_LEN],
    machine_id: Option<u64>,
    format: DataFileFormat,
    csv_value_scale: f64,
    reader: BufReader<fs::File>,
    path: PathBuf,
    pending: Option<(u64, u64)>,
}

impl TsvFile {
    fn open(path: PathBuf) -> anyhow::Result<Self> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let format = if ext == "csv" {
            DataFileFormat::Csv
        } else {
            DataFileFormat::Tsv
        };

        let f = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let mut reader = BufReader::new(f);

        let (key_id, machine_id, pending, csv_value_scale) = match format {
            DataFileFormat::Tsv => {
                let metric_id = metric_id_from_filename(&path);
                anyhow::ensure!(
                    metric_id != 255,
                    "metric id not found in filename: {}",
                    path.display()
                );
                let machine_id = extract_machine_id_from_filename(&path)
                    .with_context(|| format!("parse machine_id from filename: {}", path.display()))?;
                if machine_id >= (1u64 << 56) {
                    eprintln!(
                        "warning: machine_id {} overflows 56 bits; truncating in key_id",
                        machine_id
                    );
                }
                (encode_key_id(metric_id, machine_id), Some(machine_id), None, 1.0)
            }
            DataFileFormat::Csv => {
                let csv_value_scale = env_f64("CSV_VALUE_SCALE", 1_000_000.0);
                anyhow::ensure!(
                    csv_value_scale.is_finite() && csv_value_scale > 0.0,
                    "CSV_VALUE_SCALE must be a finite positive number"
                );

                let mut line = String::new();
                let mut metric_id = env_u8("CSV_METRIC_ID", 2);
                let mut first_row: Option<(u64, u64, u64)> = None;

                loop {
                    line.clear();
                    let n = reader
                        .read_line(&mut line)
                        .with_context(|| format!("read {}", path.display()))?;
                    if n == 0 {
                        break;
                    }
                    if let Some(mid) = metric_id_from_csv_header(&line) {
                        metric_id = mid;
                        continue;
                    }
                    if let Some(row) = parse_csv_line(&line, csv_value_scale) {
                        first_row = Some(row);
                        break;
                    }
                }

                let (t, v, machine_id) = first_row
                    .with_context(|| format!("no data rows in {}", path.display()))?;
                if machine_id >= (1u64 << 56) {
                    eprintln!(
                        "warning: machine_id {} overflows 56 bits; truncating in key_id",
                        machine_id
                    );
                }
                (
                    encode_key_id(metric_id, machine_id),
                    Some(machine_id),
                    Some((t, v)),
                    csv_value_scale,
                )
            }
        };

        Ok(Self {
            key_id,
            machine_id,
            format,
            csv_value_scale,
            reader,
            path,
            pending,
        })
    }

    fn next_row(&mut self) -> anyhow::Result<Option<(u64, u64)>> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        let mut line = String::new();
        let mut rewound = false;
        loop {
            line.clear();
            let n = self
                .reader
                .read_line(&mut line)
                .with_context(|| format!("read {}", self.path.display()))?;
            if n == 0 {
                if rewound {
                    return Ok(None);
                }
                let f = fs::File::open(&self.path)
                    .with_context(|| format!("reopen {}", self.path.display()))?;
                self.reader = BufReader::new(f);
                rewound = true;
                continue;
            }
            match self.format {
                DataFileFormat::Tsv => {
                    if let Some((t, v)) = parse_tsv_line(&line) {
                        return Ok(Some((t, v)));
                    }
                }
                DataFileFormat::Csv => {
                    if let Some((t, v, mid)) = parse_csv_line(&line, self.csv_value_scale) {
                        if let Some(expected) = self.machine_id {
                            if mid != expected {
                                eprintln!(
                                    "warning: machine_id mismatch in {}: expected {}, got {}",
                                    self.path.display(),
                                    expected,
                                    mid
                                );
                            }
                        }
                        return Ok(Some((t, v)));
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct HeapItem {
    start_time: u64,
    value: u64,
    file_idx: usize,
    key_id: [u8; KEY_BYTES_LEN],
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.start_time == other.start_time
            && self.key_id == other.key_id
            && self.file_idx == other.file_idx
            && self.value == other.value
    }
}
impl Eq for HeapItem {}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start_time
            .cmp(&other.start_time)
            .then_with(|| self.key_id.cmp(&other.key_id))
            .then_with(|| self.file_idx.cmp(&other.file_idx))
            .then_with(|| self.value.cmp(&other.value))
    }
}

fn collect_tsv_files(tsv_dir: &Path, max_files: usize) -> anyhow::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(tsv_dir).with_context(|| format!("read_dir {}", tsv_dir.display()))? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let ext = p.extension().and_then(|e| e.to_str()).unwrap_or("");
        if ext != "tsv" && ext != "txt" && ext != "csv" {
            continue;
        }
        if ext != "csv" {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let name_lc = name.to_ascii_lowercase();
            if !name_lc.starts_with("machine_")
                && !name_lc.starts_with("avg_cpu_machine_id_")
                && !name_lc.starts_with("avg_mem_machine_id_")
            {
                continue;
            }
            if extract_machine_id_from_filename(&p).is_none() {
                continue;
            }
            if metric_id_from_filename(&p) == 255 {
                continue;
            }
        }
        out.push(p);
    }
    out.sort();
    if max_files > 0 && out.len() > max_files {
        out.truncate(max_files);
    }
    anyhow::ensure!(
        !out.is_empty(),
        "no TSV files found in {}",
        tsv_dir.display()
    );
    Ok(out)
}

#[derive(Clone, Debug)]
struct BenchEvent {
    ts: u32,   // 32-bit timestamp (seconds)
    key_id: [u8; KEY_BYTES_LEN],
    value: u32,  // 32-bit value
}

struct TsvEventSource {
    files: Vec<TsvFile>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    max_events: u64,
    done: u64,
    ts_base: u32,       // Base timestamp in seconds
    ts_interval: u32,   // Interval in seconds
    ts_mode_now: bool,
}

impl TsvEventSource {
    fn new(
        tsv_dir: &Path,
        max_files: usize,
        max_events: u64,
        ts_interval_ms: u64,
    ) -> anyhow::Result<Self> {
        let ts_mode_now = matches!(env_string("TS_MODE").as_deref(), Some("now"));
        let tsv_paths = collect_tsv_files(tsv_dir, max_files)?;
        let mut files: Vec<TsvFile> = tsv_paths
            .into_iter()
            .map(TsvFile::open)
            .collect::<anyhow::Result<Vec<_>>>()?;

        let mut heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
        for (idx, f) in files.iter_mut().enumerate() {
            if let Some((t, v)) = f.next_row()? {
                heap.push(Reverse(HeapItem {
                    start_time: t,
                    value: v,
                    file_idx: idx,
                    key_id: f.key_id,
                }));
            }
        }
        anyhow::ensure!(
            !heap.is_empty(),
            "no rows found in TSV files under {}",
            tsv_dir.display()
        );

        Ok(Self {
            files,
            heap,
            max_events,
            done: 0,
            ts_base: now_ts(),
            ts_interval: (ts_interval_ms / 1000).max(1) as u32,  // Convert ms to seconds
            ts_mode_now,
        })
    }

    fn next_event(&mut self) -> anyhow::Result<Option<BenchEvent>> {
        if self.max_events > 0 && self.done >= self.max_events {
            return Ok(None);
        }
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };

        let ts = if self.ts_mode_now {
            now_ts()
        } else {
            self.ts_base
                .saturating_add(self.ts_interval.saturating_mul(self.done as u32))
        };

        let key_id = item.key_id;
        let value = item.value as u32;  // Truncate to u32

        let idx = item.file_idx;
        if let Some((t2, v2)) = self.files[idx].next_row()? {
            self.heap.push(Reverse(HeapItem {
                start_time: t2,
                value: v2,
                file_idx: idx,
                key_id,
            }));
        }

        self.done = self.done.saturating_add(1);
        Ok(Some(BenchEvent {
            ts,
            key_id,
            value,
        }))
    }
}

struct CaidaEventSource {
    files: Vec<CaidaFile>,
    file_idx: usize,
    max_events: u64,
    done: u64,
}

impl CaidaEventSource {
    fn new(caida_dir: &Path, max_files: usize, max_events: u64) -> anyhow::Result<Self> {
        eprintln!(
            "caida_load_begin dir={} max_files={} max_events={}",
            caida_dir.display(),
            max_files,
            max_events
        );
        let paths = collect_caida_txt_files(caida_dir, max_files)?;
        let files = paths
            .into_iter()
            .map(CaidaFile::open)
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !files.is_empty(),
            "no CAIDA txt files found in {}",
            caida_dir.display()
        );
        eprintln!("caida_load_finish files={}", files.len());
        Ok(Self {
            files,
            file_idx: 0,
            max_events,
            done: 0,
        })
    }

    fn next_event(&mut self) -> anyhow::Result<Option<BenchEvent>> {
        if self.max_events > 0 && self.done >= self.max_events {
            return Ok(None);
        }
        loop {
            if self.file_idx >= self.files.len() {
                return Ok(None);
            }
            let f = &mut self.files[self.file_idx];
            if let Some((src_ip, dst_ip, pkt_len)) = f.next_row()? {
                let ts = ts_for_event(self.done);
                // Encode src_ip and dst_ip into 15-byte key: upper bytes for src_ip, lower for dst_ip
                let key_num = ((src_ip as u64) << 32) | (dst_ip as u64);
                let key_id = Event::key_id_from_u64(key_num);
                let value = pkt_len;  // Already u32
                self.done = self.done.saturating_add(1);
                return Ok(Some(BenchEvent { ts, key_id, value }));
            }
            self.file_idx = self.file_idx.saturating_add(1);
        }
    }
}

struct CaidaFile {
    path: PathBuf,
    r: BufReader<fs::File>,
    line_no: u64,
}

impl CaidaFile {
    fn open(path: PathBuf) -> anyhow::Result<Self> {
        let f = fs::File::open(&path)
            .with_context(|| format!("open CAIDA txt {}", path.display()))?;
        Ok(Self {
            path,
            r: BufReader::new(f),
            line_no: 0,
        })
    }

    fn next_row(&mut self) -> anyhow::Result<Option<(u32, u32, u32)>> {
        let mut line = String::new();
        loop {
            line.clear();
            let n = self
                .r
                .read_line(&mut line)
                .with_context(|| format!("read CAIDA txt {}", self.path.display()))?;
            if n == 0 {
                return Ok(None);
            }
            self.line_no = self.line_no.saturating_add(1);
            let s = line.trim();
            if s.is_empty() {
                continue;
            }
            let mut it = s.split_whitespace();
            let src = it.next();
            let dst = it.next();
            let len = it.next();
            if src.is_none() || dst.is_none() || len.is_none() || it.next().is_some() {
                anyhow::bail!(
                    "invalid CAIDA row (expected: src_ip dst_ip pkt_len) at {}:{}: {}",
                    self.path.display(),
                    self.line_no,
                    s
                );
            }
            let src_ip: u32 = src
                .unwrap()
                .parse()
                .with_context(|| format!("parse src_ip at {}:{}", self.path.display(), self.line_no))?;
            let dst_ip: u32 = dst
                .unwrap()
                .parse()
                .with_context(|| format!("parse dst_ip at {}:{}", self.path.display(), self.line_no))?;
            let pkt_len: u32 = len
                .unwrap()
                .parse()
                .with_context(|| format!("parse pkt_len at {}:{}", self.path.display(), self.line_no))?;
            return Ok(Some((src_ip, dst_ip, pkt_len)));
        }
    }
}

fn collect_caida_txt_files(caida_dir: &Path, max_files: usize) -> anyhow::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    for ent in fs::read_dir(caida_dir)
        .with_context(|| format!("read CAIDA_DIR {}", caida_dir.display()))?
    {
        let ent = ent?;
        let p = ent.path();
        if !p.is_file() {
            continue;
        }
        if p.extension().and_then(|s| s.to_str()) != Some("txt") {
            continue;
        }
        out.push(p);
    }
    let sort_by_size = env_u8("CAIDA_SORT_BY_SIZE", 1) != 0;
    if sort_by_size {
        let mut with_sizes: Vec<(PathBuf, u64)> = Vec::with_capacity(out.len());
        for p in out {
            let len = fs::metadata(&p)
                .with_context(|| format!("stat CAIDA txt {}", p.display()))?
                .len();
            with_sizes.push((p, len));
        }
        with_sizes.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out = with_sizes.into_iter().map(|(p, _)| p).collect();
    } else {
        out.sort();
    }
    if max_files > 0 && out.len() > max_files {
        out.truncate(max_files);
    }
    anyhow::ensure!(
        !out.is_empty(),
        "no CAIDA txt files found in {}",
        caida_dir.display()
    );
    Ok(out)
}

/// Synthetic event source with round-robin key cycling.
/// Tracks current key index for consistent mixed-key generation.
struct SyntheticEventSource {
    current_key: u64,
}

impl SyntheticEventSource {
    fn new() -> Self {
        Self { current_key: 0 }
    }
}

enum BenchInputSource {
    Caida(CaidaEventSource),
    Tsv(TsvEventSource),
    Synthetic(SyntheticEventSource),
}

impl BenchInputSource {
    fn next_event(
        &mut self,
        rng: &mut rand::rngs::StdRng,
        value_zipf: Option<&ZipfSampler>,
        key_mod: u64,
        value_mod: u32,
    ) -> anyhow::Result<Option<BenchEvent>> {
        match self {
            Self::Caida(s) => s.next_event(),
            Self::Tsv(s) => s.next_event(),
            Self::Synthetic(s) => {
                let ts = now_ts();  // Use current system time
                // Round-robin through keys (matches generate_epoch_batches)
                let key_num = if key_mod > 0 {
                    let k = s.current_key % key_mod;
                    s.current_key = s.current_key.wrapping_add(1);
                    k
                } else {
                    let k = s.current_key;
                    s.current_key = s.current_key.wrapping_add(1);
                    k
                };
                let key_id = Event::key_id_from_u64(key_num);
                let value = if let Some(zipf) = value_zipf {
                    zipf.sample_u64(rng).saturating_add(1) as u32
                } else if value_mod > 0 {
                    rng.next_u32() % value_mod
                } else {
                    rng.next_u32()
                };
                Ok(Some(BenchEvent {
                    ts,
                    key_id,
                    value,
                }))
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    let bench = has_flag("--bench");
    let parallel_chains = parse_parallel_chains_flag();
    anyhow::ensure!(
        !std::env::args().any(|a| a == "--skip-verify"),
        "--skip-verify is no longer supported (no proof is generated)"
    );
    anyhow::ensure!(
        !std::env::args().any(|a| a == "--no-measure-bytes"),
        "--no-measure-bytes is no longer supported (no proof/journal is generated)"
    );
    anyhow::ensure!(
        !std::env::args().any(|a| a == "--hash-fn" || a.starts_with("--hash-fn=")),
        "--hash-fn is no longer supported; data_source commitment is sha256-only"
    );
    // Data source commitment is SHA-256 only.
    let hash_fn = ChainHashFn::Sha256;
    if let Some(n) = parse_arg_opt_u64("--threads") {
        if n > 0 {
            // Left for CLI compatibility with existing benchmark scripts.
            std::env::set_var("RAYON_NUM_THREADS", n.to_string());
        }
    }
    let requested_events = parse_arg_u64("--events", 1_000);
    let mut batch_size = parse_arg_u64("--batch-size", requested_events).max(1);
    let warmup_batches = parse_arg_u64("--warmup-batches", 0);
    let key_mod = parse_arg_u64("--key-mod", 1_000).max(1);
    let value_mod = parse_arg_u64("--value-mod", 10_000).max(1) as u32;
    let seed = parse_arg_u64("--seed", 0x5EED);
    // Timestamp is generated from current system time when event is created
    let prev_hash = parse_arg_string("--prev-hash-hex")
        .map(|s| parse_hex_32(&s))
        .transpose()?
        .unwrap_or([0u8; 32]);

    let bench_input = parse_arg_string("--bench-input")
        .or_else(|| env_string("BENCH_INPUT"))
        .unwrap_or_else(|| "synthetic".to_string());

    let streaming = has_flag("--streaming");

    // Streaming mode: benchmark per-key hash chains with optional parallelism.
    if streaming && bench {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let value_zipf = if env_string("VALUE_ZIPF_S").is_some() {
            Some(build_zipf_value(value_mod as u64)?)
        } else {
            None
        };

        let rss_kb_baseline = proc_status_kb("VmRSS:");

        // Pre-generate all events to exclude RNG from timing
        // Log structure: 23 bytes = 15-byte key + 4-byte value + 4-byte ts
        // Order matches kafka_producer.rs: key_id || value || ts
        // SHA-256 input: 32-byte prev_hash + 23-byte log = 55 bytes (fits in one 64-byte block)
        let mut events: Vec<(u64, [u8; 23])> = Vec::with_capacity(requested_events as usize);
        for _ in 0..requested_events {
            let key_id = rng.next_u64() % key_mod;
            let value: u32 = if let Some(ref zipf) = value_zipf {
                zipf.sample_u64(&mut rng) as u32
            } else {
                rng.next_u32() % value_mod
            };
            let ts = now_ts();
            // 23-byte chunk: 15-byte key + 4-byte value + 4-byte ts
            // Order: key_id || value || ts (matches kafka_producer.rs)
            let chunk = [
                // Key: 15 bytes (8-byte key_id in big-endian + 7 zero bytes)
                (key_id >> 56) as u8, (key_id >> 48) as u8, (key_id >> 40) as u8, (key_id >> 32) as u8,
                (key_id >> 24) as u8, (key_id >> 16) as u8, (key_id >> 8) as u8, key_id as u8,
                0, 0, 0, 0, 0, 0, 0,
                // Value: 4 bytes (u32 big-endian)
                (value >> 24) as u8, (value >> 16) as u8, (value >> 8) as u8, value as u8,
                // Timestamp: 4 bytes (u32 big-endian)
                (ts >> 24) as u8, (ts >> 16) as u8, (ts >> 8) as u8, ts as u8,
            ];
            events.push((key_id, chunk));
        }
        let rss_kb_after_events = proc_status_kb("VmRSS:");

        // Group events by key_id for per-key chain processing
        let mut events_by_key: HashMap<u64, Vec<[u8; 23]>> = HashMap::new();
        for (key_id, chunk) in &events {
            events_by_key.entry(*key_id).or_default().push(*chunk);
        }
        let n_chains = events_by_key.len();

        // Drop original events Vec to free memory before measuring hash-only usage
        drop(events);
        let rss_kb_after_grouping = proc_status_kb("VmRSS:");

        // Warmup pass (batch-level hashing)
        for (_key_id, chunks) in &events_by_key {
            let _ = batch_chain_hash_sha256_23(prev_hash, chunks);
        }

        // Snapshot before hash computation
        let rss_kb_before_hash = proc_status_kb("VmRSS:");

        // Timed run: per-key batch chains (serial)
        // Batch-level hashing: one SHA256 per batch instead of per event
        let serial_start = std::time::Instant::now();
        let mut final_hashes: HashMap<u64, [u8; 32]> = HashMap::new();
        for (key_id, chunks) in &events_by_key {
            let h = batch_chain_hash_sha256_23(prev_hash, chunks);
            final_hashes.insert(*key_id, h);
        }
        let serial_ns = serial_start.elapsed().as_nanos();
        let rss_kb_after_serial = proc_status_kb("VmRSS:");

        // Timed run: per-key batch chains (parallel with rayon)
        let parallel_start = std::time::Instant::now();
        let items: Vec<(u64, &Vec<[u8; 23]>)> = events_by_key.iter().map(|(k, v)| (*k, v)).collect();
        let parallel_results: Vec<(u64, [u8; 32])> = items
            .par_iter()
            .map(|(key_id, chunks)| {
                let h = batch_chain_hash_sha256_23(prev_hash, chunks);
                (*key_id, h)
            })
            .collect();
        let parallel_ns = parallel_start.elapsed().as_nanos();
        let rss_kb_after_parallel = proc_status_kb("VmRSS:");

        // Prevent optimizer from removing computations
        if final_hashes.len() != parallel_results.len() { println!("mismatch"); }

        let proc_hwm_kb = proc_status_kb("VmHWM:");

        println!("bench=1");
        println!("mode=streaming");
        println!("hash_fn=sha256");
        println!("n_events={}", requested_events);
        println!("key_mod={}", key_mod);
        println!("n_chains={}", n_chains);
        println!("parallel_chains={}", if parallel_chains { 1 } else { 0 });

        // Report serial timing
        println!("serial_ns={}", serial_ns);
        println!("serial_ms={}", serial_ns / 1_000_000);
        println!("serial_ns_per_event={:.3}", (serial_ns as f64) / (requested_events.max(1) as f64));

        // Report parallel timing
        println!("parallel_ns={}", parallel_ns);
        println!("parallel_ms={}", parallel_ns / 1_000_000);
        println!("parallel_ns_per_event={:.3}", (parallel_ns as f64) / (requested_events.max(1) as f64));

        // Report speedup
        println!("speedup={:.2}x", (serial_ns as f64) / (parallel_ns.max(1) as f64));

        // Memory breakdown (KB)
        if let Some(kb) = rss_kb_baseline {
            println!("rss_kb_baseline={}", kb);
        }
        if let Some(kb) = rss_kb_after_events {
            println!("rss_kb_after_events={}", kb);
        }
        if let Some(kb) = rss_kb_after_grouping {
            println!("rss_kb_after_grouping={}", kb);
        }
        if let Some(kb) = rss_kb_before_hash {
            println!("rss_kb_before_hash={}", kb);
        }
        if let Some(kb) = rss_kb_after_serial {
            println!("rss_kb_after_serial={}", kb);
        }
        if let Some(kb) = rss_kb_after_parallel {
            println!("rss_kb_after_parallel={}", kb);
        }
        if let Some(kb) = proc_hwm_kb {
            println!("rss_kb_hwm={}", kb);
        }

        // Computed deltas (SHA256-only memory)
        let hash_mem_serial_kb = rss_kb_after_serial.unwrap_or(0).saturating_sub(rss_kb_before_hash.unwrap_or(0));
        let hash_mem_parallel_kb = rss_kb_after_parallel.unwrap_or(0).saturating_sub(rss_kb_before_hash.unwrap_or(0));
        println!("hash_mem_serial_kb={}", hash_mem_serial_kb);
        println!("hash_mem_parallel_kb={}", hash_mem_parallel_kb);

        return Ok(());
    }

    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let mut events: Vec<Event> = Vec::with_capacity(requested_events as usize);
    let max_events = requested_events;
    let value_zipf = if bench_input.as_str() == "synthetic" && env_string("VALUE_ZIPF_S").is_some()
    {
        Some(build_zipf_value(value_mod as u64)?)
    } else {
        None
    };
    let mut source = match bench_input.as_str() {
        "synthetic" => BenchInputSource::Synthetic(SyntheticEventSource::new()),
        "tsv" | "google" | "google_cluster" | "google_cluster_data" => {
            let tsv_dir = parse_arg_string("--tsv-dir")
                .map(PathBuf::from)
                .or_else(|| env_path("TSV_DIR"))
                .context("TSV_DIR is required for BENCH_INPUT=tsv")?;
            let max_files = env_u64("TSV_MAX_FILES", 64) as usize;
            let ts_interval_ms = env_u64("TS_INTERVAL_MS", 100);
            BenchInputSource::Tsv(TsvEventSource::new(
                &tsv_dir,
                max_files,
                max_events,
                ts_interval_ms,
            )?)
        }
        "caida" | "caida_txt" => {
            let caida_dir = parse_arg_string("--caida-dir")
                .map(PathBuf::from)
                .or_else(|| env_path("CAIDA_DIR"))
                .or_else(default_caida_dir)
                .context("CAIDA_DIR is required for BENCH_INPUT=caida")?;
            let max_files = env_u64("CAIDA_MAX_FILES", 64) as usize;
            BenchInputSource::Caida(CaidaEventSource::new(
                &caida_dir,
                max_files,
                max_events,
            )?)
        }
        other => anyhow::bail!(
            "unsupported BENCH_INPUT={other}; expected synthetic, tsv/google, or caida"
        ),
    };

    let mut seq: u64 = 0;
    loop {
        if max_events > 0 && seq >= max_events {
            break;
        }
        let next = source.next_event(
            &mut rng,
            value_zipf.as_ref(),
            key_mod,
            value_mod,
        )?;
        let Some(ev) = next else {
            break;
        };
        events.push(Event {
            ts: ev.ts,
            key_id: ev.key_id,
            value: ev.value,
        });
        seq = seq.saturating_add(1);
    }

    let n_events = events.len() as u64;
    if n_events > 0 {
        batch_size = batch_size.min(n_events).max(1);
    }

    let input = ChainInput {
        prev_hash,
        hash_fn,
        events,
    };

    // Compute expected final hash using batch-level hashing
    let host_hash_start = std::time::Instant::now();
    let expected_final = batch_chain_hash_sha256(input.prev_hash, &input.events);
    let host_hash_ms = host_hash_start.elapsed().as_millis();

    let proc_rss_kb_start = proc_status_kb("VmRSS:");
    let mut hash_ns_total: u128 = 0;
    let mut timed_events: u64 = 0;

    let mut running_hash = input.prev_hash;
    let mut expected_running_hash = input.prev_hash;
    let mut batches_done: u64 = 0;

    let mut running_hashes_by_key: HashMap<[u8; KEY_BYTES_LEN], [u8; 32]> = HashMap::new();
    let mut expected_hashes_by_key: HashMap<[u8; KEY_BYTES_LEN], [u8; 32]> = HashMap::new();
    let unique_keys: std::collections::HashSet<u64> = std::collections::HashSet::new();

    let mut idx: usize = 0;
    while idx < input.events.len() {
        let end = (idx + (batch_size as usize)).min(input.events.len());
        let batch_events = &input.events[idx..end];

        // Warmup runs are excluded from timing.
        let is_warmup = batches_done < warmup_batches;

        // Batch-level hashing: one SHA256 per batch instead of per event
        let hash_start = std::time::Instant::now();
        if parallel_chains {
            // Partition by key_id and compute batch hash per key (in parallel).
            let mut by_key: HashMap<[u8; KEY_BYTES_LEN], Vec<Event>> = HashMap::new();
            for ev in batch_events.iter().copied() {
                by_key.entry(ev.key_id).or_default().push(ev);
            }
            let items: Vec<([u8; KEY_BYTES_LEN], Vec<Event>)> = by_key.into_iter().collect();

            let updates: Vec<([u8; KEY_BYTES_LEN], [u8; 32])> = items
                .par_iter()
                .map(|(key_id, evs)| {
                    let prev = *running_hashes_by_key.get(key_id).unwrap_or(&input.prev_hash);
                    (*key_id, batch_chain_hash_sha256(prev, evs))
                })
                .collect();

            for (key_id, h) in updates {
                running_hashes_by_key.insert(key_id, h);
            }
        } else {
            running_hash = batch_chain_hash_sha256(running_hash, batch_events);
        }
        let hash_ns = hash_start.elapsed().as_nanos();
        if !is_warmup {
            hash_ns_total = hash_ns_total.saturating_add(hash_ns);
            timed_events = timed_events.saturating_add((end - idx) as u64);
        }

        if parallel_chains {
            // Serial reference for correctness (batch-level hashing).
            let mut by_key: HashMap<[u8; KEY_BYTES_LEN], Vec<Event>> = HashMap::new();
            for ev in input.events[idx..end].iter().copied() {
                by_key.entry(ev.key_id).or_default().push(ev);
            }
            for (key_id, evs) in by_key {
                let prev = *expected_hashes_by_key.get(&key_id).unwrap_or(&input.prev_hash);
                let h = batch_chain_hash_sha256(prev, &evs);
                expected_hashes_by_key.insert(key_id, h);
            }
        } else {
            expected_running_hash = batch_chain_hash_sha256(expected_running_hash, batch_events);
        }

        idx = end;
        batches_done = batches_done.saturating_add(1);
    }

    let timed_batches = batches_done.saturating_sub(warmup_batches);
    if parallel_chains {
        anyhow::ensure!(
            running_hashes_by_key == expected_hashes_by_key,
            "per-key final hash mismatch vs serial recompute"
        );
    } else {
        anyhow::ensure!(
            running_hash == expected_final,
            "final hash mismatch vs host recompute"
        );
    }
    let proc_rss_kb_end = proc_status_kb("VmRSS:");
    let proc_hwm_kb = proc_status_kb("VmHWM:");

    if bench {
        println!("bench=1");
        println!("bench_input={}", bench_input);
        println!("hash_fn={}", hash_fn.as_str());
        println!("n_events={}", n_events);
        println!("batch_size={}", batch_size);
        println!("warmup_batches={}", warmup_batches);
        println!("batches_total={}", batches_done);
        println!("batches_timed={}", timed_batches);
        println!("timed_events={}", timed_events);
        println!("host_hash_ms={}", host_hash_ms);
        println!("parallel_chains={}", if parallel_chains { 1 } else { 0 });
        println!("key_mod={}", key_mod);
        println!("n_chains={}", unique_keys.len());
        println!("hash_ns_total={}", hash_ns_total);
        println!("hash_ms_total={}", hash_ns_total / 1_000_000);
        println!(
            "hash_ns_per_event={:.3}",
            (hash_ns_total as f64) / (timed_events.max(1) as f64)
        );
        if let Some(kb) = proc_rss_kb_start {
            println!("proc_rss_kb_start={}", kb);
        }
        if let Some(kb) = proc_rss_kb_end {
            println!("proc_rss_kb_end={}", kb);
        }
        if let Some(kb) = proc_hwm_kb {
            println!("proc_hwm_kb={}", kb);
        }
        if !parallel_chains {
            println!("final_hash_hex={}", hex::encode(running_hash));
        }
    } else {
        println!("n_events={}", n_events);
        println!("n_chains={}", unique_keys.len());
        if !parallel_chains {
            println!("final_hash_hex={}", hex::encode(running_hash));
            println!("expected_final_hash_hex={}", hex::encode(expected_final));
        }
    }

    Ok(())
}
