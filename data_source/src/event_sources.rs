//! Event sources for loading data from different formats.
//!
//! Supports:
//! - Synthetic: Generated random data
//! - TSV/Google Cluster: Machine resource usage data in TSV/CSV format
//! - CAIDA: Network packet data
//! - Car Emission: Vehicle CO2 emission data (model year as timestamp, 15-byte encoded key)

use anyhow::Context;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use zktelemetry_risc0_common::KEY_BYTES_LEN;

/// A benchmark event with timestamp, key, and value.
#[derive(Clone, Debug)]
pub struct BenchEvent {
    pub ts: u32,
    pub key_id: [u8; KEY_BYTES_LEN],
    pub value: u32,
}

/// Get current timestamp in seconds.
pub fn now_ts() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

/// Generate timestamp for an event based on sequence number.
pub fn ts_for_event(seq: u64) -> u32 {
    let base = now_ts();
    let interval = std::env::var("TS_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(100);
    let interval_sec = (interval / 1000).max(1) as u32;
    base.saturating_add(interval_sec.saturating_mul(seq as u32))
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u8(name: &str, default: u8) -> u8 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Default CAIDA directory candidates.
pub fn default_caida_dir() -> Option<PathBuf> {
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

/// Encode metric_id and machine_id into a 15-byte key_id.
/// Layout: [metric_id (1 byte)] [padding (6 bytes)] [machine_id (8 bytes)]
pub fn encode_key_id(metric_id: u8, machine_id: u64) -> [u8; KEY_BYTES_LEN] {
    let mut key = [0u8; KEY_BYTES_LEN];
    key[0] = metric_id;
    key[KEY_BYTES_LEN - 8..].copy_from_slice(&machine_id.to_be_bytes());
    key
}

/// Extract machine_id from a key_id (last 8 bytes).
pub fn extract_machine_id_from_key(key_id: &[u8; KEY_BYTES_LEN]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&key_id[KEY_BYTES_LEN - 8..]);
    u64::from_be_bytes(bytes)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum DataFileFormat {
    Tsv,
    Csv,
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

/// Pre-loaded event from a TSV/CSV file.
#[derive(Clone)]
struct PreloadedEvent {
    start_time: u64,
    value: u64,
    key_id: [u8; KEY_BYTES_LEN],
}

/// Load all events from a single file into memory, then close the file.
fn preload_file_events(path: &Path) -> anyhow::Result<Vec<PreloadedEvent>> {
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

    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut reader = BufReader::new(f);

    let (key_id, csv_value_scale) = match format {
        DataFileFormat::Tsv => {
            let metric_id = metric_id_from_filename(path);
            anyhow::ensure!(
                metric_id != 255,
                "metric id not found in filename: {}",
                path.display()
            );
            let machine_id = extract_machine_id_from_filename(path)
                .with_context(|| format!("parse machine_id from filename: {}", path.display()))?;
            (encode_key_id(metric_id, machine_id), 1.0)
        }
        DataFileFormat::Csv => {
            let csv_value_scale = env_f64("CSV_VALUE_SCALE", 1_000_000.0);
            anyhow::ensure!(
                csv_value_scale.is_finite() && csv_value_scale > 0.0,
                "CSV_VALUE_SCALE must be a finite positive number"
            );

            // Read first data row to get machine_id
            let mut line = String::new();
            let mut metric_id = env_u8("CSV_METRIC_ID", 2);
            let mut machine_id: Option<u64> = None;

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
                if let Some((_t, _v, mid)) = parse_csv_line(&line, csv_value_scale) {
                    machine_id = Some(mid);
                    break;
                }
            }

            let mid = machine_id
                .with_context(|| format!("no data rows in {}", path.display()))?;

            // Reset reader to beginning
            drop(reader);
            let f = fs::File::open(path).with_context(|| format!("reopen {}", path.display()))?;
            reader = BufReader::new(f);

            (encode_key_id(metric_id, mid), csv_value_scale)
        }
    };

    // Read all events from the file
    let mut events = Vec::new();
    let mut line = String::new();

    loop {
        line.clear();
        let n = reader
            .read_line(&mut line)
            .with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }

        match format {
            DataFileFormat::Tsv => {
                if let Some((t, v)) = parse_tsv_line(&line) {
                    events.push(PreloadedEvent {
                        start_time: t,
                        value: v,
                        key_id,
                    });
                }
            }
            DataFileFormat::Csv => {
                // Skip header lines
                if metric_id_from_csv_header(&line).is_some() {
                    continue;
                }
                if let Some((t, v, _mid)) = parse_csv_line(&line, csv_value_scale) {
                    events.push(PreloadedEvent {
                        start_time: t,
                        value: v,
                        key_id,
                    });
                }
            }
        }
    }

    // File handle is automatically closed when reader goes out of scope
    Ok(events)
}

/// Event source for TSV/Google Cluster data.
/// Preloads all events into memory to avoid file descriptor limits.
pub struct TsvEventSource {
    /// All events sorted by timestamp, ready to iterate
    heap: BinaryHeap<Reverse<HeapItem>>,
    /// Number of files that were loaded
    num_files: usize,
    max_events: u64,
    done: u64,
    ts_base: u32,
    ts_interval: u32,
    ts_mode_now: bool,
}

impl TsvEventSource {
    /// Create a new TSV event source.
    /// Preloads all events into memory to avoid "too many open files" errors.
    pub fn new(
        tsv_dir: &Path,
        max_files: usize,
        max_events: u64,
        ts_interval_ms: u64,
    ) -> anyhow::Result<Self> {
        let ts_mode_now = matches!(env_string("TS_MODE").as_deref(), Some("now"));
        let tsv_paths = collect_tsv_files(tsv_dir, max_files)?;
        let num_files = tsv_paths.len();
        eprintln!(
            "[TsvEventSource] loading {} files from {}",
            num_files,
            tsv_dir.display()
        );

        // Preload all events from all files into memory
        let mut heap: BinaryHeap<Reverse<HeapItem>> = BinaryHeap::new();
        let mut total_events: u64 = 0;
        let mut files_loaded: usize = 0;

        for (idx, path) in tsv_paths.iter().enumerate() {
            // Skip empty/malformed files instead of aborting the whole load
            // (some Google-cluster CSVs are header-only). They just don't
            // contribute a key; the rest of the workload is unaffected.
            let events = match preload_file_events(path) {
                Ok(ev) => ev,
                Err(e) => {
                    eprintln!("[TsvEventSource] skipping {}: {}", path.display(), e);
                    continue;
                }
            };
            for ev in events {
                heap.push(Reverse(HeapItem {
                    start_time: ev.start_time,
                    value: ev.value,
                    file_idx: idx,
                    key_id: ev.key_id,
                }));
                total_events += 1;
            }
            files_loaded += 1;

            // Progress logging for large loads
            if files_loaded % 1000 == 0 {
                eprintln!(
                    "[TsvEventSource] loaded {}/{} files ({} events so far)",
                    files_loaded, num_files, total_events
                );
            }
        }

        eprintln!(
            "[TsvEventSource] preloaded {} events from {} files",
            total_events, num_files
        );

        anyhow::ensure!(
            !heap.is_empty(),
            "no rows found in TSV files under {}",
            tsv_dir.display()
        );

        Ok(Self {
            heap,
            num_files,
            max_events,
            done: 0,
            ts_base: now_ts(),
            ts_interval: (ts_interval_ms / 1000).max(1) as u32,
            ts_mode_now,
        })
    }

    /// Get the next event from the source.
    pub fn next_event(&mut self) -> anyhow::Result<Option<BenchEvent>> {
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
        let value = item.value as u32;

        self.done = self.done.saturating_add(1);
        Ok(Some(BenchEvent { ts, key_id, value }))
    }

    /// Get the number of files loaded.
    pub fn num_files(&self) -> usize {
        self.num_files
    }
}

struct CaidaFile {
    path: PathBuf,
    r: BufReader<fs::File>,
    #[allow(dead_code)]
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

/// Event source for CAIDA network packet data.
/// Preloads all events into memory to avoid file descriptor limits.
pub struct CaidaEventSource {
    /// All events sorted by a combination of file index and sequence for fair round-robin
    heap: BinaryHeap<Reverse<CaidaHeapItem>>,
    /// Number of files that were loaded
    num_files: usize,
    max_events: u64,
    done: u64,
}

#[derive(Clone)]
struct CaidaHeapItem {
    /// Ordering key: interleaved by (sequence_within_file, file_idx) for round-robin effect
    order_key: u64,
    src_ip: u32,
    dst_ip: u32,
    pkt_len: u32,
}

impl PartialEq for CaidaHeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.order_key == other.order_key
    }
}
impl Eq for CaidaHeapItem {}
impl PartialOrd for CaidaHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CaidaHeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key.cmp(&other.order_key)
    }
}

impl CaidaEventSource {
    /// Create a new CAIDA event source.
    /// Preloads all events into memory to avoid "too many open files" errors.
    pub fn new(caida_dir: &Path, max_files: usize, max_events: u64) -> anyhow::Result<Self> {
        eprintln!(
            "[CaidaEventSource] loading from {} max_files={} max_events={}",
            caida_dir.display(),
            max_files,
            max_events
        );
        let paths = collect_caida_txt_files(caida_dir, max_files)?;
        let num_files = paths.len();

        // Preload all events from all files into memory (one file at a time)
        let mut heap: BinaryHeap<Reverse<CaidaHeapItem>> = BinaryHeap::new();
        let mut total_events: u64 = 0;
        let mut files_loaded: usize = 0;

        // First pass: count events per file to compute interleaved ordering
        let mut file_events: Vec<Vec<(u32, u32, u32)>> = Vec::with_capacity(num_files);

        for path in &paths {
            let mut events = Vec::new();
            let mut cf = CaidaFile::open(path.clone())?;
            while let Some(row) = cf.next_row()? {
                events.push(row);
            }
            // File handle is closed here when cf goes out of scope
            file_events.push(events);
            files_loaded += 1;

            if files_loaded % 100 == 0 {
                eprintln!(
                    "[CaidaEventSource] read {}/{} files",
                    files_loaded, num_files
                );
            }
        }

        // Build heap with interleaved ordering for round-robin effect
        // order_key = (seq_in_file * num_files + file_idx) gives round-robin order
        for (file_idx, events) in file_events.into_iter().enumerate() {
            for (seq, (src_ip, dst_ip, pkt_len)) in events.into_iter().enumerate() {
                let order_key = (seq as u64) * (num_files as u64) + (file_idx as u64);
                heap.push(Reverse(CaidaHeapItem {
                    order_key,
                    src_ip,
                    dst_ip,
                    pkt_len,
                }));
                total_events += 1;
            }
        }

        eprintln!(
            "[CaidaEventSource] preloaded {} events from {} files",
            total_events, num_files
        );

        anyhow::ensure!(
            !heap.is_empty(),
            "no rows found in CAIDA files under {}",
            caida_dir.display()
        );

        Ok(Self {
            heap,
            num_files,
            max_events,
            done: 0,
        })
    }

    /// Get the next event from the source.
    pub fn next_event(&mut self) -> anyhow::Result<Option<BenchEvent>> {
        if self.max_events > 0 && self.done >= self.max_events {
            return Ok(None);
        }
        let Some(Reverse(item)) = self.heap.pop() else {
            return Ok(None);
        };

        let ts = ts_for_event(self.done);
        // Encode src_ip and dst_ip into key: upper bytes for src_ip, lower for dst_ip
        let key_num = ((item.src_ip as u64) << 32) | (item.dst_ip as u64);
        let key_id = key_id_from_u64(key_num);

        self.done = self.done.saturating_add(1);
        Ok(Some(BenchEvent {
            ts,
            key_id,
            value: item.pkt_len,
        }))
    }

    /// Get the number of files loaded.
    pub fn num_files(&self) -> usize {
        self.num_files
    }
}

/// Convert a u64 to a 15-byte key_id (last 8 bytes).
pub fn key_id_from_u64(val: u64) -> [u8; KEY_BYTES_LEN] {
    let mut key = [0u8; KEY_BYTES_LEN];
    key[KEY_BYTES_LEN - 8..].copy_from_slice(&val.to_be_bytes());
    key
}

// ---------------------------------------------------------------------------
// Car emission dataset helpers
// ---------------------------------------------------------------------------

/// FNV-1a hash for encoding string fields into fixed-size bytes.
fn fnv1a_hash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Convert model year to Unix timestamp (Jan 1 00:00:00 UTC of that year).
fn model_year_to_unix_ts(year: u32) -> u32 {
    let mut days: u32 = 0;
    for y in 1970..year {
        if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
            days += 366;
        } else {
            days += 365;
        }
    }
    days * 86400
}

/// Encode car emission attributes into a 15-byte key.
///
/// Layout:
///   Bytes 0-3   (4 bytes): FNV-1a hash of Make (u32 BE)
///   Bytes 4-7   (4 bytes): FNV-1a hash of Model (u32 BE)
///   Byte  8     (1 byte):  FNV-1a hash of Vehicle class (u8)
///   Byte  9     (1 byte):  Engine size × 10 (u8), e.g. 2.0 L → 20
///   Byte  10    (1 byte):  Cylinders (u8)
///   Bytes 11-12 (2 bytes): FNV-1a hash of Transmission (u16 BE)
///   Byte  13    (1 byte):  Fuel type first ASCII byte
///   Byte  14    (1 byte):  0 (padding)
fn encode_car_emission_key(
    make: &str,
    model: &str,
    vehicle_class: &str,
    engine_size_l: f64,
    cylinders: u8,
    transmission: &str,
    fuel_type: &str,
) -> [u8; KEY_BYTES_LEN] {
    let mut key = [0u8; KEY_BYTES_LEN];
    let make_hash = fnv1a_hash(make.as_bytes()) as u32;
    let model_hash = fnv1a_hash(model.as_bytes()) as u32;
    let class_hash = fnv1a_hash(vehicle_class.as_bytes()) as u8;
    let engine_byte = (engine_size_l * 10.0).round().min(255.0) as u8;
    let trans_hash = fnv1a_hash(transmission.as_bytes()) as u16;
    let fuel_byte = fuel_type.as_bytes().first().copied().unwrap_or(0);

    key[0..4].copy_from_slice(&make_hash.to_be_bytes());
    key[4..8].copy_from_slice(&model_hash.to_be_bytes());
    key[8] = class_hash;
    key[9] = engine_byte;
    key[10] = cylinders;
    key[11..13].copy_from_slice(&trans_hash.to_be_bytes());
    key[13] = fuel_byte;
    key[14] = 0;
    key
}

/// Parse a single line from the car emission CSV.
///
/// CSV columns (0-indexed):
///   0: Model year, 1: Make, 2: Model, 3: Vehicle class, 4: Engine size (L),
///   5: Cylinders, 6: Transmission, 7: Fuel type, 8-11: fuel consumption,
///   12: CO2 emissions (g/km), 13: CO2 rating, 14: Smog rating
///
/// Returns `(model_year, scaled_co2_value, key_id)` or `None` on parse failure.
fn parse_car_emission_line(line: &str, value_scale: f64) -> Option<(u32, u64, [u8; KEY_BYTES_LEN])> {
    let s = line.trim();
    if s.is_empty() {
        return None;
    }
    let fields: Vec<&str> = s.split(',').collect();
    if fields.len() < 13 {
        return None;
    }

    let model_year: u32 = trim_quotes(fields[0]).parse().ok()?;
    let make = trim_quotes(fields[1]);
    let model = trim_quotes(fields[2]);
    let vehicle_class = trim_quotes(fields[3]);
    let engine_size: f64 = trim_quotes(fields[4]).parse().ok()?;
    let cylinders: u8 = trim_quotes(fields[5]).parse().ok()?;
    let transmission = trim_quotes(fields[6]);
    let fuel_type = trim_quotes(fields[7]);
    let co2_str = trim_quotes(fields[12]);
    let co2_f64: f64 = co2_str.parse().ok()?;

    let scaled = (co2_f64 * value_scale).round();
    if !scaled.is_finite() || scaled < 0.0 {
        return None;
    }

    let key_id = encode_car_emission_key(
        make,
        model,
        vehicle_class,
        engine_size,
        cylinders,
        transmission,
        fuel_type,
    );
    Some((model_year, scaled as u64, key_id))
}

/// Timestamp mode for car emission events.
#[derive(Copy, Clone, Debug)]
enum CarEmissionTsMode {
    /// Use model year → Unix timestamp of Jan 1 of that year (default).
    Year,
    /// Use current wall-clock time.
    Now,
    /// Sequential: base + interval × sequence number.
    Sequential,
}

/// Event source for car emission dataset.
///
/// Reads a single CSV file with vehicle CO2 emission data.
/// Uses model year as timestamp and CO2 emissions (g/km) as value.
/// Encodes Make, Model, Vehicle class, Engine size, Cylinders, Transmission,
/// and Fuel type into a 15-byte key.
pub struct CarEmissionEventSource {
    /// All events sorted by model year (timestamp).
    events: Vec<PreloadedEvent>,
    /// Current index into events.
    index: usize,
    max_events: u64,
    done: u64,
    ts_mode: CarEmissionTsMode,
    ts_base: u32,
    ts_interval: u32,
}

impl CarEmissionEventSource {
    /// Create a new car emission event source from a CSV file.
    pub fn new(
        csv_path: &Path,
        max_events: u64,
        ts_interval_ms: u64,
    ) -> anyhow::Result<Self> {
        let ts_mode = match env_string("TS_MODE").as_deref() {
            Some("now") => CarEmissionTsMode::Now,
            Some("default") => CarEmissionTsMode::Sequential,
            _ => CarEmissionTsMode::Year,
        };
        let value_scale = env_f64("EMISSION_VALUE_SCALE", 1.0);

        eprintln!(
            "[CarEmissionEventSource] loading from {} (value_scale={}, ts_mode={:?})",
            csv_path.display(),
            value_scale,
            ts_mode,
        );

        let f = fs::File::open(csv_path)
            .with_context(|| format!("open {}", csv_path.display()))?;
        let reader = BufReader::new(f);
        let mut events = Vec::new();
        let mut line_no = 0u64;

        for line_result in reader.lines() {
            let line = line_result
                .with_context(|| format!("read {}", csv_path.display()))?;
            line_no += 1;

            // Skip header line
            if line_no == 1 && line.contains("Model year") {
                continue;
            }

            if let Some((year, value, key_id)) = parse_car_emission_line(&line, value_scale) {
                let start_time = match ts_mode {
                    CarEmissionTsMode::Year => model_year_to_unix_ts(year) as u64,
                    _ => 0, // overridden in next_event
                };
                events.push(PreloadedEvent {
                    start_time,
                    value,
                    key_id,
                });
            }
        }

        // Sort by timestamp (model year)
        events.sort_by_key(|e| e.start_time);

        eprintln!(
            "[CarEmissionEventSource] loaded {} events from {} ({} lines)",
            events.len(),
            csv_path.display(),
            line_no,
        );

        anyhow::ensure!(
            !events.is_empty(),
            "no data rows found in {}",
            csv_path.display()
        );

        Ok(Self {
            events,
            index: 0,
            max_events,
            done: 0,
            ts_mode,
            ts_base: now_ts(),
            ts_interval: (ts_interval_ms / 1000).max(1) as u32,
        })
    }

    /// Get the next event from the source.
    pub fn next_event(&mut self) -> anyhow::Result<Option<BenchEvent>> {
        if self.max_events > 0 && self.done >= self.max_events {
            return Ok(None);
        }
        if self.index >= self.events.len() {
            return Ok(None);
        }

        let ev = &self.events[self.index];
        self.index += 1;

        let ts = match self.ts_mode {
            CarEmissionTsMode::Year => ev.start_time as u32,
            CarEmissionTsMode::Now => now_ts(),
            CarEmissionTsMode::Sequential => self
                .ts_base
                .saturating_add(self.ts_interval.saturating_mul(self.done as u32)),
        };

        self.done += 1;
        Ok(Some(BenchEvent {
            ts,
            key_id: ev.key_id,
            value: ev.value as u32,
        }))
    }
}

/// Synthetic event source for testing.
pub struct SyntheticEventSource {
    done: u64,
    max_events: u64,
}

impl SyntheticEventSource {
    /// Create a new synthetic event source.
    pub fn new(max_events: u64) -> Self {
        Self {
            done: 0,
            max_events,
        }
    }

    /// Get the next event using the provided RNG and parameters.
    pub fn next_event(
        &mut self,
        rng: &mut impl rand::Rng,
        key_mod: u64,
        value_mod: u32,
        source_id: u32,
    ) -> Option<BenchEvent> {
        if self.max_events > 0 && self.done >= self.max_events {
            return None;
        }

        let ts = now_ts();
        let key_index = rng.gen::<u64>() % key_mod;

        // Generate 15-byte key_id unique per source:
        // - Bytes 3-6 (4 bytes): source_id
        // - Bytes 7-14 (8 bytes): key_index
        let mut key_id = [0u8; KEY_BYTES_LEN];
        key_id[3..7].copy_from_slice(&source_id.to_be_bytes());
        key_id[KEY_BYTES_LEN - 8..].copy_from_slice(&key_index.to_be_bytes());

        let value = rng.gen::<u32>() % value_mod;

        self.done += 1;
        Some(BenchEvent { ts, key_id, value })
    }
}

/// Unified event source enum for different data types.
pub enum EventSource {
    Synthetic(SyntheticEventSource),
    Tsv(TsvEventSource),
    Caida(CaidaEventSource),
    CarEmission(CarEmissionEventSource),
}

impl EventSource {
    /// Create an event source based on BENCH_INPUT environment variable.
    pub fn from_env(max_events: u64) -> anyhow::Result<Self> {
        let bench_input = std::env::var("BENCH_INPUT").unwrap_or_else(|_| "synthetic".to_string());

        match bench_input.as_str() {
            "synthetic" => Ok(EventSource::Synthetic(SyntheticEventSource::new(max_events))),
            "tsv" | "google" | "google_cluster" | "google_cluster_data" => {
                let tsv_dir = std::env::var("TSV_DIR")
                    .map(PathBuf::from)
                    .with_context(|| "TSV_DIR is required for BENCH_INPUT=tsv")?;
                let max_files = env_u64("TSV_MAX_FILES", 64) as usize;
                let ts_interval_ms = env_u64("TS_INTERVAL_MS", 100);
                Ok(EventSource::Tsv(TsvEventSource::new(
                    &tsv_dir,
                    max_files,
                    max_events,
                    ts_interval_ms,
                )?))
            }
            "caida" | "caida_txt" => {
                let caida_dir = std::env::var("CAIDA_DIR")
                    .map(PathBuf::from)
                    .ok()
                    .or_else(default_caida_dir)
                    .with_context(|| "CAIDA_DIR is required for BENCH_INPUT=caida")?;
                let max_files = env_u64("CAIDA_MAX_FILES", 64) as usize;
                Ok(EventSource::Caida(CaidaEventSource::new(
                    &caida_dir,
                    max_files,
                    max_events,
                )?))
            }
            "car_emission" | "emission" => {
                let csv_path = std::env::var("EMISSION_CSV")
                    .map(PathBuf::from)
                    .with_context(|| "EMISSION_CSV is required for BENCH_INPUT=car_emission")?;
                let ts_interval_ms = env_u64("TS_INTERVAL_MS", 100);
                Ok(EventSource::CarEmission(CarEmissionEventSource::new(
                    &csv_path,
                    max_events,
                    ts_interval_ms,
                )?))
            }
            other => anyhow::bail!(
                "unsupported BENCH_INPUT={other}; expected synthetic, tsv/google, caida, or car_emission"
            ),
        }
    }

    /// Get the type name of this source.
    pub fn type_name(&self) -> &'static str {
        match self {
            EventSource::Synthetic(_) => "synthetic",
            EventSource::Tsv(_) => "tsv",
            EventSource::Caida(_) => "caida",
            EventSource::CarEmission(_) => "car_emission",
        }
    }
}
