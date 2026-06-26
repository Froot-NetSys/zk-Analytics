#![no_std]

extern crate alloc;

use serde::{Deserialize, Serialize};

pub mod poseidon_pasta;
#[cfg(feature = "risc0-hash")]
pub mod risc0_hash;

pub const KEY_BYTES_LEN: usize = 15;
pub const VALUE_BYTES_LEN: usize = 4;
pub const TS_BYTES_LEN: usize = 4;
pub const EVENT_BYTES_LEN: usize = TS_BYTES_LEN + KEY_BYTES_LEN + VALUE_BYTES_LEN; // 23 bytes: ts(4) + key(15) + value(4)
pub const HASH_BYTES_LEN: usize = 32;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChainHashFn {
    Sha256,
    Poseidon2,
}

impl Default for ChainHashFn {
    fn default() -> Self {
        Self::Sha256
    }
}

impl ChainHashFn {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sha256 => "sha256",
            Self::Poseidon2 => "poseidon2",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "sha256" | "sha-256" => Some(Self::Sha256),
            "poseidon2" | "poseidon-2" => Some(Self::Poseidon2),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub ts: u32,        // Timestamp in seconds (32 bits)
    pub key_id: [u8; KEY_BYTES_LEN],  // 15 bytes key identifier
    pub value: u32,     // Value (32 bits)
}

impl Event {
    /// Serialize event to bytes: ts(4) + key_id(15) + value(4) = 23 bytes
    pub fn to_bytes_be(&self) -> [u8; EVENT_BYTES_LEN] {
        let mut out = [0u8; EVENT_BYTES_LEN];
        out[..TS_BYTES_LEN].copy_from_slice(&self.ts.to_be_bytes());
        out[TS_BYTES_LEN..TS_BYTES_LEN + KEY_BYTES_LEN].copy_from_slice(&self.key_id);
        out[TS_BYTES_LEN + KEY_BYTES_LEN..].copy_from_slice(&self.value.to_be_bytes());
        out
    }

    /// Helper to get key_id as u64 (using all 15 bytes via FNV-1a mixing)
    pub fn key_id_u64(&self) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis
        for &byte in self.key_id.iter() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3); // FNV prime
        }
        hash
    }

    /// Helper to create key_id from u64 (stores in lower 8 bytes, upper 7 bytes zeroed)
    pub fn key_id_from_u64(val: u64) -> [u8; KEY_BYTES_LEN] {
        let mut key = [0u8; KEY_BYTES_LEN];
        key[KEY_BYTES_LEN - 8..].copy_from_slice(&val.to_be_bytes());
        key
    }

    /// Create a key_id embedding both source_id and key_index.
    /// Matches querier benchmark approach: source_id in bytes [3..7], key_index in bytes [7..15]
    pub fn make_key_id(source_id: u32, key_index: u64) -> [u8; KEY_BYTES_LEN] {
        let mut key_id = [0u8; KEY_BYTES_LEN];
        key_id[3..7].copy_from_slice(&source_id.to_be_bytes());
        key_id[15 - 8..].copy_from_slice(&key_index.to_be_bytes());
        key_id
    }

    /// Extract source_id from a key_id created by make_key_id.
    /// Returns the source_id embedded in bytes [3..7].
    pub fn extract_source_id(key_id: &[u8; KEY_BYTES_LEN]) -> u32 {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&key_id[3..7]);
        u32::from_be_bytes(bytes)
    }

    /// Extract key_index from a key_id created by make_key_id.
    /// Returns the key_index embedded in bytes [7..15].
    pub fn extract_key_index(key_id: &[u8; KEY_BYTES_LEN]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key_id[7..15]);
        u64::from_be_bytes(bytes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainInput {
    pub prev_hash: [u8; HASH_BYTES_LEN],
    #[serde(default)]
    pub hash_fn: ChainHashFn,
    pub events: alloc::vec::Vec<Event>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChainOutput {
    pub final_hash: [u8; HASH_BYTES_LEN],
    pub n_events: u64,
}
