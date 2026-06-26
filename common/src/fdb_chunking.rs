//! Chunking utilities for FoundationDB large value storage.
//!
//! FDB has a 100KB value limit. This module provides utilities to split
//! large values into chunks and reassemble them.

/// Maximum chunk size (90KB to leave headroom under FDB's 100KB limit)
pub const FDB_CHUNK_SIZE: usize = 90_000;

/// Split a byte slice into chunks suitable for FDB storage.
/// Returns (chunk_index, chunk_data) pairs.
pub fn chunk_bytes(data: &[u8]) -> Vec<(u32, Vec<u8>)> {
    if data.is_empty() {
        return vec![(0, Vec::new())];
    }
    data.chunks(FDB_CHUNK_SIZE)
        .enumerate()
        .map(|(i, chunk)| (i as u32, chunk.to_vec()))
        .collect()
}

/// Reassemble chunks into the original byte slice.
/// Chunks must be provided as (chunk_index, chunk_data) pairs.
/// They will be sorted by index before reassembly.
pub fn reassemble_chunks(chunks: &[(u32, Vec<u8>)]) -> Vec<u8> {
    if chunks.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<_> = chunks.to_vec();
    sorted.sort_by_key(|(idx, _)| *idx);
    sorted.into_iter().flat_map(|(_, data)| data).collect()
}

/// Check if data needs chunking (exceeds FDB limit)
pub fn needs_chunking(data: &[u8]) -> bool {
    data.len() > FDB_CHUNK_SIZE
}

/// Calculate number of chunks needed for data
pub fn chunk_count(data: &[u8]) -> u32 {
    if data.is_empty() {
        return 1;
    }
    ((data.len() + FDB_CHUNK_SIZE - 1) / FDB_CHUNK_SIZE) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_data() {
        let chunks = chunk_bytes(&[]);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], (0, Vec::new()));

        let reassembled = reassemble_chunks(&chunks);
        assert!(reassembled.is_empty());
    }

    #[test]
    fn test_small_data() {
        let data = vec![1, 2, 3, 4, 5];
        let chunks = chunk_bytes(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, 0);
        assert_eq!(chunks[0].1, data);

        let reassembled = reassemble_chunks(&chunks);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_large_data() {
        let data: Vec<u8> = (0..200_000).map(|i| (i % 256) as u8).collect();
        let chunks = chunk_bytes(&data);
        assert_eq!(chunks.len(), 3); // 200KB / 90KB = 3 chunks

        let reassembled = reassemble_chunks(&chunks);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_exact_chunk_size() {
        let data: Vec<u8> = vec![42; FDB_CHUNK_SIZE];
        let chunks = chunk_bytes(&data);
        assert_eq!(chunks.len(), 1);

        let reassembled = reassemble_chunks(&chunks);
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_chunk_count() {
        assert_eq!(chunk_count(&[]), 1);
        assert_eq!(chunk_count(&[1, 2, 3]), 1);
        assert_eq!(chunk_count(&vec![0; FDB_CHUNK_SIZE]), 1);
        assert_eq!(chunk_count(&vec![0; FDB_CHUNK_SIZE + 1]), 2);
        assert_eq!(chunk_count(&vec![0; FDB_CHUNK_SIZE * 2]), 2);
        assert_eq!(chunk_count(&vec![0; FDB_CHUNK_SIZE * 2 + 1]), 3);
    }
}
