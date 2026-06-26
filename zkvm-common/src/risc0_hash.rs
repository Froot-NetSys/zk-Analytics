use risc0_core::field::baby_bear::BabyBearElem;
use risc0_zkp::core::digest::{Digest, DIGEST_BYTES, DIGEST_WORDS};
pub use risc0_zkp::core::hash::{hash_suite_from_name, poseidon2::Poseidon2HashSuite};

pub fn digest_from_bytes32_le(bytes: [u8; DIGEST_BYTES]) -> Digest {
    let mut words = [0u32; DIGEST_WORDS];
    for (idx, word) in words.iter_mut().enumerate() {
        let start = idx * 4;
        let end = start + 4;
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&bytes[start..end]);
        *word = u32::from_le_bytes(buf);
    }
    Digest::from(words)
}

pub fn digest_to_bytes32_le(digest: &Digest) -> [u8; DIGEST_BYTES] {
    let mut out = [0u8; DIGEST_BYTES];
    for (idx, word) in digest.as_words().iter().enumerate() {
        let start = idx * 4;
        let end = start + 4;
        out[start..end].copy_from_slice(&word.to_le_bytes());
    }
    out
}

fn digest_from_bytes32_le_reduced(bytes: [u8; DIGEST_BYTES]) -> Digest {
    let mut digest = digest_from_bytes32_le(bytes);
    for word in digest.as_mut_words() {
        let elem = BabyBearElem::new(*word);
        *word = elem.as_u32_montgomery();
    }
    digest
}

pub fn poseidon2_hash_pair_bytes32_le(
    a: [u8; DIGEST_BYTES],
    b: [u8; DIGEST_BYTES],
) -> [u8; DIGEST_BYTES] {
    let suite = Poseidon2HashSuite::new_suite();
    let a = digest_from_bytes32_le(a);
    // Treat the second input as raw bytes and reduce into the BabyBear field.
    let b = digest_from_bytes32_le_reduced(b);
    let out = *suite.hashfn.hash_pair(&a, &b);
    digest_to_bytes32_le(&out)
}

pub fn hash_pair_bytes32_le(
    hashfn_name: &str,
    a: [u8; DIGEST_BYTES],
    b: [u8; DIGEST_BYTES],
) -> Option<[u8; DIGEST_BYTES]> {
    let suite = hash_suite_from_name(hashfn_name)?;
    let a = digest_from_bytes32_le(a);
    let b = digest_from_bytes32_le(b);
    let out = *suite.hashfn.hash_pair(&a, &b);
    Some(digest_to_bytes32_le(&out))
}
