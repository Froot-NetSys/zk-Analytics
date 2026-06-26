#![allow(clippy::needless_range_loop)]

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;
use core::ops::AddAssign;
use ff::{Field, PrimeField};
use halo2curves::pasta::Fq;

// This implements the same Poseidon permutation family as used by Nova's Poseidon sponge
// (ported from neptune), specialized to:
//   - Prime field = Pasta scalar field (Pallas scalar = Fq)
//   - arity = 2 (hash 2 field elements -> 1 field element)
//   - strength = Standard
//   - hash type = Sponge (domain tag = 0)
//
// It follows the Poseidon paper structure:
//   RF/2 full rounds, RP partial rounds, RF/2 full rounds
// where each round does:
//   add round constants (all elements),
//   apply S-box (all or first element),
//   mix with MDS matrix.

fn quintic_s_box<F: Field>(x: &mut F) {
    let mut tmp = *x;
    tmp = tmp.square(); // x^2
    tmp = tmp.square(); // x^4
    x.mul_assign(&tmp); // x^5
}

// Round numbers for t=3 (arity=2), base/standard strength with the same "security margin"
// behavior as nova-snark's `calc_round_numbers(t, true)`.
const POSEIDON_T: usize = 3;
const POSEIDON_RF: usize = 8;
const POSEIDON_RP: usize = 55;

// Round constants generation (Grain LFSR, self-shrinking), matching the Poseidon reference.
fn generate_constants<F: PrimeField>(
    field: u8,
    sbox: u8,
    field_size: u16,
    t: u16,
    r_f: u16,
    r_p: u16,
) -> Vec<F> {
    let n_bytes = F::Repr::default().as_ref().len();
    assert!(n_bytes == 32, "expected 32-byte field repr");
    let num_constants = (r_f + r_p) * t;

    let mut init_sequence: Vec<bool> = Vec::new();
    append_bits(&mut init_sequence, 2, field as u128); // Bits 0-1
    append_bits(&mut init_sequence, 4, sbox as u128); // Bits 2-5
    append_bits(&mut init_sequence, 12, field_size as u128); // Bits 6-17
    append_bits(&mut init_sequence, 12, t as u128); // Bits 18-29
    append_bits(&mut init_sequence, 10, r_f as u128); // Bits 30-39
    append_bits(&mut init_sequence, 10, r_p as u128); // Bits 40-49
    append_bits(
        &mut init_sequence,
        30,
        0b111111111111111111111111111111u128, // Bits 50-79
    );

    let mut grain = Grain::new(init_sequence, field_size);
    let mut round_constants: Vec<F> = Vec::new();
    match field {
        1 => {
            for _ in 0..num_constants {
                loop {
                    let mut repr = F::Repr::default();
                    grain.get_next_bytes(repr.as_mut());
                    repr.as_mut().reverse(); // interpret as big-endian integer
                    if let Some(f) = F::from_repr_vartime(repr) {
                        round_constants.push(f);
                        break;
                    }
                }
            }
        }
        _ => panic!("only prime fields supported"),
    }
    round_constants
}

fn append_bits(vec: &mut Vec<bool>, n: usize, val: u128) {
    for i in (0..n).rev() {
        vec.push(((val >> i) & 1) != 0);
    }
}

struct Grain {
    state: Vec<bool>,
    field_size: u16,
}

impl Grain {
    fn new(init_sequence: Vec<bool>, field_size: u16) -> Self {
        assert_eq!(80, init_sequence.len());
        let mut g = Grain {
            state: init_sequence,
            field_size,
        };
        for _ in 0..160 {
            g.generate_new_bit();
        }
        assert_eq!(80, g.state.len());
        g
    }

    fn bit(&self, index: usize) -> bool {
        self.state[index]
    }

    fn generate_new_bit(&mut self) -> bool {
        let new_bit =
            self.bit(62) ^ self.bit(51) ^ self.bit(38) ^ self.bit(23) ^ self.bit(13) ^ self.bit(0);
        self.state.remove(0);
        self.state.push(new_bit);
        new_bit
    }

    fn take_bits(&mut self, bit_count: usize) -> Vec<bool> {
        let mut out = Vec::with_capacity(bit_count);
        for _ in 0..bit_count {
            out.push(self.next().unwrap_or(false));
        }
        out
    }

    fn next_byte(&mut self, bit_count: usize) -> u8 {
        let mut acc: u8 = 0;
        for bit in self.take_bits(bit_count) {
            acc <<= 1;
            if bit {
                acc = acc.wrapping_add(1);
            }
        }
        acc
    }

    fn get_next_bytes(&mut self, result: &mut [u8]) {
        let remainder_bits = self.field_size as usize % 8;
        if remainder_bits > 0 {
            result[0] = self.next_byte(remainder_bits);
        } else {
            result[0] = self.next_byte(8);
        }
        for item in result.iter_mut().skip(1) {
            *item = self.next_byte(8);
        }
    }
}

impl Iterator for Grain {
    type Item = bool;
    fn next(&mut self) -> Option<Self::Item> {
        let mut new_bit = self.generate_new_bit();
        while !new_bit {
            let _ = self.generate_new_bit();
            new_bit = self.generate_new_bit();
        }
        new_bit = self.generate_new_bit();
        Some(new_bit)
    }
}

fn generate_mds<F: PrimeField>(t: usize) -> Vec<Vec<F>> {
    let xs: Vec<F> = (0..t as u64).map(F::from).collect();
    let ys: Vec<F> = (t as u64..2 * t as u64).map(F::from).collect();
    xs.iter()
        .map(|x| {
            ys.iter()
                .map(|y| {
                    let mut tmp = *x;
                    tmp.add_assign(y);
                    tmp.invert().unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

fn mds_mul<F: Field>(m: &[Vec<F>], v: &[F]) -> Vec<F> {
    let t = v.len();
    let mut out = vec![F::ZERO; t];
    for i in 0..t {
        let mut acc = F::ZERO;
        for j in 0..t {
            let mut tmp = m[i][j];
            tmp.mul_assign(&v[j]);
            acc.add_assign(&tmp);
        }
        out[i] = acc;
    }
    out
}

#[derive(Clone, Debug)]
pub struct PoseidonPastaParams {
    t: usize,
    rf: usize,
    rp: usize,
    mds: Vec<Vec<Fq>>,
    round_constants: Vec<Fq>,
}

impl PoseidonPastaParams {
    pub fn new() -> Self {
        let t = POSEIDON_T;
        let rf = POSEIDON_RF;
        let rp = POSEIDON_RP;
        let mds = generate_mds::<Fq>(t);
        let field_size = u16::try_from(Fq::NUM_BITS).unwrap_or(256);
        let round_constants =
            generate_constants::<Fq>(1, 1, field_size, t as u16, rf as u16, rp as u16);
        Self {
            t,
            rf,
            rp,
            mds,
            round_constants,
        }
    }

    pub fn hash2(&self, a: Fq, b: Fq) -> Fq {
        let mut state = vec![Fq::ZERO; self.t];
        state[0] = Fq::ZERO; // HashType::Sponge domain tag
        state[1] = a;
        state[2] = b;

        let half_full = self.rf / 2;
        let rounds_total = self.rf + self.rp;
        for r in 0..rounds_total {
            for i in 0..self.t {
                let rc = self.round_constants[r * self.t + i];
                state[i].add_assign(&rc);
            }

            if r < half_full || r >= half_full + self.rp {
                for i in 0..self.t {
                    quintic_s_box(&mut state[i]);
                }
            } else {
                quintic_s_box(&mut state[0]);
            }

            state = mds_mul(&self.mds, &state);
        }

        state[1]
    }
}

pub fn bytes32_le_to_scalar(bytes: &[u8; 32]) -> Fq {
    let mut repr = <Fq as PrimeField>::Repr::default();
    let repr_bytes = repr.as_mut();
    repr_bytes[..32].copy_from_slice(bytes);
    Fq::from_repr(repr).unwrap_or(Fq::ZERO)
}

pub fn chunk31_le_to_scalar(chunk: &[u8; 31]) -> Fq {
    let mut repr = <Fq as PrimeField>::Repr::default();
    let repr_bytes = repr.as_mut();
    repr_bytes[..31].copy_from_slice(chunk);
    Fq::from_repr(repr).unwrap_or(Fq::ZERO)
}

pub fn scalar_to_bytes32_le(x: &Fq) -> [u8; 32] {
    let mut out = [0u8; 32];
    let repr = x.to_repr();
    let bytes = repr.as_ref();
    out.copy_from_slice(&bytes[..32]);
    out
}
