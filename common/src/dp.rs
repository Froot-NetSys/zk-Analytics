use rand::Rng;

#[derive(Clone, Copy, Debug)]
pub struct DpConfig {
    pub epsilon: f64,
    pub sensitivity: f64,
    pub b: u64,
}

impl DpConfig {
    /// Generate Laplace noise for differential privacy.
    /// Returns a random sample from Laplace(0, sensitivity/epsilon).
    ///
    /// The Laplace distribution is sampled using the inverse CDF method:
    /// X = -scale * sign(U - 0.5) * ln(1 - 2|U - 0.5|) where U ~ Uniform(0, 1)
    pub fn laplace_noise<R: Rng>(&self, rng: &mut R) -> f64 {
        let scale = self.sensitivity / self.epsilon;
        // Sample from Laplace(0, scale) using inverse CDF
        let u: f64 = rng.gen::<f64>() - 0.5; // U in (-0.5, 0.5)
        // X = -scale * sign(u) * ln(1 - 2|u|)
        let abs_u = u.abs();
        if abs_u >= 0.5 {
            // Edge case: u very close to +-0.5, return large magnitude noise
            return if u >= 0.0 { scale * 10.0 } else { -scale * 10.0 };
        }
        let log_term = (1.0 - 2.0 * abs_u).ln();
        -scale * u.signum() * log_term
    }

    /// Apply Laplace noise to a u64 value, clamping to non-negative.
    /// Returns the noisy value and the actual noise applied.
    pub fn apply_noise_u64<R: Rng>(&self, value: u64, rng: &mut R) -> (u64, i64) {
        let noise = self.laplace_noise(rng);
        let noise_i64 = noise.round() as i64;
        let noisy_value = if noise_i64 >= 0 {
            value.saturating_add(noise_i64 as u64)
        } else {
            value.saturating_sub((-noise_i64) as u64)
        };
        (noisy_value, noise_i64)
    }

    /// Apply Laplace noise to an i64 value.
    /// Returns the noisy value and the actual noise applied.
    pub fn apply_noise_i64<R: Rng>(&self, value: i64, rng: &mut R) -> (i64, i64) {
        let noise = self.laplace_noise(rng);
        let noise_i64 = noise.round() as i64;
        (value.saturating_add(noise_i64), noise_i64)
    }
}

pub const DP_L: f64 = 1_000_000.0;
pub const DP_L2: f64 = DP_L * DP_L;

pub const DP_CM_ESTIMATE: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_CM_TOPK: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_HIST_BUCKET: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_HIST_TOTAL_COUNT: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_HIST_TOTAL_SUM: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L,
    b: 50,
};
pub const DP_HIST_BUCKET_COUNT: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_SAMPLES_SUM: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L,
    b: 50,
};
pub const DP_SAMPLES_COUNT: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_SAMPLES_AVG: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L,
    b: 10,
};
pub const DP_SAMPLES_SUM_KEY: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L,
    b: 50,
};
pub const DP_SAMPLES_COUNT_KEY: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_RAW_MAX: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L,
    b: 50,
};
pub const DP_RAW_HIST_BUCKET: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_RAW_CM_ESTIMATE: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_RAW_STATS_COUNT: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_RAW_STATS_SUM: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L,
    b: 50,
};
pub const DP_RAW_STATS_SUMSQ: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: DP_L2,
    b: 100,
};
pub const DP_HIST_PER_KEY_BUCKET: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
pub const DP_CM_PER_KEY_ESTIMATE: DpConfig = DpConfig {
    epsilon: 1.0,
    sensitivity: 1.0,
    b: 10,
};
