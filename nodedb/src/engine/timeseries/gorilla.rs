//! Gorilla XOR encoding for floating-point timeseries metrics.
//!
//! Implements the Facebook Gorilla paper's XOR-based compression for
//! double-precision floating-point values. Achieves ~1.5 bytes per
//! 16-byte (timestamp + value) sample by exploiting temporal locality
//! in metric streams.
//!
//! Reference: "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//! (Pelkonen et al., VLDB 2015)

/// A Gorilla XOR encoder that compresses a stream of (timestamp, f64) samples.
///
/// Timestamps use delta-of-delta encoding. Values use XOR with leading/trailing
/// zero compression.
#[derive(Debug)]
pub struct GorillaEncoder {
    /// Compressed output buffer.
    buf: Vec<u8>,
    /// Current bit position within the buffer.
    bit_pos: usize,
    /// Previous timestamp.
    prev_ts: i64,
    /// Previous delta (for delta-of-delta).
    prev_delta: i64,
    /// Previous value as raw bits.
    prev_value: u64,
    /// Previous leading zeros count.
    prev_leading: u8,
    /// Previous trailing zeros count.
    prev_trailing: u8,
    /// Number of samples encoded.
    count: u64,
}

impl GorillaEncoder {
    /// Create a new encoder.
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
            bit_pos: 0,
            prev_ts: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: u8::MAX,
            prev_trailing: 0,
            count: 0,
        }
    }

    /// Encode a (timestamp_ms, value) sample.
    pub fn encode(&mut self, timestamp_ms: i64, value: f64) {
        let value_bits = value.to_bits();

        if self.count == 0 {
            // First sample: write raw timestamp (64 bits) and value (64 bits).
            self.write_bits(timestamp_ms as u64, 64);
            self.write_bits(value_bits, 64);
            self.prev_ts = timestamp_ms;
            self.prev_value = value_bits;
            self.count = 1;
            return;
        }

        // --- Timestamp: delta-of-delta encoding ---
        let delta = timestamp_ms - self.prev_ts;
        let dod = delta - self.prev_delta;
        self.encode_timestamp_dod(dod);
        self.prev_ts = timestamp_ms;
        self.prev_delta = delta;

        // --- Value: XOR encoding ---
        let xor = self.prev_value ^ value_bits;
        self.encode_value_xor(xor);
        self.prev_value = value_bits;

        self.count += 1;
    }

    fn encode_timestamp_dod(&mut self, dod: i64) {
        if dod == 0 {
            // '0' — same delta as before.
            self.write_bit(false);
        } else if (-63..=64).contains(&dod) {
            // '10' + 7-bit value.
            self.write_bits(0b10, 2);
            self.write_bits((dod as u64) & 0x7F, 7);
        } else if (-255..=256).contains(&dod) {
            // '110' + 9-bit value.
            self.write_bits(0b110, 3);
            self.write_bits((dod as u64) & 0x1FF, 9);
        } else if (-2047..=2048).contains(&dod) {
            // '1110' + 12-bit value.
            self.write_bits(0b1110, 4);
            self.write_bits((dod as u64) & 0xFFF, 12);
        } else {
            // '1111' + 64-bit raw value.
            self.write_bits(0b1111, 4);
            self.write_bits(dod as u64, 64);
        }
    }

    fn encode_value_xor(&mut self, xor: u64) {
        if xor == 0 {
            // '0' — value unchanged.
            self.write_bit(false);
            return;
        }

        // '1' — value changed.
        self.write_bit(true);

        let leading = xor.leading_zeros() as u8;
        let trailing = xor.trailing_zeros() as u8;

        if self.prev_leading != u8::MAX
            && leading >= self.prev_leading
            && trailing >= self.prev_trailing
        {
            // '0' — fits within previous window.
            self.write_bit(false);
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            self.write_bits(xor >> self.prev_trailing, meaningful_bits as usize);
        } else {
            // '1' — new window.
            self.write_bit(true);
            // 6 bits for leading zeros count (0-63).
            self.write_bits(leading as u64, 6);
            let meaningful_bits = 64 - leading - trailing;
            // 6 bits for meaningful bits length (1-64, stored as 0-63).
            self.write_bits((meaningful_bits - 1) as u64, 6);
            self.write_bits(xor >> trailing, meaningful_bits as usize);
            self.prev_leading = leading;
            self.prev_trailing = trailing;
        }
    }

    /// Finish encoding and return the compressed bytes.
    ///
    /// Prepends a 4-byte little-endian sample count header.
    pub fn finish(self) -> Vec<u8> {
        let count_bytes = (self.count as u32).to_le_bytes();
        let mut out = Vec::with_capacity(4 + self.buf.len());
        out.extend_from_slice(&count_bytes);
        out.extend_from_slice(&self.buf);
        out
    }

    /// Number of samples encoded so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Current compressed size in bytes.
    pub fn compressed_size(&self) -> usize {
        self.bit_pos.div_ceil(8)
    }

    fn write_bit(&mut self, bit: bool) {
        let byte_idx = self.bit_pos / 8;
        let bit_idx = 7 - (self.bit_pos % 8);

        if byte_idx >= self.buf.len() {
            self.buf.push(0);
        }

        if bit {
            self.buf[byte_idx] |= 1 << bit_idx;
        }

        self.bit_pos += 1;
    }

    fn write_bits(&mut self, value: u64, num_bits: usize) {
        for i in (0..num_bits).rev() {
            self.write_bit((value >> i) & 1 == 1);
        }
    }
}

impl Default for GorillaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// A Gorilla XOR decoder that decompresses a stream of (timestamp, f64) samples.
pub struct GorillaDecoder<'a> {
    buf: &'a [u8],
    bit_pos: usize,
    prev_ts: i64,
    prev_delta: i64,
    prev_value: u64,
    prev_leading: u8,
    prev_trailing: u8,
    count: u64,
    total: u64,
    first: bool,
}

impl<'a> GorillaDecoder<'a> {
    /// Create a decoder from compressed bytes.
    ///
    /// Expects a 4-byte little-endian sample count header followed by the bitstream.
    pub fn new(buf: &'a [u8]) -> Self {
        if buf.len() < 4 {
            return Self {
                buf: &[],
                bit_pos: 0,
                prev_ts: 0,
                prev_delta: 0,
                prev_value: 0,
                prev_leading: 0,
                prev_trailing: 0,
                count: 0,
                total: 0,
                first: true,
            };
        }
        // SAFETY: We checked buf.len() >= 4 above, so this slice is exactly 4 bytes.
        let total = u32::from_le_bytes(buf[0..4].try_into().unwrap_or([0, 0, 0, 0])) as u64;
        Self {
            buf: &buf[4..],
            bit_pos: 0,
            prev_ts: 0,
            prev_delta: 0,
            prev_value: 0,
            prev_leading: 0,
            prev_trailing: 0,
            count: 0,
            total,
            first: true,
        }
    }

    /// Decode the next sample, or None if all samples decoded.
    pub fn next_sample(&mut self) -> Option<(i64, f64)> {
        if self.count >= self.total {
            return None;
        }

        if self.first {
            self.first = false;
            let ts = self.read_bits(64)? as i64;
            let val = self.read_bits(64)?;
            self.prev_ts = ts;
            self.prev_value = val;
            self.count = 1;
            return Some((ts, f64::from_bits(val)));
        }

        // --- Timestamp ---
        let ts = self.decode_timestamp()?;
        // --- Value ---
        let val = self.decode_value()?;

        self.count += 1;
        Some((ts, f64::from_bits(val)))
    }

    fn decode_timestamp(&mut self) -> Option<i64> {
        let bit = self.read_bit()?;
        let dod = if !bit {
            0i64
        } else {
            let bit2 = self.read_bit()?;
            if !bit2 {
                // '10' + 7-bit signed.
                let raw = self.read_bits(7)? as i64;
                sign_extend(raw, 7)
            } else {
                let bit3 = self.read_bit()?;
                if !bit3 {
                    // '110' + 9-bit signed.
                    let raw = self.read_bits(9)? as i64;
                    sign_extend(raw, 9)
                } else {
                    let bit4 = self.read_bit()?;
                    if !bit4 {
                        // '1110' + 12-bit signed.
                        let raw = self.read_bits(12)? as i64;
                        sign_extend(raw, 12)
                    } else {
                        // '1111' + 64-bit raw.
                        self.read_bits(64)? as i64
                    }
                }
            }
        };

        let delta = self.prev_delta + dod;
        let ts = self.prev_ts + delta;
        self.prev_ts = ts;
        self.prev_delta = delta;
        Some(ts)
    }

    fn decode_value(&mut self) -> Option<u64> {
        let bit = self.read_bit()?;
        if !bit {
            // Value unchanged.
            return Some(self.prev_value);
        }

        let bit2 = self.read_bit()?;
        let xor = if !bit2 {
            // Reuse previous window.
            let meaningful_bits = 64 - self.prev_leading - self.prev_trailing;
            let bits = self.read_bits(meaningful_bits as usize)?;
            bits << self.prev_trailing
        } else {
            // New window.
            let leading = self.read_bits(6)? as u8;
            let meaningful_bits = self.read_bits(6)? as u8 + 1;
            let trailing = 64 - leading - meaningful_bits;
            let bits = self.read_bits(meaningful_bits as usize)?;
            self.prev_leading = leading;
            self.prev_trailing = trailing;
            bits << trailing
        };

        let val = self.prev_value ^ xor;
        self.prev_value = val;
        Some(val)
    }

    fn read_bit(&mut self) -> Option<bool> {
        let byte_idx = self.bit_pos / 8;
        if byte_idx >= self.buf.len() {
            return None;
        }
        let bit_idx = 7 - (self.bit_pos % 8);
        let bit = (self.buf[byte_idx] >> bit_idx) & 1 == 1;
        self.bit_pos += 1;
        Some(bit)
    }

    fn read_bits(&mut self, num_bits: usize) -> Option<u64> {
        let mut value = 0u64;
        for _ in 0..num_bits {
            value = (value << 1) | if self.read_bit()? { 1 } else { 0 };
        }
        Some(value)
    }

    /// Decode all remaining samples into a Vec.
    pub fn decode_all(&mut self) -> Vec<(i64, f64)> {
        let mut samples = Vec::new();
        while let Some(s) = self.next_sample() {
            samples.push(s);
        }
        samples
    }
}

/// Sign-extend a value from `bits` width to i64.
fn sign_extend(value: i64, bits: u32) -> i64 {
    let shift = 64 - bits;
    (value << shift) >> shift
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_encoder() {
        let enc = GorillaEncoder::new();
        assert_eq!(enc.count(), 0);
        let data = enc.finish();
        // 4-byte count header with count=0.
        assert_eq!(data.len(), 4);
        assert_eq!(u32::from_le_bytes(data[0..4].try_into().unwrap()), 0);
    }

    #[test]
    fn single_sample_roundtrip() {
        let mut enc = GorillaEncoder::new();
        enc.encode(1000, 42.5);
        let data = enc.finish();

        let mut dec = GorillaDecoder::new(&data);
        let (ts, val) = dec.next_sample().unwrap();
        assert_eq!(ts, 1000);
        assert!((val - 42.5).abs() < f64::EPSILON);
        assert!(dec.next_sample().is_none());
    }

    #[test]
    fn monotonic_timestamps_compress_well() {
        let mut enc = GorillaEncoder::new();
        // Uniform 10-second intervals — delta-of-delta is always 0 after first pair.
        for i in 0..1000 {
            enc.encode(1_000_000 + i * 10_000, 100.0 + (i as f64) * 0.001);
        }
        let data = enc.finish();

        // 1000 samples × 16 bytes raw = 16000 bytes.
        // With incrementing values (changing mantissa), expect ~6 bytes/sample.
        assert!(
            data.len() < 8000,
            "expected good compression, got {} bytes for 1000 samples",
            data.len()
        );

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), 1000);
        assert_eq!(samples[0].0, 1_000_000);
        assert!((samples[0].1 - 100.0).abs() < f64::EPSILON);
        assert_eq!(samples[999].0, 1_000_000 + 999 * 10_000);
    }

    #[test]
    fn identical_values_compress_to_minimum() {
        let mut enc = GorillaEncoder::new();
        for i in 0..100 {
            enc.encode(1000 + i * 1000, 42.0);
        }
        let data = enc.finish();

        // Identical values: each subsequent sample is just 1 bit (timestamp dod=0)
        // + 1 bit (value unchanged) = 2 bits per sample after first two.
        // Should be very compact.
        assert!(
            data.len() < 100,
            "identical values should compress extremely well, got {} bytes",
            data.len()
        );

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), 100);
        for s in &samples {
            assert!((s.1 - 42.0).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn varying_values_roundtrip() {
        let mut enc = GorillaEncoder::new();
        let test_values = [
            0.0,
            1.0,
            -1.0,
            f64::MAX,
            f64::MIN,
            std::f64::consts::PI,
            1e-300,
            1e300,
        ];
        for (i, &val) in test_values.iter().enumerate() {
            enc.encode(i as i64 * 1000, val);
        }
        let data = enc.finish();

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), test_values.len());
        for (i, &expected) in test_values.iter().enumerate() {
            assert_eq!(
                samples[i].1.to_bits(),
                expected.to_bits(),
                "mismatch at index {i}"
            );
        }
    }

    #[test]
    fn irregular_timestamps_roundtrip() {
        let mut enc = GorillaEncoder::new();
        let timestamps = [100, 200, 350, 400, 1000, 1001, 5000];
        for (i, &ts) in timestamps.iter().enumerate() {
            enc.encode(ts, i as f64);
        }
        let data = enc.finish();

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), timestamps.len());
        for (i, &expected_ts) in timestamps.iter().enumerate() {
            assert_eq!(samples[i].0, expected_ts);
        }
    }

    #[test]
    fn compression_ratio_metric_workload() {
        // Simulate a realistic CPU usage metric: values oscillate around 50% with small jitter.
        let mut enc = GorillaEncoder::new();
        let mut rng_state: u64 = 12345;
        for i in 0..10_000 {
            // Simple LCG for deterministic "jitter".
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let jitter = ((rng_state >> 33) as f64) / (u32::MAX as f64) * 2.0 - 1.0;
            let value = 50.0 + jitter * 5.0;
            enc.encode(1_700_000_000_000 + i * 10_000, value);
        }
        let data = enc.finish();

        let raw_size = 10_000 * 16; // 160 KB
        let ratio = raw_size as f64 / data.len() as f64;
        // Real-world metrics with smooth values get ~12:1.
        // Synthetic jittery values get ~2-3:1 due to mantissa changes.
        assert!(
            ratio > 2.0,
            "compression ratio {ratio:.1}:1 is too low (expected >2:1)"
        );

        let mut dec = GorillaDecoder::new(&data);
        let samples = dec.decode_all();
        assert_eq!(samples.len(), 10_000);
    }
}
