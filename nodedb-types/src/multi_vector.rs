//! Multi-vector type for ColBERT-style late interaction retrieval.
//!
//! Stores N vectors per document (one per token/passage), all same dimension.
//! Contiguous `Vec<f32>` layout for cache efficiency: `[v0_d0..v0_dD, v1_d0..v1_dD, ...]`.

use serde::{Deserialize, Serialize};

/// A multi-vector: an array of dense vectors, all sharing the same dimension.
///
/// Used for ColBERT/ColPali-style late interaction where each document
/// produces one embedding per token. All vectors share a doc_id and are
/// inserted as separate HNSW nodes with shared doc_id metadata.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct MultiVector {
    /// Contiguous f32 data: `count × dim` elements.
    data: Vec<f32>,
    /// Number of vectors.
    count: usize,
    /// Dimensionality of each vector.
    dim: usize,
}

impl MultiVector {
    /// Create from a list of vectors. All must have the same dimension.
    pub fn from_vectors(vectors: Vec<Vec<f32>>) -> Result<Self, MultiVectorError> {
        if vectors.is_empty() {
            return Err(MultiVectorError::Empty);
        }
        let dim = vectors[0].len();
        if dim == 0 {
            return Err(MultiVectorError::ZeroDimension);
        }
        let count = vectors.len();
        let mut data = Vec::with_capacity(count * dim);
        for (i, v) in vectors.iter().enumerate() {
            if v.len() != dim {
                return Err(MultiVectorError::DimensionMismatch {
                    expected: dim,
                    got: v.len(),
                    index: i,
                });
            }
            for &val in v {
                if !val.is_finite() {
                    return Err(MultiVectorError::NonFiniteValue);
                }
            }
            data.extend_from_slice(v);
        }
        Ok(Self { data, count, dim })
    }

    /// Create from contiguous f32 data with known count and dim.
    pub fn from_raw(data: Vec<f32>, count: usize, dim: usize) -> Result<Self, MultiVectorError> {
        if count == 0 || dim == 0 {
            return Err(MultiVectorError::Empty);
        }
        if data.len() != count * dim {
            return Err(MultiVectorError::DataLengthMismatch {
                expected: count * dim,
                got: data.len(),
            });
        }
        Ok(Self { data, count, dim })
    }

    /// Number of vectors.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Dimensionality of each vector.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Access the i-th vector as a slice.
    pub fn get(&self, i: usize) -> Option<&[f32]> {
        if i >= self.count {
            return None;
        }
        let start = i * self.dim;
        Some(&self.data[start..start + self.dim])
    }

    /// Iterate over all vectors as slices.
    pub fn iter(&self) -> impl Iterator<Item = &[f32]> {
        (0..self.count).map(move |i| {
            let start = i * self.dim;
            &self.data[start..start + self.dim]
        })
    }

    /// Extract all vectors as owned Vec<Vec<f32>>.
    pub fn to_vectors(&self) -> Vec<Vec<f32>> {
        self.iter().map(|s| s.to_vec()).collect()
    }

    /// Access the raw contiguous data.
    pub fn raw_data(&self) -> &[f32] {
        &self.data
    }
}

/// Aggregation mode for multi-vector scoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultiVectorScoreMode {
    /// MaxSim: max similarity across all document vectors (ColBERT scoring).
    MaxSim,
    /// Average similarity across all document vectors.
    AvgSim,
    /// Sum of similarities across all document vectors.
    SumSim,
}

impl MultiVectorScoreMode {
    /// Parse from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "max_sim" | "maxsim" => Some(Self::MaxSim),
            "avg_sim" | "avgsim" => Some(Self::AvgSim),
            "sum_sim" | "sumsim" => Some(Self::SumSim),
            _ => None,
        }
    }

    /// Aggregate a set of similarity scores for one document.
    pub fn aggregate(&self, scores: &[f32]) -> f32 {
        if scores.is_empty() {
            return 0.0;
        }
        match self {
            Self::MaxSim => scores.iter().cloned().reduce(f32::max).unwrap_or(0.0),
            Self::AvgSim => scores.iter().sum::<f32>() / scores.len() as f32,
            Self::SumSim => scores.iter().sum(),
        }
    }
}

/// Errors from multi-vector construction.
#[derive(Debug, Clone)]
pub enum MultiVectorError {
    Empty,
    ZeroDimension,
    NonFiniteValue,
    DimensionMismatch {
        expected: usize,
        got: usize,
        index: usize,
    },
    DataLengthMismatch {
        expected: usize,
        got: usize,
    },
}

impl std::fmt::Display for MultiVectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "multi-vector must contain at least one vector"),
            Self::ZeroDimension => write!(f, "vector dimension must be > 0"),
            Self::NonFiniteValue => write!(f, "vector values must be finite"),
            Self::DimensionMismatch {
                expected,
                got,
                index,
            } => write!(
                f,
                "dimension mismatch at vector {index}: expected {expected}, got {got}"
            ),
            Self::DataLengthMismatch { expected, got } => {
                write!(f, "data length mismatch: expected {expected}, got {got}")
            }
        }
    }
}

impl std::error::Error for MultiVectorError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_vectors_basic() {
        let mv = MultiVector::from_vectors(vec![vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]]).unwrap();
        assert_eq!(mv.count(), 2);
        assert_eq!(mv.dim(), 3);
        assert_eq!(mv.get(0).unwrap(), &[1.0, 2.0, 3.0]);
        assert_eq!(mv.get(1).unwrap(), &[4.0, 5.0, 6.0]);
        assert!(mv.get(2).is_none());
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let err = MultiVector::from_vectors(vec![vec![1.0, 2.0], vec![3.0]]).unwrap_err();
        assert!(matches!(err, MultiVectorError::DimensionMismatch { .. }));
    }

    #[test]
    fn non_finite_rejected() {
        assert!(MultiVector::from_vectors(vec![vec![f32::NAN]]).is_err());
    }

    #[test]
    fn empty_rejected() {
        assert!(MultiVector::from_vectors(vec![]).is_err());
    }

    #[test]
    fn iter_all_vectors() {
        let mv = MultiVector::from_vectors(vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]])
            .unwrap();
        let collected: Vec<&[f32]> = mv.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[2], &[5.0, 6.0]);
    }

    #[test]
    fn serde_roundtrip() {
        let mv = MultiVector::from_vectors(vec![vec![1.0, 2.0], vec![3.0, 4.0]]).unwrap();
        let bytes = zerompk::to_msgpack_vec(&mv).unwrap();
        let restored: MultiVector = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(mv, restored);
    }

    #[test]
    fn score_modes() {
        let scores = vec![0.5, 0.8, 0.3];
        assert!((MultiVectorScoreMode::MaxSim.aggregate(&scores) - 0.8).abs() < 1e-6);
        assert!((MultiVectorScoreMode::SumSim.aggregate(&scores) - 1.6).abs() < 1e-6);
        let avg = MultiVectorScoreMode::AvgSim.aggregate(&scores);
        assert!((avg - 0.5333).abs() < 0.01);
    }
}
