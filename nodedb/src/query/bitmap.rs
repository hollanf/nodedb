use roaring::RoaringBitmap;

/// Build a roaring bitmap from a set of document IDs.
///
/// Used for pre-filtering during HNSW vector search: the bitmap indicates
/// which document IDs satisfy metadata predicates, so the vector engine
/// can skip non-matching candidates during traversal.
pub fn from_ids(ids: impl IntoIterator<Item = u32>) -> RoaringBitmap {
    ids.into_iter().collect()
}

/// Intersect two bitmaps (AND). Used to combine multiple filter predicates.
pub fn intersect(a: &RoaringBitmap, b: &RoaringBitmap) -> RoaringBitmap {
    a & b
}

/// Union two bitmaps (OR). Used for OR-combined predicates.
pub fn union(a: &RoaringBitmap, b: &RoaringBitmap) -> RoaringBitmap {
    a | b
}

/// Serialize a bitmap to bytes for cross-plane transport via SPSC.
pub fn serialize(bitmap: &RoaringBitmap) -> Vec<u8> {
    let mut buf = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut buf)
        .expect("bitmap serialization");
    buf
}

/// Deserialize a bitmap from bytes received via SPSC.
pub fn deserialize(bytes: &[u8]) -> crate::Result<RoaringBitmap> {
    RoaringBitmap::deserialize_from(bytes).map_err(|e| crate::Error::Serialization {
        format: "roaring_bitmap".into(),
        detail: format!("deserialize: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_check() {
        let bm = from_ids(vec![1, 5, 10, 100]);
        assert!(bm.contains(1));
        assert!(bm.contains(10));
        assert!(!bm.contains(2));
        assert_eq!(bm.len(), 4);
    }

    #[test]
    fn intersect_filters() {
        let a = from_ids(vec![1, 2, 3, 4, 5]);
        let b = from_ids(vec![3, 4, 5, 6, 7]);
        let result = intersect(&a, &b);
        assert_eq!(result.len(), 3);
        assert!(result.contains(3));
        assert!(result.contains(4));
        assert!(result.contains(5));
    }

    #[test]
    fn union_combines() {
        let a = from_ids(vec![1, 2]);
        let b = from_ids(vec![3, 4]);
        let result = union(&a, &b);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn serialize_roundtrip() {
        let bm = from_ids(0..10000);
        let bytes = serialize(&bm);
        let bm2 = deserialize(&bytes).unwrap();
        assert_eq!(bm, bm2);
    }

    #[test]
    fn empty_bitmap() {
        let bm = from_ids(std::iter::empty());
        assert!(bm.is_empty());
        let bytes = serialize(&bm);
        let bm2 = deserialize(&bytes).unwrap();
        assert!(bm2.is_empty());
    }
}
