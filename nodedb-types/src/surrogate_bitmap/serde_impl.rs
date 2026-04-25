//! `serde::Serialize` / `Deserialize` for [`SurrogateBitmap`].
//!
//! Wire shape: the portable roaring byte buffer wrapped in serde `bytes`,
//! matching the zerompk codec so the on-disk and over-the-wire shapes are
//! interchangeable.

use std::io::Cursor;

use roaring::RoaringBitmap;
use serde::de::{Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::bitmap::SurrogateBitmap;

impl Serialize for SurrogateBitmap {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut buf = Vec::with_capacity(self.0.serialized_size());
        self.0
            .serialize_into(&mut buf)
            .map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&buf)
    }
}

impl<'de> Deserialize<'de> for SurrogateBitmap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BytesVisitor;

        impl<'v> Visitor<'v> for BytesVisitor {
            type Value = SurrogateBitmap;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a roaring-bitmap byte buffer")
            }

            fn visit_bytes<E: DeError>(self, v: &[u8]) -> Result<Self::Value, E> {
                let inner = RoaringBitmap::deserialize_from(Cursor::new(v)).map_err(E::custom)?;
                Ok(SurrogateBitmap(inner))
            }

            fn visit_byte_buf<E: DeError>(self, v: Vec<u8>) -> Result<Self::Value, E> {
                self.visit_bytes(&v)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'v>,
            {
                let mut buf = Vec::new();
                while let Some(b) = seq.next_element::<u8>()? {
                    buf.push(b);
                }
                let inner = RoaringBitmap::deserialize_from(Cursor::new(&buf))
                    .map_err(<A::Error as DeError>::custom)?;
                Ok(SurrogateBitmap(inner))
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::surrogate::Surrogate;
    use crate::surrogate_bitmap::SurrogateBitmap;

    #[test]
    fn json_roundtrip_via_serde() {
        let b = SurrogateBitmap::from_iter([1u32, 7, 99, 12345].map(Surrogate));
        let bytes = sonic_rs::to_vec(&b).expect("serialize");
        let b2: SurrogateBitmap = sonic_rs::from_slice(&bytes).expect("deserialize");
        assert_eq!(b, b2);
    }
}
