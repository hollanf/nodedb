//! zerompk `ToMessagePack` / `FromMessagePack` implementations for
//! [`SurrogateBitmap`].
//!
//! Wire encoding: a single msgpack `bin` value whose bytes are the
//! portable roaring-bitmap serialization produced by
//! `RoaringBitmap::serialize_into`. This is the most compact format
//! that survives cross-platform round-trips and can be decoded without
//! allocating an intermediate `Vec<u8>` on the read side.
//!
//! The zerompk `bin` type maps to msgpack's binary ext (`0xc4`/`0xc5`/`0xc6`
//! family), which zerompk's derive macro already uses for `Vec<u8>` fields.
//! Manually implementing the same encoding keeps `SurrogateBitmap` fully
//! substitutable for the `Option<Vec<u8>>` fields it replaces.

use std::io::Cursor;

use roaring::RoaringBitmap;
use zerompk::{FromMessagePack, ToMessagePack};

use super::bitmap::SurrogateBitmap;

impl ToMessagePack for SurrogateBitmap {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        // Serialise the roaring bitmap to a byte vec.
        let mut buf = Vec::with_capacity(self.0.serialized_size());
        self.0
            .serialize_into(&mut buf)
            .map_err(zerompk::Error::IoError)?;
        // Write as msgpack `bin`.
        writer.write_binary(&buf)
    }
}

impl<'a> FromMessagePack<'a> for SurrogateBitmap {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let cow = reader.read_binary()?;
        let bytes: &[u8] = &cow;
        let inner =
            RoaringBitmap::deserialize_from(Cursor::new(bytes)).map_err(zerompk::Error::IoError)?;
        Ok(SurrogateBitmap(inner))
    }
}

#[cfg(test)]
mod tests {
    use crate::surrogate::Surrogate;
    use crate::surrogate_bitmap::SurrogateBitmap;

    fn roundtrip(b: &SurrogateBitmap) -> SurrogateBitmap {
        let bytes = zerompk::to_msgpack_vec(b).expect("serialize");
        zerompk::from_msgpack(&bytes).expect("deserialize")
    }

    #[test]
    fn empty_bitmap_codec_roundtrip() {
        let b = SurrogateBitmap::new();
        let b2 = roundtrip(&b);
        assert_eq!(b, b2);
    }

    #[test]
    fn small_bitmap_codec_roundtrip() {
        let b = SurrogateBitmap::from_iter([1, 2, 3, 100, 200].map(Surrogate));
        let b2 = roundtrip(&b);
        assert_eq!(b, b2);
    }

    #[test]
    fn large_bitmap_codec_roundtrip() {
        let b = SurrogateBitmap::from_iter((1u32..=10_000).map(Surrogate));
        let b2 = roundtrip(&b);
        assert_eq!(b, b2);
    }
}
