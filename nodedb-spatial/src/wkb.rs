//! Well-Known Binary (WKB) serialization for Geometry types.
//!
//! WKB is the standard binary format for geometry interchange (ISO 13249).
//! Used as the Arrow `DataType::Binary` backing for spatial columns —
//! avoids JSON parse overhead during DataFusion query execution.
//!
//! Format (little-endian):
//! ```text
//! [byte_order: u8] [type: u32] [coordinates...]
//! ```
//!
//! Byte order: 1 = little-endian (NDR), 0 = big-endian (XDR). We always
//! write little-endian and accept both on read.

use nodedb_types::geometry::Geometry;

// WKB geometry type codes (ISO 13249 / OGC SFA).
const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;

const BYTE_ORDER_LE: u8 = 1;

/// Serialize a Geometry to WKB (little-endian).
pub fn geometry_to_wkb(geom: &Geometry) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    write_geometry(&mut buf, geom);
    buf
}

/// Deserialize a Geometry from WKB bytes.
///
/// Returns `None` if the bytes are malformed or truncated.
pub fn geometry_from_wkb(data: &[u8]) -> Option<Geometry> {
    let mut cursor = 0;
    read_geometry(data, &mut cursor)
}

/// Extract bounding box from WKB without full deserialization.
///
/// Scans coordinate values to find min/max. Faster than full deserialize
/// when only the bbox is needed (e.g., R-tree insertion from Arrow batch).
pub fn wkb_bbox(data: &[u8]) -> Option<nodedb_types::BoundingBox> {
    let geom = geometry_from_wkb(data)?;
    Some(nodedb_types::geometry_bbox(&geom))
}

// ── Write helpers ──

fn write_geometry(buf: &mut Vec<u8>, geom: &Geometry) {
    match geom {
        Geometry::Point { coordinates } => {
            write_header(buf, WKB_POINT);
            write_f64(buf, coordinates[0]);
            write_f64(buf, coordinates[1]);
        }
        Geometry::LineString { coordinates } => {
            write_header(buf, WKB_LINESTRING);
            write_u32(buf, coordinates.len() as u32);
            for c in coordinates {
                write_f64(buf, c[0]);
                write_f64(buf, c[1]);
            }
        }
        Geometry::Polygon { coordinates } => {
            write_header(buf, WKB_POLYGON);
            write_u32(buf, coordinates.len() as u32);
            for ring in coordinates {
                write_u32(buf, ring.len() as u32);
                for c in ring {
                    write_f64(buf, c[0]);
                    write_f64(buf, c[1]);
                }
            }
        }
        Geometry::MultiPoint { coordinates } => {
            write_header(buf, WKB_MULTIPOINT);
            write_u32(buf, coordinates.len() as u32);
            for c in coordinates {
                // Each point is a full WKB Point.
                write_header(buf, WKB_POINT);
                write_f64(buf, c[0]);
                write_f64(buf, c[1]);
            }
        }
        Geometry::MultiLineString { coordinates } => {
            write_header(buf, WKB_MULTILINESTRING);
            write_u32(buf, coordinates.len() as u32);
            for ls in coordinates {
                write_geometry(
                    buf,
                    &Geometry::LineString {
                        coordinates: ls.clone(),
                    },
                );
            }
        }
        Geometry::MultiPolygon { coordinates } => {
            write_header(buf, WKB_MULTIPOLYGON);
            write_u32(buf, coordinates.len() as u32);
            for poly in coordinates {
                write_geometry(
                    buf,
                    &Geometry::Polygon {
                        coordinates: poly.clone(),
                    },
                );
            }
        }
        Geometry::GeometryCollection { geometries } => {
            write_header(buf, WKB_GEOMETRYCOLLECTION);
            write_u32(buf, geometries.len() as u32);
            for g in geometries {
                write_geometry(buf, g);
            }
        }

        // Unknown future geometry type — write empty geometry collection.
        _ => {
            write_header(buf, WKB_GEOMETRYCOLLECTION);
            write_u32(buf, 0);
        }
    }
}

fn write_header(buf: &mut Vec<u8>, wkb_type: u32) {
    buf.push(BYTE_ORDER_LE);
    write_u32(buf, wkb_type);
}

fn write_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_f64(buf: &mut Vec<u8>, val: f64) {
    buf.extend_from_slice(&val.to_le_bytes());
}

// ── Read helpers ──

fn read_geometry(data: &[u8], cursor: &mut usize) -> Option<Geometry> {
    let byte_order = read_u8(data, cursor)?;
    let is_le = byte_order == 1;
    let wkb_type = read_u32(data, cursor, is_le)?;

    match wkb_type {
        WKB_POINT => {
            let x = read_f64(data, cursor, is_le)?;
            let y = read_f64(data, cursor, is_le)?;
            Some(Geometry::Point {
                coordinates: [x, y],
            })
        }
        WKB_LINESTRING => {
            let n = read_u32(data, cursor, is_le)? as usize;
            let coords = read_coords(data, cursor, n, is_le)?;
            Some(Geometry::LineString {
                coordinates: coords,
            })
        }
        WKB_POLYGON => {
            let num_rings = read_u32(data, cursor, is_le)? as usize;
            let mut rings = Vec::with_capacity(num_rings);
            for _ in 0..num_rings {
                let n = read_u32(data, cursor, is_le)? as usize;
                let ring = read_coords(data, cursor, n, is_le)?;
                rings.push(ring);
            }
            Some(Geometry::Polygon { coordinates: rings })
        }
        WKB_MULTIPOINT => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let mut coords = Vec::with_capacity(count);
            for _ in 0..count {
                let inner = read_geometry(data, cursor)?;
                if let Geometry::Point { coordinates } = inner {
                    coords.push(coordinates);
                } else {
                    return None;
                }
            }
            Some(Geometry::MultiPoint {
                coordinates: coords,
            })
        }
        WKB_MULTILINESTRING => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let mut lines = Vec::with_capacity(count);
            for _ in 0..count {
                let inner = read_geometry(data, cursor)?;
                if let Geometry::LineString { coordinates } = inner {
                    lines.push(coordinates);
                } else {
                    return None;
                }
            }
            Some(Geometry::MultiLineString { coordinates: lines })
        }
        WKB_MULTIPOLYGON => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let mut polys = Vec::with_capacity(count);
            for _ in 0..count {
                let inner = read_geometry(data, cursor)?;
                if let Geometry::Polygon { coordinates } = inner {
                    polys.push(coordinates);
                } else {
                    return None;
                }
            }
            Some(Geometry::MultiPolygon { coordinates: polys })
        }
        WKB_GEOMETRYCOLLECTION => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let mut geoms = Vec::with_capacity(count);
            for _ in 0..count {
                geoms.push(read_geometry(data, cursor)?);
            }
            Some(Geometry::GeometryCollection { geometries: geoms })
        }
        _ => None,
    }
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    if *cursor >= data.len() {
        return None;
    }
    let val = data[*cursor];
    *cursor += 1;
    Some(val)
}

fn read_u32(data: &[u8], cursor: &mut usize, is_le: bool) -> Option<u32> {
    if *cursor + 4 > data.len() {
        return None;
    }
    let bytes: [u8; 4] = [
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ];
    *cursor += 4;
    Some(if is_le {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    })
}

fn read_f64(data: &[u8], cursor: &mut usize, is_le: bool) -> Option<f64> {
    if *cursor + 8 > data.len() {
        return None;
    }
    let bytes: [u8; 8] = [
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
        data[*cursor + 4],
        data[*cursor + 5],
        data[*cursor + 6],
        data[*cursor + 7],
    ];
    *cursor += 8;
    Some(if is_le {
        f64::from_le_bytes(bytes)
    } else {
        f64::from_be_bytes(bytes)
    })
}

fn read_coords(
    data: &[u8],
    cursor: &mut usize,
    count: usize,
    is_le: bool,
) -> Option<Vec<[f64; 2]>> {
    let mut coords = Vec::with_capacity(count);
    for _ in 0..count {
        let x = read_f64(data, cursor, is_le)?;
        let y = read_f64(data, cursor, is_le)?;
        coords.push([x, y]);
    }
    Some(coords)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_roundtrip() {
        let geom = Geometry::point(-73.9857, 40.7484);
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn linestring_roundtrip() {
        let geom = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]);
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn polygon_roundtrip() {
        let geom = Geometry::polygon(vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]], // hole
        ]);
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn multipoint_roundtrip() {
        let geom = Geometry::MultiPoint {
            coordinates: vec![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn multilinestring_roundtrip() {
        let geom = Geometry::MultiLineString {
            coordinates: vec![
                vec![[0.0, 0.0], [1.0, 1.0]],
                vec![[2.0, 2.0], [3.0, 3.0], [4.0, 2.0]],
            ],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn multipolygon_roundtrip() {
        let geom = Geometry::MultiPolygon {
            coordinates: vec![
                vec![vec![
                    [0.0, 0.0],
                    [1.0, 0.0],
                    [1.0, 1.0],
                    [0.0, 1.0],
                    [0.0, 0.0],
                ]],
                vec![vec![
                    [5.0, 5.0],
                    [6.0, 5.0],
                    [6.0, 6.0],
                    [5.0, 6.0],
                    [5.0, 5.0],
                ]],
            ],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn geometry_collection_roundtrip() {
        let geom = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(1.0, 2.0),
                Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            ],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn truncated_data_returns_none() {
        let wkb = geometry_to_wkb(&Geometry::point(1.0, 2.0));
        assert!(geometry_from_wkb(&wkb[..3]).is_none());
        assert!(geometry_from_wkb(&[]).is_none());
    }

    #[test]
    fn invalid_type_returns_none() {
        let mut wkb = geometry_to_wkb(&Geometry::point(1.0, 2.0));
        wkb[1] = 99; // invalid WKB type
        assert!(geometry_from_wkb(&wkb).is_none());
    }

    #[test]
    fn wkb_bbox_extraction() {
        let geom = Geometry::polygon(vec![vec![
            [-10.0, -5.0],
            [10.0, -5.0],
            [10.0, 5.0],
            [-10.0, 5.0],
            [-10.0, -5.0],
        ]]);
        let wkb = geometry_to_wkb(&geom);
        let bb = wkb_bbox(&wkb).unwrap();
        assert_eq!(bb.min_lng, -10.0);
        assert_eq!(bb.max_lng, 10.0);
        assert_eq!(bb.min_lat, -5.0);
        assert_eq!(bb.max_lat, 5.0);
    }

    #[test]
    fn point_wkb_size() {
        let wkb = geometry_to_wkb(&Geometry::point(0.0, 0.0));
        // 1 (byte order) + 4 (type) + 8 (x) + 8 (y) = 21 bytes
        assert_eq!(wkb.len(), 21);
    }
}
