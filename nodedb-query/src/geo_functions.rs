//! Geospatial SQL function evaluation.
//!
//! All ST_* predicates, geometry operations, and geo_* utility functions.
//! Called from `functions::eval_function` for any geo-prefixed or ST_-prefixed name.

use crate::value_ops::{to_value_number, value_to_f64};
use nodedb_types::Value;

/// Try to evaluate a geo/spatial function. Returns `Some(result)` if the
/// function name matched, `None` if unrecognized (caller falls through).
pub fn eval_geo_function(name: &str, args: &[Value]) -> Option<Value> {
    let result = match name {
        "geo_distance" | "haversine_distance" => {
            let lng1 = num_arg(args, 0).unwrap_or(0.0);
            let lat1 = num_arg(args, 1).unwrap_or(0.0);
            let lng2 = num_arg(args, 2).unwrap_or(0.0);
            let lat2 = num_arg(args, 3).unwrap_or(0.0);
            to_value_number(nodedb_types::geometry::haversine_distance(
                lng1, lat1, lng2, lat2,
            ))
        }
        "geo_bearing" | "haversine_bearing" => {
            let lng1 = num_arg(args, 0).unwrap_or(0.0);
            let lat1 = num_arg(args, 1).unwrap_or(0.0);
            let lng2 = num_arg(args, 2).unwrap_or(0.0);
            let lat2 = num_arg(args, 3).unwrap_or(0.0);
            to_value_number(nodedb_types::geometry::haversine_bearing(
                lng1, lat1, lng2, lat2,
            ))
        }
        "geo_point" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            Value::Geometry(nodedb_types::geometry::Geometry::point(lng, lat))
        }
        "geo_geohash" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let precision = num_arg(args, 2).unwrap_or(6.0) as u8;
            Value::String(nodedb_spatial::geohash_encode(lng, lat, precision))
        }
        "geo_geohash_decode" => {
            let hash = str_arg(args, 0).unwrap_or_default();
            match nodedb_spatial::geohash_decode(&hash) {
                Some(bb) => {
                    let mut map = std::collections::HashMap::new();
                    map.insert("min_lng".to_string(), Value::Float(bb.min_lng));
                    map.insert("min_lat".to_string(), Value::Float(bb.min_lat));
                    map.insert("max_lng".to_string(), Value::Float(bb.max_lng));
                    map.insert("max_lat".to_string(), Value::Float(bb.max_lat));
                    Value::Object(map)
                }
                None => Value::Null,
            }
        }
        "geo_geohash_neighbors" => {
            let hash = str_arg(args, 0).unwrap_or_default();
            let neighbors = nodedb_spatial::geohash_neighbors(&hash);
            let arr: Vec<Value> = neighbors
                .into_iter()
                .map(|(dir, h)| {
                    let mut map = std::collections::HashMap::new();
                    map.insert("direction".to_string(), Value::String(format!("{dir:?}")));
                    map.insert("hash".to_string(), Value::String(h));
                    Value::Object(map)
                })
                .collect();
            Value::Array(arr)
        }

        // ── Spatial predicates (ST_*) ──
        "st_contains" => geo_predicate_2(args, nodedb_spatial::st_contains),
        "st_intersects" => geo_predicate_2(args, nodedb_spatial::st_intersects),
        "st_within" => geo_predicate_2(args, nodedb_spatial::st_within),
        "st_disjoint" => geo_predicate_2(args, nodedb_spatial::st_disjoint),
        "st_dwithin" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(Value::Null);
            };
            let dist = num_arg(args, 2).unwrap_or(0.0);
            Value::Bool(nodedb_spatial::st_dwithin(&a, &b, dist))
        }
        "st_distance" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(Value::Null);
            };
            to_value_number(nodedb_spatial::st_distance(&a, &b))
        }
        "st_buffer" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            let dist = num_arg(args, 1).unwrap_or(0.0);
            let segs = num_arg(args, 2).unwrap_or(32.0) as usize;
            Value::Geometry(nodedb_spatial::st_buffer(&geom, dist, segs))
        }
        "st_envelope" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::Geometry(nodedb_spatial::st_envelope(&geom))
        }
        "st_union" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(Value::Null);
            };
            Value::Geometry(nodedb_spatial::st_union(&a, &b))
        }

        // ── Extended geo functions ──
        "geo_length" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            to_value_number(geo_linestring_length(&geom))
        }
        "geo_perimeter" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            to_value_number(geo_polygon_perimeter(&geom))
        }
        "geo_line" => {
            let coords: Vec<[f64; 2]> = args
                .iter()
                .filter_map(|v| {
                    let geom = v.as_geometry()?;
                    if let nodedb_types::geometry::Geometry::Point { coordinates } = geom {
                        Some(*coordinates)
                    } else {
                        None
                    }
                })
                .collect();
            if coords.len() < 2 {
                Value::Null
            } else {
                Value::Geometry(nodedb_types::geometry::Geometry::line_string(coords))
            }
        }
        "geo_polygon" => {
            // Args are arrays of coordinate pairs. Convert from Value::Array.
            let rings: Vec<Vec<[f64; 2]>> = args
                .iter()
                .filter_map(|v| {
                    let arr = v.as_array()?;
                    let coords: Vec<[f64; 2]> = arr
                        .iter()
                        .filter_map(|pt| {
                            let inner = pt.as_array()?;
                            if inner.len() >= 2 {
                                Some([inner[0].as_f64()?, inner[1].as_f64()?])
                            } else {
                                None
                            }
                        })
                        .collect();
                    if coords.is_empty() {
                        None
                    } else {
                        Some(coords)
                    }
                })
                .collect();
            if rings.is_empty() {
                Value::Null
            } else {
                Value::Geometry(nodedb_types::geometry::Geometry::polygon(rings))
            }
        }
        "geo_circle" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let radius = num_arg(args, 2).unwrap_or(0.0);
            let segs = num_arg(args, 3).unwrap_or(32.0) as usize;
            let circle = nodedb_spatial::st_buffer(
                &nodedb_types::geometry::Geometry::point(lng, lat),
                radius,
                segs,
            );
            Value::Geometry(circle)
        }
        "geo_bbox" => {
            let min_lng = num_arg(args, 0).unwrap_or(0.0);
            let min_lat = num_arg(args, 1).unwrap_or(0.0);
            let max_lng = num_arg(args, 2).unwrap_or(0.0);
            let max_lat = num_arg(args, 3).unwrap_or(0.0);
            Value::Geometry(nodedb_types::geometry::Geometry::polygon(vec![vec![
                [min_lng, min_lat],
                [max_lng, min_lat],
                [max_lng, max_lat],
                [min_lng, max_lat],
                [min_lng, min_lat],
            ]]))
        }
        "geo_as_geojson" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            match sonic_rs::to_string(&geom) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Null,
            }
        }
        "geo_from_geojson" => {
            let s = str_arg(args, 0).unwrap_or_default();
            match sonic_rs::from_str::<nodedb_types::geometry::Geometry>(&s) {
                Ok(g) => Value::Geometry(g),
                Err(_) => Value::Null,
            }
        }
        "geo_as_wkt" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::String(nodedb_spatial::geometry_to_wkt(&geom))
        }
        "geo_from_wkt" => {
            let s = str_arg(args, 0).unwrap_or_default();
            match nodedb_spatial::geometry_from_wkt(&s) {
                Some(g) => Value::Geometry(g),
                None => Value::Null,
            }
        }
        "geo_x" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            if let nodedb_types::geometry::Geometry::Point { coordinates } = geom {
                to_value_number(coordinates[0])
            } else {
                Value::Null
            }
        }
        "geo_y" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            if let nodedb_types::geometry::Geometry::Point { coordinates } = geom {
                to_value_number(coordinates[1])
            } else {
                Value::Null
            }
        }
        "geo_num_points" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::Integer(count_points(&geom) as i64)
        }
        "geo_type" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::String(geom.geometry_type().to_string())
        }
        "geo_is_valid" => {
            let Some(geom) = geom_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::Bool(nodedb_spatial::is_valid(&geom))
        }

        // ── H3 hexagonal index ──
        "geo_h3" => {
            let lng = num_arg(args, 0).unwrap_or(0.0);
            let lat = num_arg(args, 1).unwrap_or(0.0);
            let resolution = num_arg(args, 2).unwrap_or(7.0) as u8;
            match nodedb_spatial::h3::h3_encode_string(lng, lat, resolution) {
                Some(hex) => Value::String(hex),
                None => Value::Null,
            }
        }
        "geo_h3_to_boundary" => {
            let h3_str = str_arg(args, 0).unwrap_or_default();
            let h3_idx = u64::from_str_radix(&h3_str, 16).unwrap_or(0);
            if !nodedb_spatial::h3::h3_is_valid(h3_idx) {
                return Some(Value::Null);
            }
            match nodedb_spatial::h3::h3_to_boundary(h3_idx) {
                Some(geom) => Value::Geometry(geom),
                None => Value::Null,
            }
        }
        "geo_h3_resolution" => {
            let h3_str = str_arg(args, 0).unwrap_or_default();
            let h3_idx = u64::from_str_radix(&h3_str, 16).unwrap_or(0);
            if !nodedb_spatial::h3::h3_is_valid(h3_idx) {
                return Some(Value::Null);
            }
            match nodedb_spatial::h3::h3_resolution(h3_idx) {
                Some(r) => Value::Integer(r as i64),
                None => Value::Null,
            }
        }
        "st_intersection" => {
            let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
                return Some(Value::Null);
            };
            Value::Geometry(nodedb_spatial::st_intersection(&a, &b))
        }

        _ => return None,
    };
    Some(result)
}

// ── Helpers ──

fn str_arg(args: &[Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

fn num_arg(args: &[Value], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| value_to_f64(v, true))
}

/// Extract a geometry argument. Supports `Value::Geometry` directly,
/// or falls back to JSON deserialization for `Value::Object` (GeoJSON).
fn geom_arg(args: &[Value], idx: usize) -> Option<nodedb_types::geometry::Geometry> {
    match args.get(idx)? {
        Value::Geometry(g) => Some(g.clone()),
        other => {
            // Fallback: convert Value → JSON → Geometry for GeoJSON objects.
            let json = serde_json::Value::from(other.clone());
            serde_json::from_value(json).ok()
        }
    }
}

fn geo_predicate_2(
    args: &[Value],
    f: fn(&nodedb_types::geometry::Geometry, &nodedb_types::geometry::Geometry) -> bool,
) -> Value {
    let (Some(a), Some(b)) = (geom_arg(args, 0), geom_arg(args, 1)) else {
        return Value::Null;
    };
    Value::Bool(f(&a, &b))
}

fn geo_linestring_length(geom: &nodedb_types::geometry::Geometry) -> f64 {
    let coords = match geom {
        nodedb_types::geometry::Geometry::LineString { coordinates } => coordinates,
        _ => return 0.0,
    };
    let mut total = 0.0;
    for i in 0..coords.len().saturating_sub(1) {
        total += nodedb_types::geometry::haversine_distance(
            coords[i][0],
            coords[i][1],
            coords[i + 1][0],
            coords[i + 1][1],
        );
    }
    total
}

fn geo_polygon_perimeter(geom: &nodedb_types::geometry::Geometry) -> f64 {
    let rings = match geom {
        nodedb_types::geometry::Geometry::Polygon { coordinates } => coordinates,
        _ => return 0.0,
    };
    let Some(exterior) = rings.first() else {
        return 0.0;
    };
    let mut total = 0.0;
    for i in 0..exterior.len().saturating_sub(1) {
        total += nodedb_types::geometry::haversine_distance(
            exterior[i][0],
            exterior[i][1],
            exterior[i + 1][0],
            exterior[i + 1][1],
        );
    }
    total
}

fn count_points(geom: &nodedb_types::geometry::Geometry) -> usize {
    use nodedb_types::geometry::Geometry;
    match geom {
        Geometry::Point { .. } => 1,
        Geometry::LineString { coordinates } => coordinates.len(),
        Geometry::Polygon { coordinates } => coordinates.iter().map(|r| r.len()).sum(),
        Geometry::MultiPoint { coordinates } => coordinates.len(),
        Geometry::MultiLineString { coordinates } => coordinates.iter().map(|ls| ls.len()).sum(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .flat_map(|poly| poly.iter())
            .map(|ring| ring.len())
            .sum(),
        Geometry::GeometryCollection { geometries } => geometries.iter().map(count_points).sum(),
        _ => 0,
    }
}
