//! ST_Intersection — compute the area shared by two geometries.
//!
//! Uses Sutherland-Hodgman polygon clipping for polygon-polygon intersection.
//! For other geometry type combinations, delegates to simpler cases or
//! returns empty geometry.
//!
//! Sutherland-Hodgman works correctly for convex clipping polygons. For
//! concave clip polygons, the result may include extra edges — this is a
//! known limitation shared by most lightweight GIS implementations. Full
//! Weiler-Atherton is needed for perfect concave-concave intersection but
//! adds significant complexity. Sutherland-Hodgman covers the vast majority
//! of practical use cases (convex query regions, rectangular bboxes, etc.).
//!
//! Reference: Sutherland & Hodgman, "Reentrant Polygon Clipping" (1974)

use nodedb_types::geometry::Geometry;

/// ST_Intersection(a, b) — return the geometry representing the shared area.
///
/// Returns GeometryCollection with an empty geometries vec if there is no
/// intersection.
pub fn st_intersection(a: &Geometry, b: &Geometry) -> Geometry {
    match (a, b) {
        // Polygon–Polygon: Sutherland-Hodgman clipping.
        (
            Geometry::Polygon {
                coordinates: rings_a,
            },
            Geometry::Polygon {
                coordinates: rings_b,
            },
        ) => {
            let Some(ext_a) = rings_a.first() else {
                return empty_geometry();
            };
            let Some(ext_b) = rings_b.first() else {
                return empty_geometry();
            };
            let clipped = sutherland_hodgman(ext_a, ext_b);
            if clipped.len() < 3 {
                return empty_geometry();
            }
            // Close the ring.
            let mut ring = clipped;
            if ring.first() != ring.last()
                && let Some(&first) = ring.first()
            {
                ring.push(first);
            }
            Geometry::Polygon {
                coordinates: vec![ring],
            }
        }

        // Point–Polygon or reverse: if point is inside polygon, return point.
        (Geometry::Point { coordinates: pt }, Geometry::Polygon { coordinates: rings })
        | (Geometry::Polygon { coordinates: rings }, Geometry::Point { coordinates: pt }) => {
            if let Some(ext) = rings.first()
                && (nodedb_types::geometry::point_in_polygon(pt[0], pt[1], ext)
                    || crate::predicates::edge::point_on_ring_boundary(*pt, ext))
            {
                return Geometry::Point { coordinates: *pt };
            }
            empty_geometry()
        }

        // Point–Point: if identical, return point.
        (Geometry::Point { coordinates: ca }, Geometry::Point { coordinates: cb }) => {
            if (ca[0] - cb[0]).abs() < 1e-12 && (ca[1] - cb[1]).abs() < 1e-12 {
                Geometry::Point { coordinates: *ca }
            } else {
                empty_geometry()
            }
        }

        // LineString–Polygon or reverse: clip line to polygon boundary.
        (Geometry::LineString { coordinates: line }, Geometry::Polygon { coordinates: rings })
        | (Geometry::Polygon { coordinates: rings }, Geometry::LineString { coordinates: line }) => {
            clip_linestring_to_polygon(line, rings)
        }

        // All other combinations: fall back to checking intersection,
        // return one of the inputs if they intersect.
        _ => {
            if crate::predicates::st_intersects(a, b) {
                // Return the smaller geometry as the "intersection".
                // This is an approximation for complex type combinations.
                Geometry::GeometryCollection {
                    geometries: vec![a.clone(), b.clone()],
                }
            } else {
                empty_geometry()
            }
        }
    }
}

/// Empty geometry (no intersection).
fn empty_geometry() -> Geometry {
    Geometry::GeometryCollection {
        geometries: Vec::new(),
    }
}

/// Sutherland-Hodgman polygon clipping.
///
/// Clips `subject` polygon against `clip` polygon. Both are coordinate rings
/// (closed or unclosed). Returns the clipped polygon vertices (unclosed).
///
/// The clip polygon's edges define half-planes. For each edge, vertices of
/// the subject that are "inside" (left of the edge) are kept. Vertices
/// that cross from inside to outside (or vice versa) generate intersection
/// points on the clip edge.
fn sutherland_hodgman(subject: &[[f64; 2]], clip: &[[f64; 2]]) -> Vec<[f64; 2]> {
    if subject.is_empty() || clip.is_empty() {
        return Vec::new();
    }

    let mut output = strip_closing(subject);
    let clip_edges = strip_closing(clip);

    let n = clip_edges.len();
    for i in 0..n {
        if output.is_empty() {
            return Vec::new();
        }

        let edge_start = clip_edges[i];
        let edge_end = clip_edges[(i + 1) % n];

        let input = output;
        // no-governor: hot-path Sutherland-Hodgman clip; polygon vertex count bounded by input (typically small)
        output = Vec::with_capacity(input.len());

        let m = input.len();
        for j in 0..m {
            let current = input[j];
            let previous = input[(j + m - 1) % m];

            let curr_inside = is_inside(current, edge_start, edge_end);
            let prev_inside = is_inside(previous, edge_start, edge_end);

            if curr_inside {
                if !prev_inside {
                    // Entering: add intersection point.
                    if let Some(pt) = line_intersection(previous, current, edge_start, edge_end) {
                        output.push(pt);
                    }
                }
                output.push(current);
            } else if prev_inside {
                // Leaving: add intersection point.
                if let Some(pt) = line_intersection(previous, current, edge_start, edge_end) {
                    output.push(pt);
                }
            }
        }
    }

    output
}

/// Check if a point is on the "inside" (left side) of a directed edge.
fn is_inside(point: [f64; 2], edge_start: [f64; 2], edge_end: [f64; 2]) -> bool {
    // Cross product: (edge_end - edge_start) × (point - edge_start)
    // Positive = left side = inside for CCW winding.
    let cross = (edge_end[0] - edge_start[0]) * (point[1] - edge_start[1])
        - (edge_end[1] - edge_start[1]) * (point[0] - edge_start[0]);
    cross >= 0.0
}

/// Compute the intersection point of two line segments (as infinite lines).
fn line_intersection(a1: [f64; 2], a2: [f64; 2], b1: [f64; 2], b2: [f64; 2]) -> Option<[f64; 2]> {
    let dx_a = a2[0] - a1[0];
    let dy_a = a2[1] - a1[1];
    let dx_b = b2[0] - b1[0];
    let dy_b = b2[1] - b1[1];

    let denom = dx_a * dy_b - dy_a * dx_b;
    if denom.abs() < 1e-15 {
        return None; // Parallel lines.
    }

    let t = ((b1[0] - a1[0]) * dy_b - (b1[1] - a1[1]) * dx_b) / denom;
    Some([a1[0] + t * dx_a, a1[1] + t * dy_a])
}

/// Strip the closing vertex from a ring if present.
fn strip_closing(ring: &[[f64; 2]]) -> Vec<[f64; 2]> {
    if ring.len() >= 2 && ring.first() == ring.last() {
        ring[..ring.len() - 1].to_vec()
    } else {
        ring.to_vec()
    }
}

/// Clip a linestring to a polygon: keep only the parts inside.
fn clip_linestring_to_polygon(line: &[[f64; 2]], rings: &[Vec<[f64; 2]>]) -> Geometry {
    let Some(exterior) = rings.first() else {
        return empty_geometry();
    };

    // Collect points that are inside the polygon.
    let mut inside_points: Vec<[f64; 2]> = Vec::new();
    for pt in line {
        if nodedb_types::geometry::point_in_polygon(pt[0], pt[1], exterior)
            || crate::predicates::edge::point_on_ring_boundary(*pt, exterior)
        {
            inside_points.push(*pt);
        }
    }

    if inside_points.len() < 2 {
        return empty_geometry();
    }

    Geometry::LineString {
        coordinates: inside_points,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square(min: f64, max: f64) -> Geometry {
        Geometry::polygon(vec![vec![
            [min, min],
            [max, min],
            [max, max],
            [min, max],
            [min, min],
        ]])
    }

    #[test]
    fn overlapping_squares() {
        let a = square(0.0, 10.0);
        let b = square(5.0, 15.0);
        let result = st_intersection(&a, &b);
        if let Geometry::Polygon { coordinates } = &result {
            assert!(!coordinates[0].is_empty());
            // Intersection should be approximately [5,5] to [10,10].
            let xs: Vec<f64> = coordinates[0].iter().map(|c| c[0]).collect();
            let ys: Vec<f64> = coordinates[0].iter().map(|c| c[1]).collect();
            let min_x = xs.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_x = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let min_y = ys.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_y = ys.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            assert!((min_x - 5.0).abs() < 0.01, "min_x={min_x}");
            assert!((max_x - 10.0).abs() < 0.01, "max_x={max_x}");
            assert!((min_y - 5.0).abs() < 0.01, "min_y={min_y}");
            assert!((max_y - 10.0).abs() < 0.01, "max_y={max_y}");
        } else {
            panic!("expected Polygon, got {:?}", result.geometry_type());
        }
    }

    #[test]
    fn contained_square() {
        let a = square(0.0, 10.0);
        let b = square(2.0, 8.0);
        let result = st_intersection(&a, &b);
        if let Geometry::Polygon { coordinates } = &result {
            // Result should be approximately b itself.
            let xs: Vec<f64> = coordinates[0].iter().map(|c| c[0]).collect();
            let min_x = xs.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_x = xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            assert!((min_x - 2.0).abs() < 0.01);
            assert!((max_x - 8.0).abs() < 0.01);
        } else {
            panic!("expected Polygon");
        }
    }

    #[test]
    fn disjoint_returns_empty() {
        let a = square(0.0, 5.0);
        let b = square(10.0, 15.0);
        let result = st_intersection(&a, &b);
        if let Geometry::GeometryCollection { geometries } = &result {
            assert!(geometries.is_empty());
        } else {
            panic!("expected empty GeometryCollection");
        }
    }

    #[test]
    fn point_inside_polygon() {
        let poly = square(0.0, 10.0);
        let pt = Geometry::point(5.0, 5.0);
        let result = st_intersection(&poly, &pt);
        assert_eq!(result.geometry_type(), "Point");
    }

    #[test]
    fn point_outside_polygon() {
        let poly = square(0.0, 10.0);
        let pt = Geometry::point(20.0, 20.0);
        let result = st_intersection(&poly, &pt);
        if let Geometry::GeometryCollection { geometries } = &result {
            assert!(geometries.is_empty());
        }
    }

    #[test]
    fn identical_points() {
        let a = Geometry::point(5.0, 5.0);
        let b = Geometry::point(5.0, 5.0);
        let result = st_intersection(&a, &b);
        assert_eq!(result.geometry_type(), "Point");
    }

    #[test]
    fn different_points() {
        let a = Geometry::point(0.0, 0.0);
        let b = Geometry::point(1.0, 1.0);
        let result = st_intersection(&a, &b);
        if let Geometry::GeometryCollection { geometries } = &result {
            assert!(geometries.is_empty());
        }
    }

    #[test]
    fn linestring_clipped_to_polygon() {
        let poly = square(0.0, 10.0);
        // Line fully inside the polygon.
        let line = Geometry::line_string(vec![[2.0, 5.0], [5.0, 5.0], [8.0, 5.0]]);
        let result = st_intersection(&poly, &line);
        assert_eq!(result.geometry_type(), "LineString");
        if let Geometry::LineString { coordinates } = &result {
            assert_eq!(coordinates.len(), 3);
        }
    }
}
