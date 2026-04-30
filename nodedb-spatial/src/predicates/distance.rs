//! ST_Distance and ST_DWithin — minimum distance between geometries.
//!
//! Point-to-point uses haversine (great-circle distance in meters).
//! All other combinations use planar geometry to find the nearest
//! segment pair, then convert the coordinate-space result to meters
//! using equirectangular approximation.
//!
//! ST_DWithin uses bbox expansion as a fast pre-filter: expand query
//! geometry's bbox by the distance threshold, check intersection first.

use nodedb_types::geometry::{Geometry, haversine_distance, point_in_polygon};
use nodedb_types::geometry_bbox;

use super::edge::{point_on_ring_boundary, ring_edges, segment_to_segment_dist_sq};

/// ST_Distance(a, b) — minimum distance in meters between two geometries.
///
/// Point-to-point: haversine (exact great-circle).
/// All others: find minimum coordinate-space distance, convert to meters.
pub fn st_distance(a: &Geometry, b: &Geometry) -> f64 {
    match (a, b) {
        // Point–Point: haversine (exact).
        (Geometry::Point { coordinates: ca }, Geometry::Point { coordinates: cb }) => {
            haversine_distance(ca[0], ca[1], cb[0], cb[1])
        }

        // Point–LineString or reverse.
        (Geometry::Point { coordinates: pt }, Geometry::LineString { coordinates: line })
        | (Geometry::LineString { coordinates: line }, Geometry::Point { coordinates: pt }) => {
            point_to_linestring_distance(*pt, line)
        }

        // Point–Polygon or reverse.
        (Geometry::Point { coordinates: pt }, Geometry::Polygon { coordinates: rings })
        | (Geometry::Polygon { coordinates: rings }, Geometry::Point { coordinates: pt }) => {
            point_to_polygon_distance(*pt, rings)
        }

        // LineString–LineString.
        (Geometry::LineString { coordinates: la }, Geometry::LineString { coordinates: lb }) => {
            linestring_to_linestring_distance(la, lb)
        }

        // LineString–Polygon or reverse.
        (Geometry::LineString { coordinates: line }, Geometry::Polygon { coordinates: rings })
        | (Geometry::Polygon { coordinates: rings }, Geometry::LineString { coordinates: line }) => {
            linestring_to_polygon_distance(line, rings)
        }

        // Polygon–Polygon.
        (Geometry::Polygon { coordinates: ra }, Geometry::Polygon { coordinates: rb }) => {
            polygon_to_polygon_distance(ra, rb)
        }

        // Multi* types: minimum distance among all component pairs.
        (Geometry::MultiPoint { coordinates }, other)
        | (other, Geometry::MultiPoint { coordinates }) => coordinates
            .iter()
            .map(|pt| st_distance(&Geometry::Point { coordinates: *pt }, other))
            .fold(f64::INFINITY, f64::min),

        (Geometry::MultiLineString { coordinates }, other)
        | (other, Geometry::MultiLineString { coordinates }) => coordinates
            .iter()
            .map(|ls| {
                st_distance(
                    &Geometry::LineString {
                        coordinates: ls.clone(),
                    },
                    other,
                )
            })
            .fold(f64::INFINITY, f64::min),

        (Geometry::MultiPolygon { coordinates }, other)
        | (other, Geometry::MultiPolygon { coordinates }) => coordinates
            .iter()
            .map(|poly| {
                st_distance(
                    &Geometry::Polygon {
                        coordinates: poly.clone(),
                    },
                    other,
                )
            })
            .fold(f64::INFINITY, f64::min),

        (Geometry::GeometryCollection { geometries }, other)
        | (other, Geometry::GeometryCollection { geometries }) => geometries
            .iter()
            .map(|g| st_distance(g, other))
            .fold(f64::INFINITY, f64::min),

        // Unknown future geometry types — treat as infinite distance.
        (&_, &_) => f64::INFINITY,
    }
}

/// ST_DWithin(a, b, distance_meters) — are A and B within the given distance?
///
/// Optimized: uses bbox expansion pre-filter to avoid expensive exact
/// distance computation when geometries are clearly far apart.
pub fn st_dwithin(a: &Geometry, b: &Geometry, distance_meters: f64) -> bool {
    // Fast path for points: just haversine.
    if let (Geometry::Point { coordinates: ca }, Geometry::Point { coordinates: cb }) = (a, b) {
        return haversine_distance(ca[0], ca[1], cb[0], cb[1]) <= distance_meters;
    }

    // Bbox pre-filter: expand A's bbox by distance, check if B's bbox intersects.
    let a_bb = geometry_bbox(a).expand_meters(distance_meters);
    let b_bb = geometry_bbox(b);
    if !a_bb.intersects(&b_bb) {
        return false;
    }

    // Exact check.
    st_distance(a, b) <= distance_meters
}

// ── Distance helpers ──

/// Point to linestring: minimum haversine distance to any segment.
fn point_to_linestring_distance(pt: [f64; 2], line: &[[f64; 2]]) -> f64 {
    if line.is_empty() {
        return f64::INFINITY;
    }
    if line.len() == 1 {
        return haversine_distance(pt[0], pt[1], line[0][0], line[0][1]);
    }

    let mut min_dist = f64::INFINITY;
    for i in 0..line.len() - 1 {
        let d = point_to_segment_meters(pt, line[i], line[i + 1]);
        min_dist = min_dist.min(d);
    }
    min_dist
}

/// Point to polygon: 0 if inside, otherwise min distance to any edge.
fn point_to_polygon_distance(pt: [f64; 2], rings: &[Vec<[f64; 2]>]) -> f64 {
    let Some(exterior) = rings.first() else {
        return f64::INFINITY;
    };

    // Inside exterior (and not in a hole) → distance 0.
    if point_in_polygon(pt[0], pt[1], exterior) || point_on_ring_boundary(pt, exterior) {
        let in_hole = rings[1..]
            .iter()
            .any(|hole| point_in_polygon(pt[0], pt[1], hole) && !point_on_ring_boundary(pt, hole));
        if !in_hole {
            return 0.0;
        }
    }

    // Distance to nearest edge of all rings.
    let mut min_dist = f64::INFINITY;
    for ring in rings {
        for i in 0..ring.len().saturating_sub(1) {
            let d = point_to_segment_meters(pt, ring[i], ring[i + 1]);
            min_dist = min_dist.min(d);
        }
    }
    min_dist
}

fn linestring_to_linestring_distance(la: &[[f64; 2]], lb: &[[f64; 2]]) -> f64 {
    let mut min_dist = f64::INFINITY;
    for i in 0..la.len().saturating_sub(1) {
        for j in 0..lb.len().saturating_sub(1) {
            let d_sq = segment_to_segment_dist_sq(la[i], la[i + 1], lb[j], lb[j + 1]);
            if d_sq < 1e-20 {
                return 0.0;
            }
            let d = coord_dist_to_meters(d_sq.sqrt(), la[i], lb[j]);
            min_dist = min_dist.min(d);
        }
    }
    min_dist
}

fn linestring_to_polygon_distance(line: &[[f64; 2]], rings: &[Vec<[f64; 2]>]) -> f64 {
    // If any line point is inside the polygon, distance is 0.
    let Some(exterior) = rings.first() else {
        return f64::INFINITY;
    };
    for pt in line {
        if point_in_polygon(pt[0], pt[1], exterior) || point_on_ring_boundary(*pt, exterior) {
            return 0.0;
        }
    }

    // Minimum distance between line segments and ring edges.
    let mut min_dist = f64::INFINITY;
    for ring in rings {
        let edges = ring_edges(ring);
        for i in 0..line.len().saturating_sub(1) {
            for &(re_a, re_b) in &edges {
                let d_sq = segment_to_segment_dist_sq(line[i], line[i + 1], re_a, re_b);
                if d_sq < 1e-20 {
                    return 0.0;
                }
                let d = coord_dist_to_meters(d_sq.sqrt(), line[i], re_a);
                min_dist = min_dist.min(d);
            }
        }
    }
    min_dist
}

fn polygon_to_polygon_distance(ra: &[Vec<[f64; 2]>], rb: &[Vec<[f64; 2]>]) -> f64 {
    let Some(ext_a) = ra.first() else {
        return f64::INFINITY;
    };
    let Some(ext_b) = rb.first() else {
        return f64::INFINITY;
    };

    // If any vertex of B is inside A (or vice versa), distance is 0.
    for pt in ext_b {
        if point_in_polygon(pt[0], pt[1], ext_a) || point_on_ring_boundary(*pt, ext_a) {
            return 0.0;
        }
    }
    for pt in ext_a {
        if point_in_polygon(pt[0], pt[1], ext_b) || point_on_ring_boundary(*pt, ext_b) {
            return 0.0;
        }
    }

    // Min edge-to-edge distance.
    let mut min_dist = f64::INFINITY;
    let a_edges = ring_edges(ext_a);
    let b_edges = ring_edges(ext_b);
    for &(a1, a2) in &a_edges {
        for &(b1, b2) in &b_edges {
            let d_sq = segment_to_segment_dist_sq(a1, a2, b1, b2);
            if d_sq < 1e-20 {
                return 0.0;
            }
            let d = coord_dist_to_meters(d_sq.sqrt(), a1, b1);
            min_dist = min_dist.min(d);
        }
    }
    min_dist
}

/// Point to segment distance in meters.
///
/// Projects onto segment in coordinate space, then uses haversine for the
/// final distance to the nearest point on the segment.
fn point_to_segment_meters(pt: [f64; 2], seg_a: [f64; 2], seg_b: [f64; 2]) -> f64 {
    let dx = seg_b[0] - seg_a[0];
    let dy = seg_b[1] - seg_a[1];
    let len_sq = dx * dx + dy * dy;

    let nearest = if len_sq < 1e-20 {
        seg_a
    } else {
        let t = ((pt[0] - seg_a[0]) * dx + (pt[1] - seg_a[1]) * dy) / len_sq;
        let t = t.clamp(0.0, 1.0);
        [seg_a[0] + t * dx, seg_a[1] + t * dy]
    };

    haversine_distance(pt[0], pt[1], nearest[0], nearest[1])
}

/// Convert coordinate-space distance to approximate meters.
///
/// Uses equirectangular approximation at the midpoint latitude.
fn coord_dist_to_meters(coord_dist: f64, a: [f64; 2], b: [f64; 2]) -> f64 {
    let mid_lat = ((a[1] + b[1]) / 2.0).to_radians();
    let meters_per_deg_lat = 110_540.0;
    let meters_per_deg_lng = 111_320.0 * mid_lat.cos();
    // Approximate: treat coord_dist as an isotropic distance in degrees,
    // scale by average meters per degree.
    let avg_scale = (meters_per_deg_lat + meters_per_deg_lng) / 2.0;
    coord_dist * avg_scale
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square() -> Geometry {
        Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]])
    }

    #[test]
    fn point_to_point_haversine() {
        let d = st_distance(&Geometry::point(0.0, 0.0), &Geometry::point(0.0, 1.0));
        // ~111 km for 1 degree latitude.
        assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    }

    #[test]
    fn point_to_point_same() {
        let d = st_distance(&Geometry::point(5.0, 5.0), &Geometry::point(5.0, 5.0));
        assert!(d < 0.01);
    }

    #[test]
    fn point_inside_polygon_zero_distance() {
        let d = st_distance(&Geometry::point(0.5, 0.5), &square());
        assert!(d < 0.01);
    }

    #[test]
    fn point_on_edge_zero_distance() {
        let d = st_distance(&Geometry::point(0.5, 0.0), &square());
        assert!(d < 0.01);
    }

    #[test]
    fn point_to_polygon_outside() {
        let d = st_distance(&Geometry::point(2.0, 0.5), &square());
        // 1 degree longitude at equator ≈ 111 km.
        assert!(d > 100_000.0, "got {d}");
    }

    #[test]
    fn point_to_linestring() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0]]);
        // Point 1 degree north of the line.
        let d = st_distance(&Geometry::point(5.0, 1.0), &line);
        assert!((d - 111_195.0).abs() < 500.0, "got {d}");
    }

    #[test]
    fn linestring_to_linestring_crossing() {
        let a = Geometry::line_string(vec![[0.0, 0.0], [10.0, 10.0]]);
        let b = Geometry::line_string(vec![[0.0, 10.0], [10.0, 0.0]]);
        let d = st_distance(&a, &b);
        assert!(d < 0.01);
    }

    #[test]
    fn dwithin_points_close() {
        let a = Geometry::point(0.0, 0.0);
        let b = Geometry::point(0.001, 0.0);
        // ~111 meters apart.
        assert!(st_dwithin(&a, &b, 200.0));
        assert!(!st_dwithin(&a, &b, 50.0));
    }

    #[test]
    fn dwithin_point_polygon() {
        let pt = Geometry::point(2.0, 0.5);
        // Point is ~1 degree east of polygon.
        assert!(st_dwithin(&pt, &square(), 200_000.0)); // 200 km
        assert!(!st_dwithin(&pt, &square(), 50_000.0)); // 50 km
    }

    #[test]
    fn dwithin_bbox_prefilter_rejects() {
        // Points very far apart — bbox pre-filter should reject cheaply.
        let a = Geometry::point(0.0, 0.0);
        let b = Geometry::point(90.0, 45.0);
        assert!(!st_dwithin(&a, &b, 1000.0));
    }

    #[test]
    fn polygon_to_polygon_overlapping() {
        let other = Geometry::polygon(vec![vec![
            [0.5, 0.5],
            [1.5, 0.5],
            [1.5, 1.5],
            [0.5, 1.5],
            [0.5, 0.5],
        ]]);
        let d = st_distance(&square(), &other);
        assert!(d < 0.01);
    }

    #[test]
    fn polygon_to_polygon_separated() {
        let far = Geometry::polygon(vec![vec![
            [5.0, 5.0],
            [6.0, 5.0],
            [6.0, 6.0],
            [5.0, 6.0],
            [5.0, 5.0],
        ]]);
        let d = st_distance(&square(), &far);
        assert!(d > 100_000.0, "got {d}");
    }
}
