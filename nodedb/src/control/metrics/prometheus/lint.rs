//! Convention lint tests for Prometheus metric output.
//!
//! These tests parse the text output of `to_prometheus()` and assert
//! naming conventions: unit suffixes, histogram naming, ratio naming.

#[cfg(test)]
mod tests {
    use crate::control::metrics::SystemMetrics;

    fn prometheus_output() -> String {
        SystemMetrics::new().to_prometheus()
    }

    /// Every `# TYPE <name> counter` line must have `<name>` ending in `_total`.
    #[test]
    fn counter_names_end_in_total() {
        let output = prometheus_output();
        let mut violations: Vec<String> = Vec::new();

        for line in output.lines() {
            let Some(rest) = line.strip_prefix("# TYPE ") else {
                continue;
            };
            let mut parts = rest.split_whitespace();
            let name = match parts.next() {
                Some(n) => n,
                None => continue,
            };
            let kind = match parts.next() {
                Some(k) => k,
                None => continue,
            };
            if kind == "counter" && !name.ends_with("_total") {
                violations.push(name.to_string());
            }
        }

        assert!(
            violations.is_empty(),
            "counter metrics missing _total suffix: {violations:?}"
        );
    }

    /// Every `# TYPE <name> histogram` line where `<name>` contains a
    /// latency-related stem must end in `_seconds`.
    ///
    /// Additionally, no gauge or counter name may contain a raw duration
    /// unit suffix (`_ms`, `_micros`, `_nanos`, `_us`).
    #[test]
    fn histogram_names_use_seconds_when_duration() {
        let output = prometheus_output();
        let latency_stems = [
            "fsync",
            "query",
            "planning",
            "execution",
            "latency",
            "duration",
        ];
        let forbidden_unit_suffixes = ["_ms", "_micros", "_nanos", "_us"];

        let mut missing_seconds: Vec<String> = Vec::new();
        let mut raw_unit_leaks: Vec<String> = Vec::new();

        for line in output.lines() {
            let Some(rest) = line.strip_prefix("# TYPE ") else {
                continue;
            };
            let mut parts = rest.split_whitespace();
            let name = match parts.next() {
                Some(n) => n,
                None => continue,
            };
            let kind = match parts.next() {
                Some(k) => k,
                None => continue,
            };

            if kind == "histogram" {
                let has_latency_stem = latency_stems.iter().any(|s| name.contains(s));
                if has_latency_stem && !name.ends_with("_seconds") {
                    missing_seconds.push(name.to_string());
                }
            }

            if kind == "gauge" || kind == "counter" {
                for suffix in forbidden_unit_suffixes {
                    if name.contains(suffix) {
                        raw_unit_leaks.push(format!("{name} (contains {suffix})"));
                    }
                }
            }
        }

        assert!(
            missing_seconds.is_empty(),
            "histogram metrics with latency stem missing _seconds suffix: {missing_seconds:?}"
        );
        assert!(
            raw_unit_leaks.is_empty(),
            "gauge/counter metrics with raw duration unit suffix: {raw_unit_leaks:?}"
        );
    }

    /// Any gauge metric whose name segment is a ratio/utilization semantic word
    /// must end in `_ratio`. Names ending in `_percent` are rejected.
    #[test]
    fn ratio_gauges_use_ratio_suffix() {
        let output = prometheus_output();
        let ratio_stems = [
            "compression",
            "utilization",
            "efficiency",
            "hit_rate",
            "ratio",
        ];

        let mut missing_ratio: Vec<String> = Vec::new();
        let mut percent_violations: Vec<String> = Vec::new();

        for line in output.lines() {
            let Some(rest) = line.strip_prefix("# TYPE ") else {
                continue;
            };
            let mut parts = rest.split_whitespace();
            let Some(name) = parts.next() else {
                continue;
            };
            let Some(kind) = parts.next() else {
                continue;
            };
            if kind != "gauge" {
                continue;
            }
            // Word-boundary match: only consider segments split on `_`
            // so e.g. `migrations` doesn't trigger on `ratio` substring.
            let segments: Vec<&str> = name.split('_').collect();
            let has_ratio_stem = ratio_stems
                .iter()
                .any(|stem| segments.iter().any(|seg| seg == stem));
            if has_ratio_stem && !name.ends_with("_ratio") {
                missing_ratio.push(name.to_string());
            }
            if name.ends_with("_percent") {
                percent_violations.push(name.to_string());
            }
        }

        assert!(
            missing_ratio.is_empty(),
            "gauge metrics with ratio/utilization stem missing _ratio suffix: {missing_ratio:?}"
        );
        assert!(
            percent_violations.is_empty(),
            "gauge metrics ending in _percent (use _ratio instead): {percent_violations:?}"
        );
    }
}
