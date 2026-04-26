"""Map normal form violations to anomalies."""


def detect_anomalies(partial_violations, transitive_violations):
    """Return anomaly descriptions derived from dependencies."""
    merged = {}

    def _merge(dependency_type, dependency, anomalies):
        if dependency not in merged:
            merged[dependency] = {
                "dependency_type": dependency_type,
                "dependency": dependency,
                "anomalies": [],
            }

        # Preserve stable display order while removing duplicates.
        for anomaly in anomalies:
            if anomaly not in merged[dependency]["anomalies"]:
                merged[dependency]["anomalies"].append(anomaly)

    for violation in partial_violations:
        dependency = f"{', '.join(violation.get('lhs', []))} -> {', '.join(violation.get('rhs', []))}"
        _merge("PARTIAL_DEPENDENCY", dependency, ["Update anomaly", "Insertion anomaly"])

    for violation in transitive_violations:
        dependency = f"{', '.join(violation.get('lhs', []))} -> {', '.join(violation.get('rhs', []))}"
        _merge(
            "TRANSITIVE_DEPENDENCY",
            dependency,
            ["Update anomaly", "Insertion anomaly", "Deletion anomaly"],
        )

    return list(merged.values())
