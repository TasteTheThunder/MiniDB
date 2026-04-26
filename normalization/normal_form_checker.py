"""Normal form checks (1NF, 2NF, 3NF)."""

from .key_finder import is_superkey


def check_1nf(columns):
    """1NF check.

    For MiniDB metadata, multi-valued attributes are not represented explicitly,
    so 1NF is assumed true unless malformed column definitions are found.
    """
    violations = []
    for column in columns:
        if not isinstance(column, (list, tuple)) or len(column) < 2:
            violations.append({
                "type": "1NF",
                "detail": f"Malformed column definition: {column}"
            })

    return {
        "status": len(violations) == 0,
        "violations": violations,
    }


def check_2nf(all_attributes, fds, candidate_keys, prime_attributes):
    """2NF check: detect partial dependencies when a composite key exists."""
    violations = []

    composite_keys = [set(k) for k in candidate_keys if len(k) > 1]
    if not composite_keys:
        return {
            "status": True,
            "violations": [],
        }

    for fd in fds:
        lhs = set(fd.get("lhs", []))
        rhs = set(fd.get("rhs", []))
        non_prime_rhs = sorted(a for a in rhs if a not in prime_attributes)
        if not non_prime_rhs:
            continue

        for key in composite_keys:
            if lhs < key:  # Proper subset of a composite candidate key
                violations.append({
                    "type": "PARTIAL_DEPENDENCY",
                    "lhs": sorted(lhs),
                    "rhs": non_prime_rhs,
                    "detail": (
                        f"{', '.join(sorted(lhs))} -> {', '.join(non_prime_rhs)} "
                        "(Partial Dependency)"
                    ),
                })
                break

    return {
        "status": len(violations) == 0,
        "violations": violations,
    }


def check_3nf(all_attributes, fds, candidate_keys, prime_attributes):
    """3NF check: detect transitive dependencies.

    A dependency X -> A violates 3NF when X is not a superkey and A is non-prime.
    """
    violations = []
    candidate_key_sets = [set(k) for k in candidate_keys]
    composite_key_sets = [k for k in candidate_key_sets if len(k) > 1]

    for fd in fds:
        lhs = set(fd.get("lhs", []))
        rhs = set(fd.get("rhs", []))

        if not lhs or not rhs:
            continue

        if is_superkey(lhs, all_attributes, fds):
            continue

        # Proper-subset dependencies of composite keys are partial dependencies.
        # Keep them in 2NF output to avoid duplicate/misleading 3NF labels.
        if any(lhs < key for key in composite_key_sets):
            continue

        non_prime_rhs = sorted(a for a in rhs if a not in prime_attributes)
        if not non_prime_rhs:
            continue

        violations.append({
            "type": "TRANSITIVE_DEPENDENCY",
            "lhs": sorted(lhs),
            "rhs": non_prime_rhs,
            "detail": (
                f"{', '.join(sorted(lhs))} -> {', '.join(non_prime_rhs)} "
                "(Transitive Dependency)"
            ),
        })

    return {
        "status": len(violations) == 0,
        "violations": violations,
    }
