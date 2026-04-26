"""Candidate key discovery."""

from itertools import combinations

from .closure import compute_attribute_closure


def _normalize_key(key):
    if key is None:
        return []
    if isinstance(key, list):
        return key
    return [key]


def _is_superkey(attr_set, all_attributes, fds):
    return compute_attribute_closure(attr_set, fds) == set(all_attributes)


def find_candidate_keys(all_attributes, fds, primary_key=None):
    """Find candidate keys for relation.

    If metadata primary key exists, use it directly as required.
    """
    pk = _normalize_key(primary_key)
    if pk:
        return [pk]

    attrs = list(all_attributes)
    attr_set = set(attrs)
    candidate_keys = []

    for size in range(1, len(attrs) + 1):
        for subset in combinations(attrs, size):
            subset_set = set(subset)

            # Minimality pruning: skip if a known candidate key is subset.
            if any(set(key).issubset(subset_set) for key in candidate_keys):
                continue

            if _is_superkey(subset_set, attr_set, fds):
                candidate_keys.append(list(subset))

    return candidate_keys


def is_superkey(attributes, all_attributes, fds):
    """Public helper for normal form checks."""
    return _is_superkey(set(attributes), set(all_attributes), fds)
