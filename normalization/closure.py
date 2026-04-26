"""Attribute closure computation."""


def compute_attribute_closure(attributes, fds):
    """Compute closure of a set of attributes using iterative FD expansion.

    Args:
        attributes: Iterable of attribute names.
        fds: List of FD dicts in form {"lhs": [...], "rhs": [...]}.

    Returns:
        set: Closure set.
    """
    closure = set(attributes)
    changed = True

    while changed:
        changed = False
        for fd in fds:
            lhs = set(fd.get("lhs", []))
            rhs = set(fd.get("rhs", []))
            if lhs.issubset(closure) and not rhs.issubset(closure):
                closure.update(rhs)
                changed = True

    return closure
