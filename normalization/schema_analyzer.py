"""Schema analysis pipeline entry point."""

from .anomaly_detector import detect_anomalies
from .closure import compute_attribute_closure
from .fd_manager import get_fds, save_fds
from .key_finder import find_candidate_keys
from .normal_form_checker import check_1nf, check_2nf, check_3nf
from .normalization_utils import (
    format_attribute_list,
    format_fd,
    load_schema,
    parse_fd_line,
)


def _visual_step(step_no, title, lines):
    # Intentionally no-op for cleaner educational dashboard output.
    return


def _request_fd_input(table_name):
    print(f"No functional dependencies found. Please define FDs for table {table_name}.")
    print("Enter one FD per line in the format: A -> B, C")
    print("Press Enter on an empty line to finish.")

    fds = []
    while True:
        user_line = input("FD > ").strip()
        if not user_line:
            break
        try:
            fds.append(parse_fd_line(user_line))
        except ValueError as exc:
            print(f"Invalid FD: {exc}")

    return fds


def _compute_closure_map(attributes, fds):
    closure_map = {}
    for attr in attributes:
        closure_map[attr] = sorted(compute_attribute_closure({attr}, fds))
    return closure_map


def _build_decomposition_suggestions(partial_violations, transitive_violations):
    suggestions = []
    seen = set()

    def _append_unique(text):
        if text not in seen:
            seen.add(text)
            suggestions.append(text)

    for violation in partial_violations:
        lhs = violation.get("lhs", [])
        rhs = violation.get("rhs", [])
        _append_unique(
            f"Decompose relation by separating {{ {', '.join(rhs)} }} with determinant {{ {', '.join(lhs)} }}"
        )

    for violation in transitive_violations:
        lhs = violation.get("lhs", [])
        rhs = violation.get("rhs", [])
        _append_unique(
            f"Create a new relation for transitive dependency {{ {', '.join(lhs)} -> {', '.join(rhs)} }}"
        )

    return suggestions


def _validate_fds_against_schema(fds, attributes):
    """Ensure FD attributes exist in table schema."""
    attr_set = set(attributes)
    invalid = []

    for fd in fds:
        for attr in fd.get("lhs", []) + fd.get("rhs", []):
            if attr not in attr_set:
                invalid.append(attr)

    if invalid:
        invalid_sorted = sorted(set(invalid))
        raise Exception(
            "Invalid FD attributes not found in schema: "
            f"{', '.join(invalid_sorted)}"
        )


def _print_report(report):
    width = 62
    divider = "─" * width

    def top_box(title):
        print("\n" + "╔" + "═" * width + "╗")
        print(f"║{title:^{width}}║")
        print("╚" + "═" * width + "╝")

    def bottom_box(title):
        print("\n" + "╔" + "═" * width + "╗")
        print(f"║{title:^{width}}║")
        print("╚" + "═" * width + "╝" + "\n")

    def section(title):
        print(f"\n{title}")
        print(divider)

    def nf_state(label, result, reason=""):
        mark = "✔" if result else "❌"
        status = "Satisfied" if result else "Violated"
        suffix = f"   → {reason}" if reason else ""
        print(f"{label:<5} {mark} {status}{suffix}")

    def dependency_flow_lines(fds):
        lines = []
        for fd in fds:
            lhs = ", ".join(fd.get("lhs", []))
            rhs = fd.get("rhs", [])
            if not rhs:
                continue

            lines.append(f"{lhs} ─────────────▶ {rhs[0]}")
            if len(rhs) > 1:
                for idx, attr in enumerate(rhs[1:], 1):
                    branch = "└" if idx == len(rhs) - 1 else "├"
                    lines.append(f"{' ' * len(lhs)}  {branch}──────────────▶ {attr}")
        return lines

    def anomaly_icons(anomaly_group):
        icons = []
        if "Update anomaly" in anomaly_group["anomalies"]:
            icons.append("🔄 Update")
        if "Insertion anomaly" in anomaly_group["anomalies"]:
            icons.append("➕ Insert")
        if "Deletion anomaly" in anomaly_group["anomalies"]:
            icons.append("❌ Delete")
        return " | ".join(icons)

    def fd_text(violation):
        return f"{', '.join(violation.get('lhs', []))} → {', '.join(violation.get('rhs', []))}"

    def quote_list(values):
        quoted = [f"'{v}'" for v in values]
        if len(quoted) == 0:
            return ""
        if len(quoted) == 1:
            return quoted[0]
        if len(quoted) == 2:
            return f"{quoted[0]} and {quoted[1]}"
        return ", ".join(quoted[:-1]) + f", and {quoted[-1]}"

    def partial_reason(violation, candidate_key):
        lhs = quote_list(violation.get("lhs", []))
        rhs_list = violation.get("rhs", [])
        rhs = quote_list(rhs_list)
        verb = "depends" if len(rhs_list) == 1 else "depend"
        return f"{rhs} {verb} only on {lhs}, not the full key ({', '.join(candidate_key)})"

    def transitive_reason(violation):
        lhs = quote_list(violation.get("lhs", []))
        rhs_list = violation.get("rhs", [])
        rhs = quote_list(rhs_list)
        verb = "depends" if len(rhs_list) == 1 else "depend"
        return f"{rhs} {verb} on {lhs}, not directly on the primary key"

    def parse_dependency(dep_text):
        left, right = dep_text.split("->", 1)
        lhs = [p.strip() for p in left.split(",") if p.strip()]
        rhs = [p.strip() for p in right.split(",") if p.strip()]
        return lhs, rhs

    def update_anomaly_text(lhs, rhs):
        lhs_txt = ", ".join(lhs)
        rhs_txt = ", ".join(rhs)
        return (
            f"If the same {lhs_txt} value appears in multiple rows, "
            f"{rhs_txt} may need updates in many places"
        )

    def insert_anomaly_text(lhs, rhs):
        lhs_txt = ", ".join(lhs)
        rhs_txt = ", ".join(rhs)
        return (
            f"Cannot insert facts about {rhs_txt} without also providing "
            f"a valid value for {lhs_txt}"
        )

    def delete_anomaly_text(lhs, rhs):
        lhs_txt = ", ".join(lhs)
        rhs_txt = ", ".join(rhs)
        return (
            f"Deleting the last row for a {lhs_txt} value may also remove "
            f"its {rhs_txt} information"
        )

    def _normalize_name_token(token):
        token = token.strip().lower()
        if token.endswith("_id") and len(token) > 3:
            token = token[:-3]

        # Handle compact id abbreviations without underscore, e.g. sid/cid/tid.
        if token.endswith("id") and "_" not in token and len(token) <= 4:
            compact_map = {
                "sid": "student",
                "cid": "course",
                "tid": "teacher",
                "did": "department",
            }
            token = compact_map.get(token, token)

        # Common short forms that improve readability across schemas.
        replacements = {
            "dept": "department",
            "fac": "faculty",
            "inst": "instructor",
            "stud": "student",
            "stu": "student",
            "crs": "course",
        }
        return replacements.get(token, token)

    def relation_name_from_lhs(lhs, idx, used_names):
        if not lhs:
            candidate = f"relation_{idx}"
        else:
            parts = [_normalize_name_token(token) for token in lhs]
            parts = [p for p in parts if p]
            candidate = "_".join(parts) if parts else f"relation_{idx}"

        base = candidate
        counter = 2
        while candidate in used_names:
            candidate = f"{base}_{counter}"
            counter += 1
        used_names.add(candidate)
        return candidate

    def base_relation_name(table_name, used_names):
        name = table_name.lower().strip()
        for suffix in ("_test", "_tmp", "_table", "_tbl"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break

        name = name.strip("_") or "base_relation"

        if name in used_names:
            name = f"{name}_base"
        used_names.add(name)
        return name

    def explain_partial(violation, candidate_key):
        lhs = set(violation.get("lhs", []))
        key = set(candidate_key)
        if lhs and lhs < key:
            return (
                f"Because {{{', '.join(sorted(lhs))}}} is only part of composite key "
                f"{{{', '.join(candidate_key)}}}, non-prime attributes should not depend on it alone."
            )
        return "This creates a partial dependency on a non-key subset."

    def explain_transitive(violation, prime_attributes):
        lhs = violation.get("lhs", [])
        rhs = [a for a in violation.get("rhs", []) if a not in prime_attributes]
        rhs_text = ", ".join(rhs) if rhs else ", ".join(violation.get("rhs", []))
        return (
            f"Because determinant {{{', '.join(lhs)}}} is not a superkey and determines "
            f"non-prime attribute(s) {{{rhs_text}}}."
        )

    nf1 = report["normal_forms"]["1NF"]
    nf2 = report["normal_forms"]["2NF"]
    nf3 = report["normal_forms"]["3NF"]
    nf2v = nf2["violations"]
    nf3v = nf3["violations"]
    nf2_effective = nf2["status"] and nf1["status"]
    nf3_effective = nf3["status"] and nf2_effective

    top_box("MiniDB Schema Analyzer")

    section("🧩 SCHEMA OVERVIEW")
    print(f"Table        : {report['table']}")
    print(f"Attributes   : {format_attribute_list(report['attributes'])}")
    print(f"Primary Key  : ({format_attribute_list(report['candidate_key'])})")

    section("🔗 FUNCTIONAL DEPENDENCY MAP")
    for line in dependency_flow_lines(report["functional_dependencies"]):
        print(line)
    if not report["functional_dependencies"]:
        print("No functional dependencies provided.")

    section("🔑 KEY ANALYSIS")
    print(f"Candidate Key       : {{{format_attribute_list(report['candidate_key'])}}}")
    print()
    print(f"Prime Attributes    : {format_attribute_list(report['prime_attributes'])}")
    print(f"Non-Prime Attributes: {format_attribute_list(report['non_prime_attributes'])}")

    section("⚠️ NORMALIZATION STATUS")
    nf_state("1NF", nf1["status"])
    nf_state("2NF", nf2_effective)
    nf_state("3NF", nf3_effective)

    section("🚨 DETECTED VIOLATIONS")
    if not (nf2v or nf3v):
        print("No violations detected.")
    else:
        if not nf1["status"]:
            print("[1NF - Prerequisite Not Met]")
            print("  • A relation must satisfy 1NF before it can satisfy 2NF or 3NF.\n")

        if nf2v:
            print("[2NF - Partial Dependency]")
            for violation in nf2v:
                print(f"  • FD     : {fd_text(violation)}")
                print(f"    Reason : {partial_reason(violation, report['candidate_key'])}\n")
            print("  👉 Problem: Data is duplicated across rows -> redundancy\n")
        if not nf2_effective:
            print("[3NF - Prerequisite Not Met]")
            print("  • A relation must satisfy 2NF before it can satisfy 3NF.")
            print("  • Since a partial dependency exists, the relation cannot be in 3NF.\n")
        elif nf3v:
            print("[3NF - Transitive Dependency]")
            for violation in nf3v:
                print(f"  • FD     : {fd_text(violation)}")
                print(f"    Reason : {transitive_reason(violation)}\n")
            print("  👉 Problem: Indirect dependency -> causes inconsistency")

    section("⚡ ANOMALY ANALYSIS")
    if report["anomalies"]:
        for anomaly_group in report["anomalies"]:
            dep = anomaly_group["dependency"].replace("->", "→")
            lhs, rhs = parse_dependency(anomaly_group["dependency"])
            print(f"• {dep}")

            if "Update anomaly" in anomaly_group["anomalies"]:
                print("  - 🔄 Update  : " + update_anomaly_text(lhs, rhs))

            if "Insertion anomaly" in anomaly_group["anomalies"]:
                print("  - ➕ Insert  : " + insert_anomaly_text(lhs, rhs))

            if "Deletion anomaly" in anomaly_group["anomalies"]:
                print("  - ❌ Delete  : " + delete_anomaly_text(lhs, rhs))

            print()
    else:
        print("No anomaly risks detected.")

    section("🧱 PROPOSED 3NF DECOMPOSITION")
    if nf2v or nf3v:
        relations = []
        seen = set()
        for violation in nf2v + nf3v:
            attrs = tuple(sorted(set(violation.get("lhs", []) + violation.get("rhs", []))))
            if attrs not in seen:
                seen.add(attrs)
                relations.append({
                    "lhs": tuple(violation.get("lhs", [])),
                    "attrs": attrs,
                })

        covered = set()
        for rel in relations:
            covered.update(rel["attrs"])

        # Ensure full attribute coverage for any table shape.
        residual = sorted(set(report["attributes"]) - covered)
        transitive_lhs = sorted({
            attr
            for violation in nf3v
            for attr in violation.get("lhs", [])
        })
        base_attrs = sorted(
            set(report["candidate_key"]) | set(residual) | set(transitive_lhs)
        )

        used_relation_names = set()

        for idx, rel in enumerate(relations, 1):
            rel_name = relation_name_from_lhs(rel["lhs"], idx, used_relation_names)
            print(rel_name)
            print(f"  ({', '.join(rel['attrs'])})")
            print()

        print(base_relation_name(report["table"], used_relation_names))
        print(f"  ({format_attribute_list(base_attrs)})")
    else:
        print("Schema already in 3NF. No decomposition required.")

    section("🎯 FINAL RESULT")
    if nf2_effective and nf3_effective:
        print("✔ Schema is already normalized to 3NF")
        print("✔ No redundancy issues detected")
        print("✔ No update/insert/delete anomalies from FDs")
        print("✔ Clean modular design")
    else:
        print("✔ 3NF decomposition generated")
        print("✔ Redundancy can be eliminated")
        print("✔ Anomalies can be resolved with decomposed schema")
        print("✔ Clean modular design prepared")

    bottom_box("Analysis Completed Successfully")


def analyze_schema(table_name, interactive=True, database=None):
    """Analyze schema and evaluate normalization status.

    Args:
        table_name: Target table name.
        interactive: If True and FDs are missing, prompts for FD input.
        database: Database name. If specified, analyzes table in that database.

    Returns:
        dict: Structured analysis report.
    """
    # Step 1: Load schema
    schema = load_schema(table_name, database=database)
    attributes = schema["attributes"]
    _visual_step(1, "Load Schema", [
        f"Table: {table_name}",
        f"Attributes: {format_attribute_list(attributes)}",
        f"Primary Key: {format_attribute_list(schema['primary_key'])}",
    ])

    # Step 2: Load or request FDs
    fds = get_fds(table_name, database)
    step2_lines = []
    if fds is None and interactive:
        step2_lines.extend([
            "No stored functional dependencies found.",
            "Requesting FD input from user.",
        ])
        fds = _request_fd_input(table_name)
        if fds:
            save_fds(table_name, fds, database)
            step2_lines.append("FDs saved for future analysis.")
    elif fds is None:
        fds = []
        step2_lines.append("No stored functional dependencies found.")
        step2_lines.append("Interactive mode disabled, proceeding with empty FD set.")
    else:
        step2_lines.append("Loaded functional dependencies from metadata.")

    _visual_step(2, "Load / Request Functional Dependencies", step2_lines + [
        f"Functional Dependencies Loaded: {len(fds)}",
        *([format_fd(fd) for fd in fds] if fds else ["No FDs provided"]) 
    ])

    _validate_fds_against_schema(fds, attributes)

    # Step 3: Compute closure
    closure_map = _compute_closure_map(attributes, fds)
    closure_lines = [f"{attr}+ = {{{', '.join(vals)}}}" for attr, vals in closure_map.items()]
    _visual_step(3, "Compute Closure", closure_lines)

    # Step 4: Candidate key
    candidate_keys = find_candidate_keys(attributes, fds, schema["raw_primary_key"])
    selected_key = candidate_keys[0] if candidate_keys else []
    _visual_step(4, "Identify Candidate Key", [
        f"Candidate Keys: {', '.join(['{' + ', '.join(k) + '}' for k in candidate_keys]) if candidate_keys else 'None'}",
        f"Selected Candidate Key: {format_attribute_list(selected_key)}",
    ])

    # Step 5: Prime/non-prime attributes
    prime_attributes = sorted({attr for key in candidate_keys for attr in key})
    non_prime_attributes = sorted(a for a in attributes if a not in set(prime_attributes))
    _visual_step(5, "Identify Prime / Non-prime Attributes", [
        f"Prime Attributes: {format_attribute_list(prime_attributes)}",
        f"Non-prime Attributes: {format_attribute_list(non_prime_attributes)}",
    ])

    # Step 6/7/8: Normal forms
    nf1 = check_1nf(schema["columns"])
    _visual_step(6, "Check 1NF", ["Satisfied" if nf1["status"] else "Not satisfied"])

    nf2 = check_2nf(attributes, fds, candidate_keys, set(prime_attributes))
    _visual_step(7, "Check 2NF", [
        "Satisfied" if nf2["status"] else "Not satisfied",
        *(v["detail"] for v in nf2["violations"]),
    ])

    nf3 = check_3nf(attributes, fds, candidate_keys, set(prime_attributes))
    _visual_step(8, "Check 3NF", [
        "Satisfied" if nf3["status"] else "Not satisfied",
        *(v["detail"] for v in nf3["violations"]),
    ])

    # Step 9: Anomaly detection
    anomalies = detect_anomalies(nf2["violations"], nf3["violations"])
    anomaly_lines = []
    for anomaly in anomalies:
        dependency_type = anomaly["dependency_type"].replace("_", " ").title()
        anomaly_lines.append(
            f"[{dependency_type}] {anomaly['dependency']}: {', '.join(anomaly['anomalies'])}"
        )
    if not anomaly_lines:
        anomaly_lines = ["No anomalies detected from dependency analysis"]
    _visual_step(9, "Detect Anomalies", anomaly_lines)

    # Step 10: Decomposition suggestion
    decomposition_suggestions = _build_decomposition_suggestions(nf2["violations"], nf3["violations"])
    _visual_step(10, "Suggest Decomposition", decomposition_suggestions or ["No decomposition needed"])

    all_violations = nf1["violations"] + nf2["violations"] + nf3["violations"]

    report = {
        "table": table_name,
        "attributes": attributes,
        "functional_dependencies": fds,
        "closure": closure_map,
        "candidate_keys": candidate_keys,
        "candidate_key": selected_key,
        "prime_attributes": prime_attributes,
        "non_prime_attributes": non_prime_attributes,
        "normal_forms": {
            "1NF": nf1,
            "2NF": nf2,
            "3NF": nf3,
        },
        "violations": all_violations,
        "anomalies": anomalies,
        "decomposition_suggestions": decomposition_suggestions,
    }

    _print_report(report)
    return report
