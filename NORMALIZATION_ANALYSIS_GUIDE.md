# NirvahaDB Schema Normalization Guide

This guide explains how NirvahaDB analyzes schema design quality using functional dependencies (FDs), keys, and normal forms.

## 1. Command

Use:

```sql
ANALYZE SCHEMA <table_name>;
```

Example:

```sql
ANALYZE SCHEMA students;
```

## 2. Functional Dependencies (FDs)

An FD means one attribute set determines another.

- Example: `roll -> name, dept`
- Meaning: if two rows have the same `roll`, they must have the same `name` and `dept`.

NirvahaDB stores table-specific FDs in metadata for reuse.

## 3. Interactive FD Input Flow

If no FD is stored for a table and interactive mode is enabled, MiniDB asks for FD input.

Prompt format:

```text
No functional dependencies found. Please define FDs for table students.
FD > roll -> name, dept
FD > dept -> hod
FD >
```

Rules:
- One FD per line
- Use `->` between left and right sides
- Use commas for multiple attributes
- Press Enter on an empty line to finish

## 4. Step-by-Step Analysis Pipeline

NirvahaDB follows this analysis pipeline internally and presents a compact educational dashboard report:

1. Load Schema
2. Load / Request Functional Dependencies
3. Compute Closure
4. Identify Candidate Key
5. Identify Prime / Non-prime Attributes
6. Check 1NF
7. Check 2NF
8. Check 3NF
9. Detect Anomalies
10. Suggest Decomposition

Note:
- `ANALYZE SCHEMA` intentionally uses its own dedicated dashboard output.
- Generic tokenizer/parser/executor trace blocks are skipped for this command for readability.

## 5. Attribute Closure

Closure of attribute set `X` (written as `X+`) is all attributes derivable from `X` using FDs.

NirvahaDB computes closure by iterative FD expansion:
- Start with `X+ = X`
- Repeatedly apply FDs where LHS is in closure
- Add RHS attributes until no new attributes appear

Why it matters:
- Determines superkeys
- Helps discover candidate keys

## 6. Candidate Key Detection

NirvahaDB determines candidate keys as follows:

1. If metadata primary key exists, it is used directly as the candidate key base.
2. Otherwise, MiniDB searches minimal attribute combinations whose closure covers all table attributes.

A candidate key is:
- Unique determinant of all attributes (superkey)
- Minimal (no proper subset is a superkey)

## 7. Prime and Non-Prime Attributes

- Prime attribute: appears in at least one candidate key
- Non-prime attribute: does not appear in any candidate key

These sets are required for 2NF and 3NF checks.

## 8. Normal Form Checks

### 8.1 1NF

NirvahaDB assumes 1NF is satisfied unless malformed schema metadata is found.

### 8.2 2NF

2NF violations are checked only when a composite candidate key exists.

Violation condition (partial dependency):
- A proper subset of composite key determines a non-prime attribute.

### 8.3 3NF

FD `X -> A` violates 3NF when:
- `X` is not a superkey, and
- `A` is non-prime

NirvahaDB reports these as transitive dependency violations.
Note:
- A relation must satisfy 2NF before it can satisfy 3NF.

## 9. Dependency to Anomaly Mapping

NirvahaDB maps detected violations to practical data anomalies.

### Partial Dependency

Anomalies:
- Update anomaly
- Insertion anomaly

Reason:
- Repeated facts tied to part of a key force redundant updates and awkward inserts.

### Transitive Dependency

Anomalies:
- Update anomaly
- Insertion anomaly
- Deletion anomaly

Reason:
- Non-key facts depend on other non-key facts, causing redundancy and risk of information loss.

## 10. Suggested Decomposition

NirvahaDB suggests decomposition patterns based on violations:

- Partial dependency: separate attributes on RHS into a relation with the determinant (LHS)
- Transitive dependency: create a new relation for transitive determinant and dependents

Goal:
- Reduce redundancy
- Remove anomalies
- Move schema toward higher normal forms

## 11. Report Structure

Analysis produces both:

1. Structured report object (programmatic)
2. Human-readable visual report

Typical report includes:
- Table name
- Candidate key
- Prime and non-prime attributes
- 1NF/2NF/3NF status
- Violation details
- Mapped anomalies
- Decomposition suggestions

## 12. Practical Example

Given:
- Attributes: `roll, name, dept, hod`
- FDs:
  - `roll -> name, dept`
  - `dept -> hod`

Interpretation:
- `dept -> hod` creates transitive dependency via `roll -> dept -> hod`
- 3NF violation occurs
- Possible anomalies:
  - Update anomaly
  - Insertion anomaly
  - Deletion anomaly

Possible decomposition:
- `Student(roll, name, dept)`
- `Department(dept, hod)`

## 13. Notes

- CREATE TABLE behavior is unchanged.
- Analysis supports both interactive FD input and predefined FD mode.
- FD attributes are validated against the table schema before analysis.
