# 🚀 NirvahaDB - Educational SQL Database Management System

A lightweight, educational SQL database management system built entirely in Python from scratch. NirvahaDB demonstrates core database concepts including ACID properties, indexing, normalization, and query processing without relying on external database libraries.

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)
![Status](https://img.shields.io/badge/Status-Active-success.svg)


## 🎯 Overview

NirvahaDB is a fully functional file based relational database management system designed for educational purposes. It implements fundamental database concepts from the ground up, providing insights into how production databases work internally.

### Why NirvahaDB?

- **Educational**: Trace query execution step-by-step to understand database internals
- **Lightweight**: No external dependencies, pure Python implementation
- **Feature-Rich**: Supports complex queries, joins, aggregations, and constraints
- **Portable**: File-based storage, easy to deploy and test

## ✨ Features

### Core Database Operations

- ✅ **DDL (Data Definition Language)**
  - CREATE, DROP, ALTER TABLE operations
  - Composite primary key support
  - Dynamic schema modifications

- ✅ **DML (Data Manipulation Language)**
  - INSERT (single and bulk operations)
  - SELECT with WHERE, ORDER BY, GROUP BY, LIMIT
  - UPDATE with conditional logic
  - DELETE with WHERE clause
  - TRUNCATE for data cleanup

- ✅ **Advanced Query Features**
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
  - GROUP BY with aggregations
  - ORDER BY (ASC/DESC)
  - LIMIT for result pagination
  - Multi-row INSERT statements
  - INNER JOIN, LEFT JOIN, RIGHT JOIN (nested loop)
  - Subqueries with WHERE IN
  - EXISTS / NOT EXISTS (non-correlated)

- ✅ **Constraint Management**
  - Primary Key constraints (single and composite)
  - Duplicate detection
  - Foreign key relationships with RESTRICT enforcement

- ✅ **Schema Operations**
  - Add/Drop columns
  - Modify column datatypes
  - Rename columns and tables
  - SHOW TABLES
  - DESCRIBE table structure
  - ANALYZE SCHEMA (functional dependency and normal form analysis)

### Data Types Supported

- `INT` - Integer values
- `DOUBLE` - Floating-point numbers
- `CHAR` - Fixed-length strings
- `VARCHAR` - Variable-length strings

### Storage & Performance

- File-based storage system (`.tbl` for data, `.meta` for metadata)
- JSON metadata management
- Efficient indexing system
- Query optimization

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────┐
│              User Input (SQL Query)             │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│              Tokenizer                          │
│  (Lexical Analysis - Breaks query into tokens)  │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│              Parser                             │
│  (Syntax Analysis - Validates & structures)     │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│              Executor                           │
│  (Command execution orchestrator)               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│           Storage Engine                        │
│  (File I/O, Data persistence, Indexing)         │
└─────────────────────────────────────────────────┘
```

## 🚀 Installation

### Prerequisites

- Python 3.8 or higher
- No external dependencies required!

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/minidb.git

# Navigate to project directory
cd minidb

# Run NirvahaDB
python minidb.py
```

NirvahaDB creates `data/` and `metadata/` automatically on startup.
If you run internal modules directly (without `minidb.py`), create them manually:

```bash
mkdir data metadata
```

## 💻 Usage

### Starting NirvahaDB

```bash
python minidb.py
```

### Interactive Mode

```
🚀 Welcome to NirvahaDB
Current Mode: EDUCATIONAL
Type 'HELP' for commands, or 'EXIT' to quit

NirvahaDB > 
```

### Setting Modes

```sql
-- Enable educational mode (shows execution traces)
SET MODE EDUCATIONAL;

-- Enable production mode (faster, no traces)
SET MODE PRODUCTION;

-- Check current mode
SHOW MODE;

-- Clear the screen
CLEAR;
```

## 📚 Supported SQL Commands

### 0. DATABASE COMMANDS

Create, switch, list, and drop databases.

```sql
-- Create a database
CREATE DATABASE college_db;

-- List databases
SHOW DATABASES;

-- Use a database
USE college_db;

-- Drop a database
DROP DATABASE college_db;
```

### 1. CREATE TABLE

Create a new table with columns and optional primary key.

```sql
-- Simple table (table-level primary key)
CREATE TABLE students (
  id INT,
  name VARCHAR,
  age INT,
  PRIMARY KEY (id)
);

-- Composite primary key (table-level)
CREATE TABLE enrollments (
  student_id INT,
  course_id INT,
  grade DOUBLE,
  PRIMARY KEY (student_id, course_id)
);

-- Inline primary key
CREATE TABLE students (
  id INT PRIMARY KEY,
  name VARCHAR,
  age INT
);

-- Foreign key (table-level)
CREATE TABLE students (
  id INT,
  name VARCHAR,
  dept_id INT,
  PRIMARY KEY (id),
  FOREIGN KEY (dept_id) REFERENCES departments(id)
);
```

### 2. INSERT INTO

Insert single or multiple rows into a table.

```sql
-- Single row insert
INSERT INTO students VALUES (1, 'Alice', 20);

-- Insert with column specification
INSERT INTO students (id, name) VALUES (2, 'Bob');

-- Multiple row insert
INSERT INTO students VALUES 
    (3, 'Charlie', 22),
    (4, 'David', 21),
    (5, 'Eve', 23);
```

### 3. SELECT

Query data with various filtering and aggregation options.

```sql
-- Select all columns
SELECT * FROM students;

-- Select specific columns
SELECT id, name FROM students;

-- INNER JOIN
SELECT *
FROM students
INNER JOIN departments
ON students.dept_id = departments.id;

-- LEFT JOIN
SELECT *
FROM students
LEFT JOIN departments
ON students.dept_id = departments.id;

-- RIGHT JOIN
SELECT *
FROM students
RIGHT JOIN departments
ON students.dept_id = departments.id;

-- With WHERE clause
SELECT * FROM students WHERE age > 21;

-- WHERE IN (subquery)
SELECT name
FROM students
WHERE dept_id IN (
  SELECT id FROM departments WHERE name = 'CSE'
);

-- EXISTS / NOT EXISTS (non-correlated)
SELECT * FROM departments
WHERE EXISTS (
  SELECT id FROM students WHERE dept_id = 10
);

SELECT * FROM departments
WHERE NOT EXISTS (
  SELECT id FROM students WHERE dept_id = 999
);

-- With ORDER BY
SELECT * FROM students ORDER BY age DESC;

-- With LIMIT
SELECT * FROM students LIMIT 5;

-- Aggregate functions
SELECT COUNT(*) FROM students;
SELECT AVG(age) FROM students;
SELECT SUM(grade) FROM enrollments;
SELECT MIN(age), MAX(age) FROM students;

-- GROUP BY with aggregation
SELECT course_id, AVG(grade) FROM enrollments GROUP BY course_id;

-- GROUP BY with HAVING
SELECT department, AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 60000;

-- Combined features
SELECT course_id, COUNT(*) FROM enrollments 
    WHERE grade > 3.0 
    GROUP BY course_id 
    ORDER BY COUNT(*) DESC 
    LIMIT 3;
```

### 4. UPDATE

Modify existing records.

```sql
-- Update with condition
UPDATE students SET age = 21 WHERE id = 1;

UPDATE students SET name = 'Alice Smith' WHERE name = 'Alice';
```

### 5. DELETE

Remove records from a table.

```sql
-- Delete with condition
DELETE FROM students WHERE age < 18;

DELETE FROM enrollments WHERE grade < 2.0;
```

### 6. DROP TABLE

Remove a table and all its data.

```sql
DROP TABLE students;
```

### 7. ALTER TABLE

Modify table structure dynamically.

```sql
-- Add single column
ALTER TABLE students ADD COLUMN email VARCHAR;

-- Add multiple columns
ALTER TABLE students ADD COLUMN gpa DOUBLE, city VARCHAR, major VARCHAR;

-- Drop column
ALTER TABLE students DROP COLUMN email;

-- Modify column datatype
ALTER TABLE students MODIFY COLUMN age DOUBLE;

-- Rename column
ALTER TABLE students RENAME COLUMN age TO student_age;

-- Rename table
ALTER TABLE students RENAME TO pupils;

-- Add primary key constraint
ALTER TABLE students ADD PRIMARY KEY (id);

-- Add primary key with CONSTRAINT keyword
ALTER TABLE students ADD CONSTRAINT PRIMARY KEY (id);

-- Add composite primary key
ALTER TABLE enrollments ADD PRIMARY KEY (student_id, course_id);

-- Drop primary key
ALTER TABLE students DROP PRIMARY KEY;

-- Drop primary key with CONSTRAINT keyword
ALTER TABLE students DROP CONSTRAINT PRIMARY KEY;
```

### 8. SHOW TABLES

List all tables in the database.

```sql
-- List all tables in the database
SHOW TABLES;
```

### 9. DESCRIBE

Show table structure and schema.

```sql
-- Show table structure
DESCRIBE students;

-- View column details and constraints
DESCRIBE enrollments;
```

### 10. TRUNCATE

Remove all data from a table while preserving structure.

```sql
-- Truncate with TABLE keyword
TRUNCATE TABLE students;

-- Truncate without TABLE keyword
TRUNCATE students;

-- Clear all enrollment data
TRUNCATE TABLE enrollments;
```

### 11. ANALYZE SCHEMA

Analyze schema quality using functional dependencies (FDs), candidate keys, and normal forms.

```sql
-- Run schema analysis
ANALYZE SCHEMA students;
```

If no FDs are stored for a table, NirvahaDB prompts for interactive FD input:

```text
No functional dependencies found. Please define FDs for table students.
FD > roll -> name, dept
FD > dept -> hod
FD >
```

Stored FDs are reused in future analysis runs.

Analysis output includes:
- Candidate key and attribute closures
- Prime and non-prime attribute classification
- 1NF, 2NF, 3NF checks
- Partial/transitive dependency detection
- Anomaly mapping and decomposition suggestions

For full theory and practical anomaly examples, see `NORMALIZATION_ANALYSIS_GUIDE.md`.

### 12. Adaptive Indexing (Automatic)

NirvahaDB includes an adaptive hybrid indexing system that optimizes repeated queries automatically.

- Hash index for equality queries (average `O(1)` lookup)
- Sorted index for range queries (binary search, `O(log n)` lookup)
- Automatic index creation after 3 repeated query patterns on the same column
- Transparent behavior: no query syntax changes required
- Uses a row-offset file to seek directly to matching rows (so Rows Scanned equals Index Hits when an index is used)

Example:

```sql
SELECT * FROM students WHERE id = 2;
SELECT * FROM students WHERE id = 3;
SELECT * FROM students WHERE id = 4;
-- After threshold, same-pattern queries use index automatically
```

For full indexing details, see `INDEX_README.md`.

### 13. Utility Commands

```sql
-- Clear the screen
CLEAR;

-- Show help
HELP;

-- Exit the CLI
EXIT;
```

## 🎓 Educational Mode

NirvahaDB features a unique **Educational Mode** that traces query execution step-by-step:

```sql
SET MODE EDUCATIONAL;

CREATE TABLE students (
  id INT,
  name VARCHAR,
  PRIMARY KEY (id)
);
```

**Output with Educational Traces:**

```
╔════════════════════════════════════════════════╗
║              TOKENIZER                         ║
╚════════════════════════════════════════════════╝
Breaking query into tokens
Tokens Generated:
CREATE | TABLE | students | ( | id | INT | , | name | VARCHAR | , | PRIMARY | KEY | ( | id | ) | )

╔════════════════════════════════════════════════╗
║              PARSER                            ║
╚════════════════════════════════════════════════╝
Operation Type : CREATE
Target Table   : students
Columns        : [('id', 'INT'), ('name', 'VARCHAR')]
Primary Key    : id

╔════════════════════════════════════════════════╗
║              EXECUTOR                          ║
╚════════════════════════════════════════════════╝
Operation Identified: CREATE

╔════════════════════════════════════════════════╗
║           STORAGE ENGINE                       ║
╚════════════════════════════════════════════════╝
Created Data File : data/students.tbl
Created Metadata : metadata/students.meta

✅ Table Created Successfully
```

Note for schema analysis:
- `ANALYZE SCHEMA` uses a dedicated normalization dashboard output.
- It intentionally skips tokenizer/parser/executor trace blocks for cleaner educational reading.

## 📁 Project Structure

```
NirvahaDB/
│
├── minidb.py              # Main entry point and REPL
├── tokenizer.py           # Lexical analyzer
├── executor.py            # Query execution coordinator
├── config.py              # Configuration management
├── utils.py               # Helper functions
├── visualizer.py          # Educational mode visualizations
│
├── parser/                # Modular parser
│   ├── __init__.py        # Main parse_query() with visualization
│   ├── create_parser.py   # CREATE TABLE parser
│   ├── insert_parser.py   # INSERT INTO parser
│   ├── select_parser.py   # SELECT query parser
│   ├── update_parser.py   # UPDATE parser
│   ├── delete_parser.py   # DELETE parser
│   ├── drop_parser.py     # DROP TABLE parser
│   ├── alter_parser.py    # ALTER TABLE parser 
│   ├── show_parser.py     # SHOW TABLES parser
│   ├── describe_parser.py # DESCRIBE TABLE parser
│   ├── truncate_parser.py # TRUNCATE TABLE parser
│   └── analyze_parser.py  # ANALYZE SCHEMA parser
│
├── storage/               # Modular storage engine 
│   ├── __init__.py        # Main execute_command() router
│   ├── create_storage.py  # CREATE TABLE storage handler
│   ├── insert_storage.py  # INSERT INTO storage handler
│   ├── select_storage.py  # SELECT query executor
│   ├── update_storage.py  # UPDATE storage handler
│   ├── delete_storage.py  # DELETE storage handler
│   ├── drop_storage.py    # DROP TABLE storage handler
│   ├── alter_storage.py   # ALTER TABLE storage handler
│   ├── show_storage.py    # SHOW TABLES storage handler
│   ├── describe_storage.py# DESCRIBE TABLE storage handler
│   ├── truncate_storage.py# TRUNCATE TABLE storage handler
│   ├── join_engine.py      # Nested-loop join engine
│   ├── subquery_engine.py  # Subquery execution helpers
│   └── foreign_key_manager.py # FK enforcement and metadata helpers
│
├── normalization/         # Schema normalization analysis module
│   ├── __init__.py
│   ├── fd_manager.py
│   ├── closure.py
│   ├── key_finder.py
│   ├── normal_form_checker.py
│   ├── anomaly_detector.py
│   ├── normalization_utils.py
│   └── schema_analyzer.py
│
├── index/                 # Indexing system
│   ├── __init__.py
│   ├── hash_index.py
│   ├── index_manager.py
│   ├── index_persistence.py
│   ├── index_utils.py
│   ├── query_stats.py
│   └── sorted_index.py
│   └── <db_name>/          # Generated per-database index artifacts (ignored in Git)
│
├── data/                  # Table data files (.tbl)
├── metadata/              # Table metadata files (.meta)
│
└── README.md              # This file
```

## 🔧 Technical Implementation

### Key Components

1. **Tokenizer** (`tokenizer.py`)
   - Lexical analysis
   - Keyword recognition
   - String literal handling
   - Symbol parsing

2. **Parser Module** (`parser/`)
  - 12 specialized parsers (one per command type)
   - Syntax validation and AST generation
   - Command structure creation
   - Educational trace generation via `_display_parsed_command()`
   - Average 60-80 lines per parser file

3. **Executor** (`executor.py`)
   - Command routing to storage handlers
   - Operation orchestration
   - Error handling

4. **Storage Engine Module** (`storage/`)
   - 11 specialized storage handlers (one per command type)
   - File-based persistence
   - Metadata management (JSON)
   - CRUD operations with constraint enforcement
   - Indexing integration
   - Average 78 lines per storage file

5. **Normalization Module** (`normalization/`)
  - Functional dependency persistence and retrieval
  - Attribute closure and candidate key computation
  - 1NF/2NF/3NF evaluation
  - Partial/transitive dependency detection
  - Anomaly mapping and decomposition guidance

### Data Storage Format

**Data Files (`.tbl`)**: CSV-like format
```
1,Alice,20,3.8
2,Bob,21,3.5
3,Charlie,22,3.9
```

**Metadata Files (`.meta`)**: JSON format
```json
{
  "columns": [
    ["id", "INT"],
    ["name", "VARCHAR"],
    ["age", "INT"],
    ["gpa", "DOUBLE"]
  ],
  "primary_key": ["id"],
  "foreign_keys": [
    {
      "column": "dept_id",
      "references_table": "departments",
      "references_column": "id"
    }
  ]
}
```

Note: EXISTS/NOT EXISTS currently support non-correlated subqueries only.

## 🤝 Contributing

Contributions are welcome! Here's how you can help:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Your Name**
- GitHub: [Aditya Raj](https://github.com/TasteTheThunder)
- LinkedIn: [Aditya Raj](https://www.linkedin.com/in/aditya-raj3112/)
- Email: raj285003@gmail.com

## 🙏 Acknowledgments

- Inspired by PostgreSQL and MySQL architectures
- Built as a learning project to understand database internals
- Special thanks to the open-source community

---

<div align="center">

**⭐ Star this repository if you find it helpful!**

**📚 Perfect for placement preparation and portfolio showcasing!**

Made with ❤️ for learning and education purpose

</div>
