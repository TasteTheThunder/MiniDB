# NirvahaDB Supported Queries (Examples Using students)

All commands require a trailing semicolon.

## Database Commands

```sql
CREATE DATABASE college_db;
SHOW DATABASES;
USE college_db;
DROP DATABASE college_db;
```

## Table Commands

### CREATE TABLE

```sql
CREATE TABLE students (
  id INT,
  name VARCHAR,
  age INT,
  gpa DOUBLE,
  PRIMARY KEY (id)
);

CREATE TABLE enrollments (
  student_id INT,
  course_id INT,
  grade DOUBLE,
  PRIMARY KEY (student_id, course_id)
);

CREATE TABLE students (
  id INT PRIMARY KEY,
  name VARCHAR,
  age INT
);
```

### DROP TABLE

```sql
DROP TABLE students;
```

### SHOW TABLES

```sql
SHOW TABLES;
```

### DESCRIBE TABLE

```sql
DESCRIBE students;
```

### TRUNCATE

```sql
TRUNCATE TABLE students;
TRUNCATE students;
```

## INSERT

Single row insert (all columns):
```sql
INSERT INTO students VALUES (1, 'Alice', 20, 3.7);
```

Insert with column list:
```sql
INSERT INTO students (id, name) VALUES (2, 'Bob');
```

Multiple row insert:
```sql
INSERT INTO students VALUES
  (3, 'Cara', 21, 3.9),
  (4, 'Dan', 22, 3.2),
  (5, 'Eve', 23, 3.5);
```

## SELECT

Select all columns:
```sql
SELECT * FROM students;
```

Select specific columns:
```sql
SELECT id, name FROM students;
```

WHERE (equality):
```sql
SELECT * FROM students WHERE age = 21;
```

WHERE (greater than):
```sql
SELECT * FROM students WHERE age > 21;
```

WHERE (less than):
```sql
SELECT * FROM students WHERE age < 21;
```

WHERE (greater than or equal):
```sql
SELECT * FROM students WHERE age >= 21;
```

WHERE (less than or equal):
```sql
SELECT * FROM students WHERE age <= 21;
```

WHERE (not equal):
```sql
SELECT * FROM students WHERE age != 21;
```

ORDER BY:
```sql
SELECT * FROM students ORDER BY age ASC;
SELECT * FROM students ORDER BY age DESC;
```

GROUP BY with aggregate:
```sql
SELECT age, COUNT(*) FROM students GROUP BY age;
SELECT age, AVG(gpa) FROM students GROUP BY age;
```

Aggregates (no GROUP BY):
```sql
SELECT COUNT(*) FROM students;
SELECT SUM(gpa) FROM students;
SELECT AVG(gpa) FROM students;
SELECT MIN(gpa) FROM students;
SELECT MAX(gpa) FROM students;
```

ORDER BY aggregate:
```sql
SELECT age, COUNT(*) FROM students GROUP BY age ORDER BY COUNT(*) DESC;
```

LIMIT:
```sql
SELECT * FROM students LIMIT 5;
```

Combined example:
```sql
SELECT age, COUNT(*) FROM students
  WHERE gpa > 3.0
  GROUP BY age
  ORDER BY COUNT(*) DESC
  LIMIT 3;
```

## UPDATE

Single-column update with condition:
```sql
UPDATE students SET gpa = 3.8 WHERE id = 1;
UPDATE students SET name = 'Alice Smith' WHERE name = 'Alice';
```

## DELETE

Delete with condition:
```sql
DELETE FROM students WHERE id = 1;
DELETE FROM students WHERE age < 18;
```

## ALTER TABLE

Add column(s):
```sql
ALTER TABLE students ADD COLUMN email VARCHAR;
ALTER TABLE students ADD COLUMN city VARCHAR, major VARCHAR;
ALTER TABLE students ADD age INT;
```

Drop column:
```sql
ALTER TABLE students DROP COLUMN email;
```

Modify column datatype:
```sql
ALTER TABLE students MODIFY COLUMN age DOUBLE;
ALTER TABLE students MODIFY age DOUBLE;
```

Rename column:
```sql
ALTER TABLE students RENAME COLUMN age TO student_age;
```

Rename table:
```sql
ALTER TABLE students RENAME TO pupils;
```

Add primary key:
```sql
ALTER TABLE students ADD PRIMARY KEY (id);
ALTER TABLE students ADD PRIMARY KEY (id, name);
ALTER TABLE students ADD CONSTRAINT PRIMARY KEY (id);
```

Drop primary key:
```sql
ALTER TABLE students DROP PRIMARY KEY;
ALTER TABLE students DROP CONSTRAINT PRIMARY KEY;
```

## Normalization

```sql
ANALYZE SCHEMA students;
```

## Indexing

Indexing is automatic. Run repeated queries on the same column to trigger index creation.
Example:
```sql
SELECT * FROM students WHERE id = 1;
SELECT * FROM students WHERE id = 2;
SELECT * FROM students WHERE id = 3;
```

Range queries trigger sorted index creation after repeated use:
```sql
SELECT * FROM students WHERE age > 20;
SELECT * FROM students WHERE age >= 21;
SELECT * FROM students WHERE age < 25;
```

## Utility

```sql
CLEAR;
HELP;
EXIT;
```
