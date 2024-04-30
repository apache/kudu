---
layout: post
title: "Introducing Auto-incrementing Column in Kudu"
author: Abhishek Chennaka
---
<!--more-->

# Introduction

Kudu has a strict requirement for a primary key presence in a table. This is primarily to help in
point lookups and support DELETE and UPDATE operations on the table data. There are situations where
users are unable to define a unique primary key in their data set and have to either introduce
additional columns to be a part of the primary key or define a new column and maintain it to enforce
uniqueness. Kudu 1.17 has introduced support for the auto-incrementing column to have partially
defined primary keys (keys which are not unique across the table) during table creation. This way a
user does not have to worry about the uniqueness constraint when defining a primary key.

# Implementation Details

When a primary key is partially defined, Kudu internally creates a new column named
“auto_incrementing_id” as a part of the primary key. The column is populated with a monotonically
increasing counter. The system updates the counter value upon every INSERT operation and populates
the "auto_incrementing_id" column on the server side. The counter is partition-local i.e. every
tablet has its own counter.

## Server Side

When a user writes data into a table with the auto-incrementing column, the server makes sure that
no INSERT operations have the “auto_incrementing_id” column field value set and populates this
column value. The highest value of the counter written into the "auto_incrementing_id" column
until any particular point is stored in memory and this is used to set the column value for the
next INSERT operation.

## Client Side

When creating a table without an explicitly defined primary key, users will have to declare the key
as non-unique. Internally, the client builds a schema with an extra column named
“auto_incrementing_id” and forwards the request to the server where the table is created. For
INSERT operations, the user shouldn’t specify the “auto_incrementing_id” column value as it will be
populated on the server side.

### Impala Integration

In Impala, the new column is not exposed to the user by default. This is due to the reason that it
is not a part of the user table schema. The below query will not return the “auto_incrementing_id”
column
SELECT \* FROM &lt;tablename&gt;

If the auto-incrementing column's data is needed, the column name has to be specifically requested.
The below query will return the column values:
SELECT \*, auto_incrementing_id FROM &lt;tablename&gt;

#### Examples

Create a table with two columns and two hash partitions:
```
default> CREATE TABLE demo_table(id INT NON UNIQUE PRIMARY KEY, name STRING) PARTITION BY HASH (id) PARTITIONS 2 STORED AS KUDU;
Query: CREATE TABLE demo_table(id INT NON UNIQUE PRIMARY KEY, name STRING) PARTITION BY HASH (id) PARTITIONS 2 STORED AS KUDU
+-------------------------+
| summary                 |
+-------------------------+
| Table has been created. |
+-------------------------+
Fetched 1 row(s) in 3.94s
```

Describe the table:
```
default> DESCRIBE demo_table;
Query: DESCRIBE demo_table
+----------------------+--------+---------+-------------+------------+----------+---------------+---------------+---------------------+------------+
| name                 | type   | comment | primary_key | key_unique | nullable | default_value | encoding      | compression         | block_size |
+----------------------+--------+---------+-------------+------------+----------+---------------+---------------+---------------------+------------+
| id                   | int    |         | true        | false      | false    |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| auto_incrementing_id | bigint |         | true        | false      | false    |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
| name                 | string |         | false       |            | true     |               | AUTO_ENCODING | DEFAULT_COMPRESSION | 0          |
+----------------------+--------+---------+-------------+------------+----------+---------------+---------------+---------------------+------------+
```

Insert rows with duplicate partial primary key column values:
```
default> INSERT INTO demo_table VALUES (1, 'John'), (2, 'Bob'), (3, 'Mary'), (1, 'Joe');
Query: INSERT INTO demo_table VALUES (1, 'John'), (2, 'Bob'), (3, 'Mary'), (1, 'Joe')
..
Modified 4 row(s), 0 row error(s) in 0.41s
```

Scan the table (notice the duplicate values in the 'id' column):
```
default> SELECT * FROM demo_table;
Query: SELECT * FROM demo_table
..
+----+------+
| id | name |
+----+------+
| 3  | Mary |
| 1  | John |
| 1  | Joe  |
| 2  | Bob  |
+----+------+
Fetched 4 row(s) in 0.24s
```

Explicitly specify the auto-incrementing column name to fetch the column values:
```
default> SELECT *, auto_incrementing_id FROM demo_table;
Query: SELECT *, auto_incrementing_id FROM demo_table
..
+----+------+----------------------+
| id | name | auto_incrementing_id |
+----+------+----------------------+
| 3  | Mary | 1                    |
| 1  | John | 1                    |
| 1  | Joe  | 2                    |
| 2  | Bob  | 2                    |
+----+------+----------------------+
Fetched 4 row(s) in 0.24s
```

Update and Delete rows:
```
default> UPDATE demo_table SET name='Matt' WHERE id=3;
Query: UPDATE demo_table SET name='Matt' WHERE id=3
Modified 1 row(s), 0 row error(s) in 1.99s
default> UPDATE demo_table SET name='Liam' WHERE id=1;
Query: UPDATE demo_table SET name='Liam' WHERE id=1
Modified 2 row(s), 0 row error(s) in 2.15s
default> DELETE FROM demo_table where id=2;
Query: DELETE FROM demo_table where id=2;
Modified 1 row(s), 0 row error(s) in 1.40s
```

Scan all the columns of the table:
```
default> SELECT *, auto_incrementing_id FROM demo_table;
Query: SELECT *, auto_incrementing_id FROM demo_table
..
+----+------+----------------------+
| id | name | auto_incrementing_id |
+----+------+----------------------+
| 3  | Matt | 1                    |
| 1  | Liam | 1                    |
| 1  | Liam | 2                    |
+----+------+----------------------+
Fetched 3 row(s) in 0.20s
```

#### Limitations

Impala doesn’t support UPSERT operations on tables with the auto-incrementing column as of writing
this article.

### Kudu clients (Java, C++, Python)

Unlike in Impala, scanning the table fetches all the table data including the auto incrementing column.
There is no need to explicitly request the auto-incrementing column.

There is also support for UPSERT operations but the user has to provide the entire primary key
including the value for the auto-incrementing column. If the row is present, it will be considered a
regular UPDATE operation. If the row is not present, it is considered an INSERT operation.

#### Examples

<https://github.com/apache/kudu/blob/master/examples/cpp/non_unique_primary_key.cc>

<https://github.com/apache/kudu/blob/master/examples/python/basic-python-example/non_unique_primary_key.py>

## Backup and Restore

The Kudu backup tool from Kudu 1.17 and later supports backing up tables with the
auto-incrementing column. The prior backup tools will fail with an error message -
"auto_incrementing_id is a reserved column name" during backup and restore operations.

The backed up data (from Kudu 1.17 and later) includes the auto-incrementing column in the table
schema and the column values as well. Restoring this backed up table with the Kudu restore tool
will create a table with the auto-incrementing column and the column values identical to the
original source table.
