# Existing SQLite Structure and Behavior

This document is a complete behavior extraction from the C SQLite source code. It
describes **what SQLite does** -- its file formats, data structures, instruction
set, built-in functions, limits, locking protocol, and extension surface area.
This is the authoritative spec for any reimplementation effort.

---

## 1. Database File Format

### 1.1 File Header (100 bytes at offset 0)

Every SQLite database file begins with a 100-byte header. All multi-byte integers
in the header are stored in big-endian (network) byte order.

| Byte Offset | Size | Description |
|-------------|------|-------------|
| 0-15 | 16 | Magic string: `"SQLite format 3\000"` (including the null terminator) |
| 16-17 | 2 | Page size in bytes. A value of `1` encodes a page size of 65536. Valid power-of-two values: 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536 (stored as 1). |
| 18 | 1 | File format write version: `1` = legacy (rollback journal), `2` = WAL mode. |
| 19 | 1 | File format read version: `1` = legacy, `2` = WAL mode. |
| 20 | 1 | Reserved space at the end of each page (typically 0). Used by extensions such as SEE encryption. |
| 21 | 1 | Maximum embedded payload fraction (must be 64). |
| 22 | 1 | Minimum embedded payload fraction (must be 32). |
| 23 | 1 | Leaf payload fraction (must be 32). |
| 24-27 | 4 | File change counter. Incremented on every transaction commit. Used by readers to detect changes. |
| 28-31 | 4 | Database size in pages. Zero means "compute from file size." Set correctly on every commit. |
| 32-35 | 4 | Page number of the first freelist trunk page (0 if freelist is empty). |
| 36-39 | 4 | Total number of freelist pages. |
| 40-43 | 4 | Schema cookie. Incremented when the schema changes. Prepared statements check this to detect stale schemas. |
| 44-47 | 4 | Schema format number. Current value is `4`. Values 1-4 are recognized; 1 was the original format. |
| 48-51 | 4 | Default page cache size (as suggested by `PRAGMA default_cache_size`). |
| 52-55 | 4 | Largest root b-tree page number in auto-vacuum or incremental-vacuum mode. Zero otherwise. |
| 56-59 | 4 | Text encoding: `1` = UTF-8, `2` = UTF-16le, `3` = UTF-16be. |
| 60-63 | 4 | User version (set and read by `PRAGMA user_version`). |
| 64-67 | 4 | Incremental vacuum mode flag (non-zero = enabled). |
| 68-71 | 4 | Application ID (set and read by `PRAGMA application_id`). |
| 72-91 | 20 | Reserved for expansion (must be zero). |
| 92-95 | 4 | Version-valid-for number. The change counter value at the time the database size (bytes 28-31) was last updated. |
| 96-99 | 4 | SQLite version number that most recently modified the file, in the format `X*1000000 + Y*1000 + Z` (e.g., 3039004 for 3.39.4). |

### 1.2 B-Tree Page Format

SQLite stores all data in a B-tree structure. There are four page types, identified
by a single-byte flag at the beginning of the page header:

| Flag Byte | Page Type |
|-----------|-----------|
| `0x02` (2) | Interior index b-tree page |
| `0x05` (5) | Interior table b-tree page |
| `0x0a` (10) | Leaf index b-tree page |
| `0x0d` (13) | Leaf table b-tree page |

**Page header layout:**

| Offset | Size | Description |
|--------|------|-------------|
| 0 | 1 | Page type flag (2, 5, 10, or 13) |
| 1-2 | 2 | Offset to the first freeblock on the page (0 if none) |
| 3-4 | 2 | Number of cells on the page |
| 5-6 | 2 | Offset to the first byte of the cell content area (0 means 65536) |
| 7 | 1 | Number of fragmented free bytes in the cell content area |
| 8-11 | 4 | Right-most child pointer (interior pages only; absent on leaf pages) |

Leaf pages have an 8-byte header. Interior pages have a 12-byte header (the extra
4 bytes store the right-most child page number).

**Page 1 special case:** Page 1 of the database file contains the 100-byte file
header before the page header. The cell pointer offsets on page 1 account for this
100-byte prefix.

**Cell pointer array:** Immediately after the page header is an array of 2-byte
integers, one per cell, giving the byte offset of each cell within the page. Cells
are stored from the end of the page growing backward toward the cell pointer array.

**Unallocated space** exists between the end of the cell pointer array and the
beginning of the cell content area. **Freeblocks** form a linked list of reclaimed
space within the cell content area.

**Table B-Tree cells (leaf, flag 13):**
- Payload size (varint)
- Rowid (varint)
- Payload data (record format)
- Overflow page number (4-byte big-endian u32, only if payload exceeds local capacity)

**Table B-Tree cells (interior, flag 5):**
- Left child page number (4-byte big-endian u32)
- Rowid key (varint)

**Index B-Tree cells (leaf, flag 10):**
- Payload size (varint)
- Payload data (record format containing the indexed columns plus the rowid)
- Overflow page number (if needed)

**Index B-Tree cells (interior, flag 2):**
- Left child page number (4-byte big-endian u32)
- Payload size (varint)
- Payload data
- Overflow page number (if needed)

**Overflow pages:** When a cell payload is too large to fit on a single page,
the excess is stored on a linked list of overflow pages. Each overflow page
begins with a 4-byte pointer to the next overflow page (0 if last), followed
by the overflow payload data.

The maximum local payload before overflow is computed from the page usable size
(`U`), the max embedded payload fraction (`M = 64`), and the min embedded
payload fraction (`m = 32`):
- Max local payload = `(U - 12) * M / 255 - 23` for table b-trees
- If payload exceeds max local, the amount stored locally is `min(M, (U-12)*m/255-23)`

### 1.3 Record Format

A record (also called a "payload") stores one row of a table or one entry of
an index. Records use a self-describing format:

1. **Header size** (varint): The total number of bytes in the header, including
   this size field itself.
2. **Serial type array**: A sequence of varints, one per column, each encoding
   both the data type and the size of the corresponding value.
3. **Data area**: The column values concatenated in order, with sizes determined
   by the serial types.

**Serial type encoding:**

| Serial Type | Meaning | Content Size (bytes) |
|-------------|---------|---------------------|
| 0 | NULL | 0 |
| 1 | 8-bit signed integer | 1 |
| 2 | 16-bit big-endian signed integer | 2 |
| 3 | 24-bit big-endian signed integer | 3 |
| 4 | 32-bit big-endian signed integer | 4 |
| 5 | 48-bit big-endian signed integer | 6 |
| 6 | 64-bit big-endian signed integer | 8 |
| 7 | IEEE 754 64-bit float (big-endian) | 8 |
| 8 | Integer constant 0 | 0 |
| 9 | Integer constant 1 | 0 |
| 10-11 | Reserved for internal use | - |
| N >= 12, even | BLOB of length `(N-12)/2` | `(N-12)/2` |
| N >= 13, odd | TEXT of length `(N-13)/2` | `(N-13)/2` |

**Varint encoding:** SQLite uses a variable-length integer encoding called
"varint" throughout the file format. A varint is 1-9 bytes. For the first 8
bytes, the high bit is a continuation flag; the lower 7 bits contribute to
the value. If all 8 continuation bits are set, the 9th byte provides 8 full
bits (no continuation flag), yielding a maximum of 64 bits.

### 1.4 WAL (Write-Ahead Log) Format

WAL mode replaces the rollback journal with an append-only log file. The WAL
file has the suffix `-wal` appended to the database filename.

**WAL file header (32 bytes):**

| Offset | Size | Description |
|--------|------|-------------|
| 0-3 | 4 | Magic number: `0x377f0682` (big-endian) or `0x377f0683` (little-endian). The endianness of the magic number indicates the byte order of checksums. |
| 4-7 | 4 | WAL format version (currently `3007000`). |
| 8-11 | 4 | Database page size. |
| 12-15 | 4 | Checkpoint sequence number. |
| 16-19 | 4 | Salt-1: random value copied from the database file change counter at WAL creation. |
| 20-23 | 4 | Salt-2: random value. |
| 24-27 | 4 | Checksum part 1 (over bytes 0-23). |
| 28-31 | 4 | Checksum part 2 (over bytes 0-23). |

**WAL frame header (24 bytes):** Each frame stores one modified database page.

| Offset | Size | Description |
|--------|------|-------------|
| 0-3 | 4 | Page number. |
| 4-7 | 4 | For commit frames: the database size in pages after the commit. Otherwise 0. |
| 8-11 | 4 | Salt-1 (must match the WAL header salt-1). |
| 12-15 | 4 | Salt-2 (must match the WAL header salt-2). |
| 16-19 | 4 | Checksum part 1 (cumulative, over the frame header and page data, chained from the previous frame). |
| 20-23 | 4 | Checksum part 2. |

After the frame header is the raw page data (page-size bytes).

**Checksum algorithm:** Checksums are computed using a custom algorithm that
processes data in 8-byte chunks using two 32-bit accumulators (`s1` and `s2`).
Each chunk is split into two 32-bit words and combined into the accumulators
using addition and unsigned overflow. The byte order of the two words within
each chunk is determined by the WAL header magic number.

**WAL index (shared memory):** The WAL index is a separate shared-memory region
(file suffix `-shm`) used to coordinate concurrent readers. It contains:
- A hash table mapping page numbers to frame numbers for efficient lookups.
- Reader "marks" that track which frames each reader is using.
- Lock bytes for coordinating readers and writers.

The WAL index allows readers to find the most recent version of any page
without scanning the entire WAL file.

**Checkpoint:** Periodically, modified pages from the WAL are written back to
the database file. This is called a checkpoint. The default auto-checkpoint
threshold is 1000 frames. Checkpoint modes:
- `PASSIVE`: Checkpoint as many frames as possible without waiting for readers.
- `FULL`: Wait until all readers are finished, then checkpoint all frames.
- `RESTART`: Like FULL, then also reset the WAL file to the beginning.
- `TRUNCATE`: Like RESTART, then also truncate the WAL file to zero bytes.

### 1.5 Rollback Journal Format

In legacy journal mode (non-WAL), SQLite uses a rollback journal file (suffix
`-journal`) to provide atomic commit and rollback.

The journal file contains:
1. An 8-byte magic header: `\xd9\xd5\x05\xf9\x20\xa1\x63\xd7`
2. A record count (4 bytes), page count, and other bookkeeping fields in
   the journal header (variable layout).
3. A sequence of page records, each containing:
   - Page number (4 bytes)
   - Original page data (page-size bytes)
   - Checksum (4 bytes)

On rollback, the original pages are copied from the journal back into the
database file, restoring it to its pre-transaction state.

### 1.6 Freelist

Unused pages in the database are tracked by a freelist, organized as a linked
list of trunk pages. Each trunk page contains:
- A 4-byte pointer to the next trunk page (0 if last).
- A 4-byte count of leaf page numbers stored on this trunk page.
- An array of 4-byte page numbers (the leaf pages).

When pages are deleted (e.g., by `DROP TABLE` or `DELETE`), they are added to
the freelist rather than being immediately reused. New allocations prefer
freelist pages over growing the file.

### 1.7 Pointer Map (Auto-Vacuum Mode)

When auto-vacuum is enabled, the database contains pointer-map pages that track
the parent of each page. This allows the database to be compacted by moving
pages and updating their parent pointers. Pointer-map pages appear at specific
intervals and contain 5-byte entries (1-byte type + 4-byte parent page number)
for each trackable page.

---

## 2. Core Data Structures

These are the fundamental runtime objects that drive SQLite behavior. They are
described here in terms of their roles and relationships, not their C-level
field layouts.

### 2.1 sqlite3 (Database Connection)

The top-level connection object. One is created per `sqlite3_open()` call.
It holds:
- The list of attached databases (main, temp, plus any ATTACHed databases).
- The default text encoding (UTF-8, UTF-16le, or UTF-16be).
- Registered custom functions, collations, and virtual table modules.
- Authorization and tracing callbacks.
- Error state (error code, error message).
- Busy handler and timeout.
- The schema for each attached database.
- Transaction nesting state.
- Flags controlling behavior (e.g., foreign keys enabled, recursive triggers).

### 2.2 Btree / BtCursor

**Btree** represents one open B-tree file (database file). Each attached database
has one Btree object. It wraps the Pager and provides B-tree operations:
- Begin/commit/rollback transactions.
- Create and destroy tables/indexes (by root page number).
- Open cursors for traversal.

**BtCursor** is a positioned iterator over a B-tree. Operations:
- Move to a specific key (`MovetoUnpacked`).
- Move to first/last entry.
- Step forward (`Next`) or backward (`Previous`).
- Read the current key and data.
- Insert or delete the entry at the current position.

A cursor remembers its position even when the underlying tree is modified by
other cursors (using saved position restoration).

### 2.3 Pager

The Pager manages the interface between the B-tree layer and the operating system.
It is responsible for:
- Reading and writing database pages from/to the file.
- Maintaining the page cache (an in-memory cache of recently used pages).
- Managing the rollback journal or WAL.
- Implementing the transaction lifecycle: shared locks for reads, reserved/exclusive
  locks for writes.
- Ensuring crash-safe atomic commits via the journal or WAL.
- Enforcing the `synchronous` pragma setting for durability guarantees.

### 2.4 Vdbe (Virtual Database Engine / Prepared Statement)

A Vdbe is a compiled bytecode program produced by the SQL compiler. Each prepared
statement (`sqlite3_stmt`) is a Vdbe. It contains:
- An array of opcodes (the program).
- An array of `Mem` registers (the working memory).
- A list of open cursors.
- Bound parameter values.
- Column metadata for result rows.

Execution is a simple fetch-decode-execute loop over the opcode array. The VDBE
is a register-based machine: operands reference numbered registers rather than
a stack.

### 2.5 Mem (sqlite3_value)

A `Mem` object is a single register in the VDBE. It can hold any SQLite value:
- NULL
- 64-bit signed integer
- 64-bit IEEE 754 floating point
- Text (with encoding: UTF-8, UTF-16le, or UTF-16be)
- Blob (arbitrary bytes)

A Mem may cache multiple representations simultaneously (e.g., both the integer
and text form of a number). It tracks which representations are currently valid
via flags.

### 2.6 Table / Column / Index

**Table** describes a table's schema:
- Table name.
- Column list (names, types, default values, constraints).
- The root page number for the table's B-tree.
- Whether it is a WITHOUT ROWID table, a virtual table, a view, etc.
- Associated triggers.

**Column** describes one column:
- Name and declared type string.
- Affinity (TEXT, NUMERIC, INTEGER, REAL, BLOB/NONE).
- Default value expression.
- NOT NULL and other constraints.
- Whether it is part of the primary key.

**Index** describes an index:
- Index name and the table it indexes.
- The list of indexed columns (or expressions).
- Root page number for the index's B-tree.
- Whether it is a UNIQUE index.
- Partial index WHERE clause (if any).

### 2.7 Schema

Each attached database has a Schema object caching the parsed representation of
`sqlite_master` (the catalog table). It holds:
- A hash table of all Table objects.
- A hash table of all Index objects.
- A hash table of all Trigger objects.
- The schema cookie value at the time the schema was last loaded.

When the schema cookie in the database file changes, the schema is reloaded.

---

## 3. SQL Grammar Coverage

SQLite implements a substantial subset of SQL. The grammar (defined in `parse.y`)
supports the following statement types:

### 3.1 Data Manipulation Language (DML)

- **SELECT**: Full query support including:
  - `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
  - `DISTINCT` and `ALL`
  - Joins: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`
  - Subqueries (scalar, table-valued, correlated)
  - `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT` (compound SELECT)
  - `VALUES` clause as a standalone query
  - Window functions with `OVER` clause (frame specifications: `ROWS`, `RANGE`, `GROUPS`)
  - Common Table Expressions (`WITH` / `WITH RECURSIVE`)

- **INSERT**: `INSERT INTO ... VALUES (...)`, `INSERT INTO ... SELECT ...`,
  `INSERT OR REPLACE`, `INSERT OR IGNORE`, and all other conflict resolution
  clauses. Also supports `UPSERT` via `ON CONFLICT` clause. `DEFAULT VALUES` is supported.
  `RETURNING` clause is supported.

- **UPDATE**: `UPDATE ... SET ... WHERE ...`, with `UPDATE OR REPLACE`, etc.
  Supports `FROM` clause for update-from-join. `RETURNING` clause is supported.
  `ORDER BY` and `LIMIT` are supported with compile-time option.

- **DELETE**: `DELETE FROM ... WHERE ...`. `RETURNING` clause is supported.
  `ORDER BY` and `LIMIT` are supported with compile-time option.

- **REPLACE**: Syntactic sugar for `INSERT OR REPLACE`.

### 3.2 Data Definition Language (DDL)

- **CREATE TABLE**: Column definitions with type names, constraints
  (`PRIMARY KEY`, `NOT NULL`, `UNIQUE`, `CHECK`, `DEFAULT`, `COLLATE`,
  `REFERENCES` for foreign keys, `GENERATED ALWAYS AS` for generated columns).
  Table constraints. `WITHOUT ROWID` tables. `STRICT` tables. `IF NOT EXISTS`.
  `CREATE TABLE ... AS SELECT ...`.

- **CREATE INDEX**: Regular and `UNIQUE` indexes. Partial indexes (`WHERE` clause).
  Indexes on expressions. `IF NOT EXISTS`.

- **CREATE VIEW**: `CREATE VIEW name AS SELECT ...`. `IF NOT EXISTS`.
  `CREATE TEMP VIEW`.

- **CREATE TRIGGER**: `BEFORE`, `AFTER`, `INSTEAD OF` triggers on
  `INSERT`, `UPDATE`, `DELETE`. `FOR EACH ROW`. `WHEN` clause. Trigger
  body can contain multiple statements. `IF NOT EXISTS`.

- **CREATE VIRTUAL TABLE**: `CREATE VIRTUAL TABLE ... USING module(args)`.

- **DROP TABLE / INDEX / VIEW / TRIGGER**: `IF EXISTS` supported.

- **ALTER TABLE**: `RENAME TO`, `RENAME COLUMN`, `ADD COLUMN`, `DROP COLUMN`.

### 3.3 Transaction Control

- `BEGIN` / `BEGIN DEFERRED` / `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`
- `COMMIT` / `END`
- `ROLLBACK`
- `SAVEPOINT name`
- `RELEASE name`
- `ROLLBACK TO name`

### 3.4 Database Management

- `ATTACH DATABASE filename AS name`
- `DETACH DATABASE name`
- `REINDEX`
- `ANALYZE`
- `VACUUM` (with optional `INTO filename`)
- `PRAGMA name` / `PRAGMA name = value` / `PRAGMA name(value)`
- `EXPLAIN` / `EXPLAIN QUERY PLAN`

### 3.5 Expression Syntax

- Literal values: integers, floats, strings, blobs (`X'hex'`), NULL, TRUE, FALSE
- Column references: `table.column`, `schema.table.column`
- Unary operators: `-`, `+`, `~`, `NOT`
- Binary operators: `||`, `*`, `/`, `%`, `+`, `-`, `<<`, `>>`, `&`, `|`,
  `<`, `<=`, `>`, `>=`, `=`, `==`, `!=`, `<>`, `IS`, `IS NOT`, `AND`, `OR`
- `BETWEEN ... AND ...`
- `IN (value-list)`, `IN (subquery)`, `IN table`
- `LIKE`, `GLOB`, `REGEXP`, `MATCH` (with optional `ESCAPE`)
- `CASE WHEN ... THEN ... ELSE ... END`
- `CAST(expr AS type)`
- `EXISTS (subquery)`
- `COLLATE collation-name`
- `expr IS NULL`, `expr IS NOT NULL`, `expr ISNULL`, `expr NOTNULL`
- Aggregate function calls
- Window function calls with `OVER` clause
- `RAISE(IGNORE)`, `RAISE(ROLLBACK, msg)`, etc. (inside triggers)
- `->>` and `->` operators (JSON extraction, mapped to functions)

### 3.6 Type Affinity Rules

SQLite uses type affinity rather than strict typing. The five affinities are:

1. **TEXT**: Prefers storing data as text.
2. **NUMERIC**: Attempts to convert text to integer or real.
3. **INTEGER**: Like NUMERIC but prefers integer over real when the value is exact.
4. **REAL**: Forces integer values to floating point representation.
5. **BLOB** (also called NONE): No conversion; stores data as-is.

Affinity is determined from the declared column type name using these rules
(applied in order):
1. If the type contains "INT" -> INTEGER affinity.
2. If the type contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity.
3. If the type contains "BLOB" or no type is specified -> BLOB affinity.
4. If the type contains "REAL", "FLOA", or "DOUB" -> REAL affinity.
5. Otherwise -> NUMERIC affinity.

---

## 4. VDBE Opcodes

The Virtual Database Engine (VDBE) executes a program of bytecoded instructions.
There are approximately 190 opcodes. Each instruction has a 3-operand format
(`P1`, `P2`, `P3`) with two additional operand fields (`P4`, `P5`) for extended
parameters.

Below is a complete listing of all opcodes, organized by functional category.

### 4.1 Initialization and Control Flow

| Opcode | Behavior |
|--------|----------|
| `Init` | Initialize the VDBE program; jump to address P2. Used as the entry point. |
| `Goto` | Unconditional jump to address P2. |
| `Gosub` | Store the current address in register P1, then jump to P2. |
| `Return` | Jump to the address stored in register P1 (return from subroutine). |
| `InitCoroutine` | Initialize a coroutine. Store return address in P1, jump to P2 (or fall through if P2=0). |
| `EndCoroutine` | Terminate a coroutine. Jump to the address stored in register P1. |
| `Yield` | Swap program counters with the coroutine identified by register P1. |
| `Halt` | Terminate execution. P1 is the result code, P2 is the error action. |
| `HaltIfNull` | Like Halt, but only if register P3 is NULL. |
| `Jump` | Jump to P1, P2, or P3 depending on the result of the most recent comparison. |
| `Once` | Jump to P2 only the first time this instruction is reached. Subsequent executions fall through. |
| `If` | Jump to P2 if register P1 is true (non-zero and non-NULL). |
| `IfNot` | Jump to P2 if register P1 is false (zero) or NULL. |
| `IsNull` | Jump to P2 if register P1 is NULL. |
| `NotNull` | Jump to P2 if register P1 is not NULL. |
| `IsType` | Jump to P2 based on the type of a value (checks type flags in P5). |
| `IfNullRow` | Jump to P2 if cursor P1 is pointing to a NULL row. |
| `IfPos` | Jump to P2 if register P1 is positive; also decrement P1 by P3. |
| `IfNotZero` | Jump to P2 if register P1 is non-zero; also decrement P1. |
| `DecrJumpZero` | Decrement register P1 and jump to P2 if the result is zero. |
| `IfNotOpen` | Jump to P2 if cursor P1 is not open. |
| `IfEmpty` | Jump to P2 if the table or index opened by cursor P1 is empty. |
| `IfSizeBetween` | Jump to P2 if the number of pages in a b-tree is between P3 and P4. |

### 4.2 Subroutine and Program Control

| Opcode | Behavior |
|--------|----------|
| `Program` | Begin executing a trigger sub-program. |
| `Param` | Read a parameter from the parent trigger frame. |
| `BeginSubrtn` | Mark the start of a subroutine (alias for Null with cleanup semantics). |
| `Trace` | Emit a trace callback with the SQL text. |
| `Abortable` | Assert that the statement can be aborted at this point. |
| `CursorHint` | Provide a hint expression to a cursor (optimization for virtual tables). |

### 4.3 Constants and Register Operations

| Opcode | Behavior |
|--------|----------|
| `Integer` | Store integer value P1 in register P2. |
| `Int64` | Store 64-bit integer from P4 in register P2. |
| `Real` | Store floating-point value from P4 in register P2. |
| `String8` | Store a UTF-8 string from P4 in register P2. |
| `String` | Store a string of length P1 from P4 in register P2. |
| `Null` | Set registers P2 through P2+P3 to NULL. |
| `SoftNull` | Set register P1 to NULL without freeing memory (for reuse). |
| `Blob` | Store a blob of P1 bytes from P4 in register P2. |
| `Variable` | Copy the value of bound parameter P1 into register P2. |
| `Move` | Move values from registers P1..P1+P3-1 to P2..P2+P3-1. |
| `Copy` | Copy register P1 into register P2 (deep copy). |
| `SCopy` | Shallow copy of register P1 into register P2. |
| `IntCopy` | Copy only the integer value from register P1 to register P2. |
| `ZeroOrNull` | Set register P2 to zero or NULL depending on registers P1 and P3. |

### 4.4 Arithmetic and Math

| Opcode | Behavior |
|--------|----------|
| `Add` | P3 = P2 + P1 (integer or float). |
| `Subtract` | P3 = P2 - P1. |
| `Multiply` | P3 = P2 * P1. |
| `Divide` | P3 = P2 / P1 (NULL if P1 is zero). |
| `Remainder` | P3 = P2 % P1 (NULL if P1 is zero). |
| `AddImm` | Add integer P2 to register P1 in place. |

### 4.5 Bitwise Operations

| Opcode | Behavior |
|--------|----------|
| `BitAnd` | P3 = P2 & P1. |
| `BitOr` | P3 = P2 \| P1. |
| `BitNot` | P2 = ~P1. |
| `ShiftLeft` | P3 = P2 << P1. |
| `ShiftRight` | P3 = P2 >> P1. |

### 4.6 String Operations

| Opcode | Behavior |
|--------|----------|
| `Concat` | P3 = P2 concatenated with P1. |

### 4.7 Comparison and Logic

| Opcode | Behavior |
|--------|----------|
| `Eq` | Jump to P2 if P1 == P3. |
| `Ne` | Jump to P2 if P1 != P3. |
| `Lt` | Jump to P2 if P3 < P1. |
| `Le` | Jump to P2 if P3 <= P1. |
| `Gt` | Jump to P2 if P3 > P1. |
| `Ge` | Jump to P2 if P3 >= P1. |
| `ElseEq` | Used after a prior Lt or Gt to implement three-way branching. |
| `Compare` | Compare two vectors of registers and set the comparison result. |
| `Permutation` | Set a permutation used by the next Compare instruction. |
| `And` | P3 = P1 AND P2 (three-valued logic: NULL propagation). |
| `Or` | P3 = P1 OR P2 (three-valued logic). |
| `Not` | P2 = NOT P1. |
| `IsTrue` | P2 = (P1 is true), with configurable NULL handling. |
| `CollSeq` | Set the collation sequence for the next comparison. |
| `MustBeInt` | Force register P1 to an integer or jump to P2 on failure. |
| `RealAffinity` | Convert register P1 from integer to real if it is an integer with an exact real representation. |
| `Cast` | Apply a type cast to register P1 according to affinity P2. |

### 4.8 Cursor Open / Close

| Opcode | Behavior |
|--------|----------|
| `OpenRead` | Open a read-only cursor P1 on the b-tree with root page P2 in database P3. |
| `OpenWrite` | Open a read/write cursor P1 on the b-tree with root page P2 in database P3. |
| `OpenDup` | Open a duplicate of cursor P2 as cursor P1. |
| `OpenAutoindex` | Open an ephemeral cursor P1 for an automatic index. |
| `OpenEphemeral` | Open an ephemeral (temporary) table as cursor P1. |
| `OpenPseudo` | Open a pseudo-cursor P1 that reads from a single register. |
| `ReopenIdx` | Reopen cursor P1 if its root page has changed; otherwise no-op. |
| `SorterOpen` | Open a sorter cursor P1. |
| `Close` | Close cursor P1 and release its resources. |
| `ColumnsUsed` | Declare which columns of cursor P1 are used (optimization hint). |

### 4.9 Cursor Seek / Position

| Opcode | Behavior |
|--------|----------|
| `SeekLT` | Position cursor P1 to the largest entry less than key P3. Jump to P2 if no such entry. |
| `SeekLE` | Position cursor P1 to the largest entry less than or equal to key P3. |
| `SeekGE` | Position cursor P1 to the smallest entry greater than or equal to key P3. |
| `SeekGT` | Position cursor P1 to the smallest entry greater than key P3. |
| `SeekRowid` | Position cursor P1 to the row with rowid in register P3. Jump to P2 if not found. |
| `NotExists` | Jump to P2 if rowid P3 does not exist in cursor P1. |
| `SeekScan` | Optimization: scan forward from the current cursor position instead of seeking. |
| `SeekHit` | Indicate to cursor P1 that it has encountered a matching row (skip ahead optimization). |
| `SeekEnd` | Position cursor P1 at the end of the b-tree (for fast append). |
| `Found` | Jump to P2 if a record matching key P3 exists in cursor P1. |
| `NotFound` | Jump to P2 if a record matching key P3 does NOT exist in cursor P1. |
| `NoConflict` | Like NotFound but specifically for constraint checking. |
| `IfNoHope` | Optimization: skip seek if bloom filter says key is not present. |
| `Last` | Move cursor P1 to the last entry. Jump to P2 if the table is empty. |
| `Rewind` | Move cursor P1 to the first entry. Jump to P2 if the table is empty. |
| `DeferredSeek` | Defer the seek of cursor P1 using rowid from cursor P3 (lazy evaluation). |
| `FinishSeek` | Complete a deferred seek on cursor P1. |

### 4.10 Cursor Traversal

| Opcode | Behavior |
|--------|----------|
| `Next` | Advance cursor P1 to the next entry. Jump to P2 if there is one. |
| `Prev` | Move cursor P1 to the previous entry. Jump to P2 if there is one. |
| `SorterNext` | Advance a sorter cursor to the next entry. Jump to P2 if there is one. |
| `SorterSort` | Sort the sorter and position to the first element. Jump to P2 if empty. |
| `Sort` | Same as Rewind but marks the loop as a sort (for EXPLAIN). |

### 4.11 Record Reading

| Opcode | Behavior |
|--------|----------|
| `Column` | Extract column P2 from the record in cursor P1 into register P3. |
| `Rowid` | Store the rowid of cursor P1 into register P2. |
| `RowData` | Copy the complete record data of cursor P1 into register P2. |
| `SorterData` | Copy the current record from sorter cursor P1 into register P2. |
| `Offset` | Store the byte offset of cursor P1's current entry in register P3. |
| `NullRow` | Set cursor P1 to point to a NULL row (all columns return NULL). |
| `Sequence` | Store an incrementing sequence number for cursor P1 in register P2. |
| `SequenceTest` | Test if a sequence value is the first use; jump if not. |

### 4.12 Record Writing

| Opcode | Behavior |
|--------|----------|
| `MakeRecord` | Create a record from P2 registers starting at P1, store in register P3. |
| `NewRowid` | Generate a new unique rowid for cursor P1, store in register P2. |
| `Insert` | Insert the record in register P2 as rowid P3 into cursor P1. |
| `Delete` | Delete the entry at cursor P1's current position. |
| `RowCell` | Copy a cell directly from one cursor to another without decoding. |
| `IdxInsert` | Insert record P2 into index cursor P1. |
| `SorterInsert` | Insert record P2 into sorter cursor P1. |
| `IdxDelete` | Delete from index cursor P1 the entry matching key P2. |

### 4.13 Index Comparison

| Opcode | Behavior |
|--------|----------|
| `IdxRowid` | Extract the rowid from index cursor P1 into register P2. |
| `IdxGT` | Jump to P2 if the index key at cursor P1 is greater than key P3. |
| `IdxGE` | Jump to P2 if the index key at cursor P1 is greater than or equal to key P3. |
| `IdxLT` | Jump to P2 if the index key at cursor P1 is less than key P3. |
| `IdxLE` | Jump to P2 if the index key at cursor P1 is less than or equal to key P3. |
| `SorterCompare` | Compare sorter cursor P1's current key with P3; jump to P2 if different. |

### 4.14 Type Checking and Affinity

| Opcode | Behavior |
|--------|----------|
| `Affinity` | Apply affinities (from string P4) to P2 registers starting at P1. |
| `TypeCheck` | Verify that registers match the declared column types (for STRICT tables). |

### 4.15 Transaction and Savepoint

| Opcode | Behavior |
|--------|----------|
| `Transaction` | Begin a transaction on database P1. P2=0 for read, P2=1 for write. |
| `Savepoint` | Create (P1=0), release (P1=1), or rollback (P1=2) a savepoint named P4. |
| `AutoCommit` | Set the auto-commit flag to P1. If P2 is non-zero, rollback. |
| `ReadCookie` | Read cookie P3 from database P1 into register P2. |
| `SetCookie` | Set cookie P2 of database P1 to value P3. |

### 4.16 Schema and DDL Operations

| Opcode | Behavior |
|--------|----------|
| `CreateBtree` | Create a new b-tree in database P1 (P3: 1=table, 2=index) and store root page in P2. |
| `SqlExec` | Execute the SQL string in P4. Used internally for DDL operations. |
| `ParseSchema` | Reload schema entries for database P1, filtered by P4. |
| `LoadAnalysis` | Load the sqlite_stat tables for database P1. |
| `DropTable` | Remove the internal schema entry for table P4 in database P1. |
| `DropIndex` | Remove the internal schema entry for index P4 in database P1. |
| `DropTrigger` | Remove the internal schema entry for trigger P4 in database P1. |
| `Destroy` | Free all pages of the b-tree rooted at P1; store the former root page in P2. |
| `Clear` | Delete all rows from the b-tree rooted at P1 (but keep the tree structure). |
| `ResetSorter` | Reset a sorter or ephemeral table cursor P1 to empty. |
| `IntegrityCk` | Run an integrity check on the database. |

### 4.17 Result and Output

| Opcode | Behavior |
|--------|----------|
| `ResultRow` | Output registers P1 through P1+P2-1 as a result row to the caller. |
| `Count` | Store the number of entries in cursor P1 into register P2. |

### 4.18 Function Calls

| Opcode | Behavior |
|--------|----------|
| `Function` | Call function P4 with P5 arguments from register P2; store result in P3. May have side effects. |
| `PureFunc` | Like Function but the function is deterministic (allows optimization). |

### 4.19 Aggregate Functions

| Opcode | Behavior |
|--------|----------|
| `AggStep` | Call the step function of aggregate P4 with arguments. Accumulator in P3. |
| `AggStep1` | Same as AggStep, but with a single-argument fast-path. |
| `AggInverse` | Call the inverse function of a window aggregate (for sliding windows). |
| `AggValue` | Call the value function of a window aggregate to get the current result. |
| `AggFinal` | Call the finalize function of aggregate P4. Store result in P3. |

### 4.20 Subtype Operations

| Opcode | Behavior |
|--------|----------|
| `ClrSubtype` | Clear the subtype from register P1. |
| `GetSubtype` | Copy the subtype of register P1 to register P2. |
| `SetSubtype` | Set the subtype of register P2 to the value from register P1. |

### 4.21 Rowset Operations

| Opcode | Behavior |
|--------|----------|
| `RowSetAdd` | Insert integer P2 into the rowset in register P1. |
| `RowSetRead` | Extract the smallest value from rowset P1 into P3. Jump to P2 if empty. |
| `RowSetTest` | Test if integer P3 is in rowset P1. Jump to P2 if found. |

### 4.22 Bloom Filter

| Opcode | Behavior |
|--------|----------|
| `FilterAdd` | Insert a hash into the bloom filter in register P1. |
| `Filter` | Test bloom filter P1 for a key; jump to P2 if definitely not present. |

### 4.23 Foreign Key Support

| Opcode | Behavior |
|--------|----------|
| `FkCheck` | Raise an error if there are outstanding unresolved foreign key violations. |
| `FkCounter` | Increment (P2>0) or decrement (P2<0) the FK violation counter in P1. |
| `FkIfZero` | Jump to P2 if the foreign key violation counter is zero. |

### 4.24 Journal and WAL

| Opcode | Behavior |
|--------|----------|
| `Checkpoint` | Run a WAL checkpoint on database P1 with mode P2. |
| `JournalMode` | Set or query the journal mode for database P1. |
| `Vacuum` | Run VACUUM on database P1. |
| `IncrVacuum` | Perform one step of incremental vacuum on database P1. Jump to P2 if finished. |

### 4.25 Miscellaneous

| Opcode | Behavior |
|--------|----------|
| `Expire` | Invalidate all prepared statements (P1=0) or only this statement (P1=1). |
| `CursorLock` | Prevent cursor P1 from being repositioned. |
| `CursorUnlock` | Allow cursor P1 to be repositioned again. |
| `TableLock` | Acquire a lock on table P2 in database P1. |
| `Pagecount` | Store the total page count of database P1 in register P2. |
| `MaxPgcnt` | Set or query the maximum page count for database P1. |
| `OffsetLimit` | Calculate the combined LIMIT+OFFSET value. |
| `MemMax` | Set register P1 to the maximum of its current value and register P2. |
| `ResetCount` | Copy the change counter to the connection's changes count and reset. |
| `ReleaseReg` | Release memory from registers (optimization hint). |

### 4.26 Virtual Table Operations

| Opcode | Behavior |
|--------|----------|
| `VBegin` | Begin a virtual table transaction. |
| `VCreate` | Create a virtual table by invoking its xCreate method. |
| `VDestroy` | Destroy a virtual table by invoking its xDestroy method. |
| `VOpen` | Open a virtual table cursor. |
| `VCheck` | Validate a virtual table's integrity. |
| `VInitIn` | Initialize an `IN (...)` constraint for a virtual table filter. |
| `VFilter` | Begin a filtered scan of virtual table cursor P1. Jump to P2 when done. |
| `VColumn` | Read column P2 from virtual table cursor P1 into register P3. |
| `VNext` | Advance virtual table cursor P1 to next row. Jump to P2 if there is one. |
| `VRename` | Rename a virtual table. |
| `VUpdate` | Invoke the xUpdate method of a virtual table for INSERT/UPDATE/DELETE. |

---

## 5. Built-in Functions

### 5.1 Scalar Functions

**Core scalar functions:**

| Function | Description |
|----------|-------------|
| `abs(X)` | Absolute value of X. Returns NULL if X is NULL. Returns integer if input is integer. |
| `char(X1, X2, ...)` | Return the string composed of characters with the given Unicode code points. |
| `coalesce(X, Y, ...)` | Return the first non-NULL argument. Short-circuit evaluation. |
| `concat(X, ...)` | Concatenate all arguments, treating NULLs as empty strings. |
| `concat_ws(SEP, X, ...)` | Concatenate with separator, skipping NULLs. |
| `format(FORMAT, ...)` | Printf-style string formatting (alias: `printf`). |
| `glob(PATTERN, STRING)` | Test if STRING matches PATTERN using Unix-style glob rules. |
| `hex(X)` | Return a hexadecimal rendering of the content of X. |
| `iif(X, Y, Z)` | If X is true, return Y; otherwise return Z. (Also `if`.) |
| `ifnull(X, Y)` | Return X if not NULL, otherwise Y. |
| `instr(X, Y)` | Return the 1-based position of the first occurrence of Y in X, or 0. |
| `last_insert_rowid()` | Return the rowid of the most recent successful INSERT on this connection. |
| `length(X)` | Length of string X in characters, or length of blob X in bytes. |
| `like(PATTERN, STRING)` / `like(PATTERN, STRING, ESCAPE)` | SQL LIKE pattern matching. |
| `likelihood(X, P)` | Hint to the query planner that X is true with probability P. |
| `likely(X)` | Hint that X is likely to be true. |
| `lower(X)` | Convert ASCII characters of X to lowercase. |
| `ltrim(X)` / `ltrim(X, Y)` | Remove characters in Y (default: spaces) from the left of X. |
| `max(X, Y, ...)` | Return the argument with the maximum value (scalar form). |
| `min(X, Y, ...)` | Return the argument with the minimum value (scalar form). |
| `nullif(X, Y)` | Return NULL if X equals Y; otherwise return X. |
| `octet_length(X)` | Number of bytes in the text or blob representation of X. |
| `printf(FORMAT, ...)` | Printf-style formatting. |
| `quote(X)` | Return the SQL literal representation of X. |
| `random()` | Return a random 64-bit signed integer. |
| `randomblob(N)` | Return an N-byte blob of pseudo-random data. |
| `replace(X, Y, Z)` | Return X with every occurrence of Y replaced by Z. |
| `round(X)` / `round(X, Y)` | Round X to Y decimal places (default 0). |
| `rtrim(X)` / `rtrim(X, Y)` | Remove characters in Y from the right of X. |
| `sign(X)` | Return -1, 0, or +1 depending on the sign of X. |
| `soundex(X)` | Return the Soundex encoding of X. |
| `substr(X, Y, Z)` / `substring(X, Y, Z)` | Extract substring from X starting at position Y with length Z. |
| `trim(X)` / `trim(X, Y)` | Remove characters in Y from both sides of X. |
| `typeof(X)` | Return the type name of X: "null", "integer", "real", "text", or "blob". |
| `unhex(X)` / `unhex(X, Y)` | Decode hex string X to blob; Y specifies characters to ignore. |
| `unicode(X)` | Return the Unicode code point of the first character of X. |
| `unistr(X)` | Interpret `\uXXXX` escape sequences in string X. |
| `unlikely(X)` | Hint that X is unlikely to be true. |
| `upper(X)` | Convert ASCII characters of X to uppercase. |
| `zeroblob(N)` | Return a blob of N zero bytes. |

**Math functions (compiled-in since 3.35.0):**

| Function | Description |
|----------|-------------|
| `acos(X)` | Arc cosine of X in radians. |
| `acosh(X)` | Hyperbolic arc cosine of X. |
| `asin(X)` | Arc sine of X. |
| `asinh(X)` | Hyperbolic arc sine of X. |
| `atan(X)` | Arc tangent of X. |
| `atan2(Y, X)` | Arc tangent of Y/X, using signs to determine quadrant. |
| `atanh(X)` | Hyperbolic arc tangent of X. |
| `ceil(X)` / `ceiling(X)` | Smallest integer not less than X. |
| `cos(X)` | Cosine of X (radians). |
| `cosh(X)` | Hyperbolic cosine of X. |
| `degrees(X)` | Convert radians to degrees. |
| `exp(X)` | e raised to the power X. |
| `floor(X)` | Largest integer not greater than X. |
| `ln(X)` | Natural logarithm of X. |
| `log(X)` | Base-10 logarithm of X. |
| `log(B, X)` | Base-B logarithm of X. |
| `log2(X)` | Base-2 logarithm of X. |
| `log10(X)` | Base-10 logarithm of X. |
| `mod(X, Y)` | Remainder of X/Y (floating-point modulo). |
| `pi()` | The value of pi. |
| `pow(X, Y)` / `power(X, Y)` | X raised to the power Y. |
| `radians(X)` | Convert degrees to radians. |
| `sign(X)` | Return -1, 0, or +1. |
| `sin(X)` | Sine of X (radians). |
| `sinh(X)` | Hyperbolic sine of X. |
| `sqrt(X)` | Square root of X. |
| `tan(X)` | Tangent of X (radians). |
| `tanh(X)` | Hyperbolic tangent of X. |
| `trunc(X)` | Truncate X toward zero. |

**Informational functions:**

| Function | Description |
|----------|-------------|
| `sqlite_version()` | Return the SQLite version string (e.g., "3.46.0"). |
| `sqlite_source_id()` | Return the check-in identifier of the source code. |
| `sqlite_compileoption_used(X)` | Return 1 if compile option X was used, else 0. |
| `sqlite_compileoption_get(N)` | Return the Nth compile-time option string. |
| `changes()` | Number of rows changed by the most recent INSERT/UPDATE/DELETE. |
| `total_changes()` | Total rows changed since the connection was opened. |
| `last_insert_rowid()` | Rowid of the most recent successful INSERT. |

**Other:**

| Function | Description |
|----------|-------------|
| `load_extension(X)` / `load_extension(X, Y)` | Load a shared library extension. |
| `parseuri(URI)` | Parse a URI string into components. |
| `unistr_quote(X)` | Like quote(), but with `\uXXXX` escaping for non-ASCII. |
| `fpdecode(X, Y, Z)` | Internal: floating-point decimal formatting. |

### 5.2 Aggregate Functions

| Function | Description |
|----------|-------------|
| `avg(X)` | Return the average of all non-NULL values of X. Returns a float. |
| `count(*)` | Return the number of rows in the group. |
| `count(X)` | Return the number of non-NULL values of X in the group. |
| `group_concat(X)` / `group_concat(X, SEP)` | Concatenate all non-NULL values of X, separated by SEP (default: ",".) |
| `string_agg(X, SEP)` | Alias for group_concat with explicit separator (SQL standard name). |
| `max(X)` | Return the maximum non-NULL value of X in the group. |
| `min(X)` | Return the minimum non-NULL value of X in the group. |
| `sum(X)` | Return the sum of all non-NULL values of X. Returns integer if all inputs are integer. Returns NULL for empty set. |
| `total(X)` | Like sum(), but returns 0.0 (float) for empty set instead of NULL. |
| `median(X)` | Return the median of all non-NULL values of X. |
| `percentile(X, P)` | Return the Pth percentile of all non-NULL values of X. |
| `percentile_cont(X, P)` | Continuous percentile (interpolated). |
| `percentile_disc(X, P)` | Discrete percentile (nearest actual value). |

All aggregate functions above can also be used as window functions.

### 5.3 Window Functions

These functions are only valid when used with an `OVER` clause.

| Function | Description |
|----------|-------------|
| `row_number()` | Sequential integer for each row in the partition, starting at 1. |
| `rank()` | Rank of the current row with gaps (rows with equal ORDER BY values get the same rank). |
| `dense_rank()` | Rank of the current row without gaps. |
| `percent_rank()` | (rank - 1) / (partition-rows - 1). Returns 0.0 for a single-row partition. |
| `cume_dist()` | Cumulative distribution: (number of rows <= current row) / (total rows). |
| `ntile(N)` | Divide the partition into N groups and return the group number (1-based) for the current row. |
| `lag(X)` / `lag(X, N)` / `lag(X, N, DEFAULT)` | Return the value of X from N rows before the current row (default N=1). Returns DEFAULT if no such row. |
| `lead(X)` / `lead(X, N)` / `lead(X, N, DEFAULT)` | Return the value of X from N rows after the current row (default N=1). Returns DEFAULT if no such row. |
| `first_value(X)` | Return the value of X for the first row in the window frame. |
| `last_value(X)` | Return the value of X for the last row in the window frame. |
| `nth_value(X, N)` | Return the value of X for the Nth row in the window frame (1-based). |

**Window frame types:**
- `ROWS`: Frame boundaries are defined by row count offsets.
- `RANGE`: Frame boundaries are defined by value ranges relative to the ORDER BY expression.
- `GROUPS`: Frame boundaries are defined by peer group count offsets.

**Frame boundary specifications:**
- `UNBOUNDED PRECEDING` / `UNBOUNDED FOLLOWING`
- `N PRECEDING` / `N FOLLOWING`
- `CURRENT ROW`

**Frame exclusion:**
- `EXCLUDE NO OTHERS` (default)
- `EXCLUDE CURRENT ROW`
- `EXCLUDE GROUP`
- `EXCLUDE TIES`

---

## 6. PRAGMA Commands

PRAGMAs are SQLite-specific commands for querying and modifying engine settings.
They do not follow standard SQL syntax.

### 6.1 Cache and Performance

| PRAGMA | Description |
|--------|-------------|
| `cache_size` / `cache_size = N` | Get or set the number of pages in the page cache. Negative values specify cache size in KiB. Default: -2000 (about 2 MB). |
| `default_cache_size = N` | Set the persistent default cache size stored in the database header. |
| `cache_spill` / `cache_spill = N` | Control when dirty pages are written to disk during a transaction. |
| `mmap_size = N` | Set the maximum memory-mapped I/O size in bytes. |
| `temp_store` / `temp_store = N` | Control where temporary tables and indexes are stored: 0=default, 1=file, 2=memory. |
| `temp_store_directory` | Get or set the directory for temporary files (deprecated). |
| `threads = N` | Set the maximum number of auxiliary worker threads. |
| `analysis_limit = N` | Limit the number of rows examined during ANALYZE. |

### 6.2 Journal and Durability

| PRAGMA | Description |
|--------|-------------|
| `journal_mode` / `journal_mode = MODE` | Get or set the journal mode. Modes: `DELETE`, `TRUNCATE`, `PERSIST`, `MEMORY`, `WAL`, `OFF`. |
| `journal_size_limit = N` | Limit the size of the rollback journal or WAL file. |
| `synchronous` / `synchronous = N` | Get or set the sync level: 0=OFF, 1=NORMAL, 2=FULL, 3=EXTRA. Controls fsync behavior for crash safety. |
| `wal_autocheckpoint = N` | Set the WAL auto-checkpoint threshold (default: 1000 frames). |
| `wal_checkpoint(MODE)` | Run a checkpoint. MODE: PASSIVE, FULL, RESTART, TRUNCATE. |
| `locking_mode` / `locking_mode = MODE` | Get or set the locking mode: NORMAL or EXCLUSIVE. In EXCLUSIVE mode, the connection never releases file locks. |

### 6.3 Database Configuration

| PRAGMA | Description |
|--------|-------------|
| `page_size` / `page_size = N` | Get or set the page size. Can only be set on an empty database or immediately before VACUUM. Valid: 512 to 65536 (powers of 2). |
| `max_page_count = N` | Set the maximum number of pages in the database file. |
| `page_count` | Return the total number of pages in the database file. |
| `auto_vacuum` / `auto_vacuum = N` | Get or set auto-vacuum mode: 0=NONE, 1=FULL, 2=INCREMENTAL. |
| `incremental_vacuum(N)` | Free up to N pages from the freelist. |
| `secure_delete` / `secure_delete = BOOLEAN` | When enabled, overwrite deleted content with zeros. |
| `encoding` / `encoding = "UTF-8"` | Get or set the text encoding. Can only be set on an empty database. |
| `user_version` / `user_version = N` | Get or set the user version integer in the database header. |
| `application_id` / `application_id = N` | Get or set the application ID in the database header. |
| `data_version` | Return a value that changes whenever the database is modified by any connection. |

### 6.4 Schema Inspection

| PRAGMA | Description |
|--------|-------------|
| `table_info(TABLE)` | Return one row per column: cid, name, type, notnull, dflt_value, pk. |
| `table_list` | Return all tables and views across all attached databases. |
| `table_xinfo(TABLE)` | Like table_info, but also shows hidden columns and generated columns. |
| `index_info(INDEX)` | Return columns of the given index. |
| `index_list(TABLE)` | Return all indexes on the given table. |
| `index_xinfo(INDEX)` | Like index_info, but includes key columns and auxiliary storage columns. |
| `database_list` | Return all attached databases (seq, name, file). |
| `collation_list` | Return all available collation sequences. |
| `function_list` | Return all registered functions. |
| `module_list` | Return all registered virtual table modules. |
| `pragma_list` | Return all recognized PRAGMA names. |

### 6.5 Foreign Keys

| PRAGMA | Description |
|--------|-------------|
| `foreign_keys` / `foreign_keys = BOOLEAN` | Enable or disable foreign key enforcement (default: off). |
| `foreign_key_list(TABLE)` | List all foreign key constraints on TABLE. |
| `foreign_key_check` / `foreign_key_check(TABLE)` | Check for foreign key violations. |

### 6.6 Integrity and Optimization

| PRAGMA | Description |
|--------|-------------|
| `integrity_check` / `integrity_check(N)` | Verify database integrity. Returns "ok" or a list of errors. N limits the number of errors reported. |
| `quick_check` / `quick_check(N)` | Like integrity_check, but skips verifying that the content of each row matches the index entries. Faster. |
| `optimize` | Run ANALYZE on tables that would benefit from it (heuristic). |
| `shrink_memory` | Free as much memory as possible from the connection. |

### 6.7 Compile-Time and Misc

| PRAGMA | Description |
|--------|-------------|
| `compile_options` | Return all compile-time options used to build the library. |
| `busy_timeout = N` | Set the busy timeout in milliseconds. |
| `soft_heap_limit = N` | Set the soft heap limit. |
| `hard_heap_limit = N` | Set the hard heap limit. |
| `case_sensitive_like = BOOLEAN` | Make the LIKE operator case-sensitive. |
| `lock_status` | Show the lock state of each database (used mainly for debugging). |

---

## 7. Locking Protocol

### 7.1 Rollback Journal Mode Locking

SQLite uses a file-level locking protocol with five lock states:

| Lock State | Description |
|------------|-------------|
| **NONE** (Unlocked) | No lock held. The connection is not reading or writing. |
| **SHARED** | One or more connections hold a shared lock. They can all read simultaneously. No writing is permitted. |
| **RESERVED** | One connection intends to write. There can be only one RESERVED lock at a time. Other SHARED locks are still permitted. The writer has not yet modified the database file. |
| **PENDING** | The writer is waiting to acquire an EXCLUSIVE lock. No new SHARED locks can be acquired, but existing SHARED locks can continue. |
| **EXCLUSIVE** | One connection holds this lock. No other locks of any kind are permitted. The connection is writing to the database file. |

**Lock acquisition order:** NONE -> SHARED -> RESERVED -> PENDING -> EXCLUSIVE.
A connection cannot skip levels.

**Reader behavior:** A reader acquires SHARED, reads data, then releases back
to NONE.

**Writer behavior:** A writer acquires SHARED, then RESERVED (marking intent to
write), accumulates changes in the journal, then escalates to PENDING then
EXCLUSIVE to write changes to the database file, then releases back to NONE.

**Deadlock prevention:** Because lock transitions are strictly ordered and only
one RESERVED lock is allowed, deadlocks cannot occur.

### 7.2 WAL Mode Locking

WAL mode uses a fundamentally different concurrency model:

- **Multiple concurrent readers** are always allowed, even while a writer is active.
- **A single writer** can proceed without blocking readers.
- Readers see a consistent snapshot of the database as of the start of their
  read transaction. They are not affected by concurrent writes.
- The writer appends new pages to the WAL file rather than modifying the
  database file directly.
- Readers and the writer coordinate through the WAL index (shared memory).

**WAL locks:** The WAL index contains a set of lock bytes:
- `WAL_WRITE_LOCK` (byte 0): Exclusive lock held by the writer.
- `WAL_CKPT_LOCK` (byte 1): Exclusive lock held during checkpoint.
- `WAL_RECOVER_LOCK` (byte 2): Held during WAL recovery.
- Reader locks (bytes 3-7): Each concurrent reader holds a shared lock on one
  of these bytes, recording which WAL frame is the end-mark for their snapshot.

**Limitations in WAL mode:**
- WAL mode only works for databases on the local filesystem (not network filesystems).
- There can be at most about 5 simultaneous readers (limited by the number of
  reader lock slots in the WAL index).
- The WAL file can grow without bound if there are long-running read transactions
  preventing checkpoints from completing.

### 7.3 Busy Handling

When a lock cannot be acquired, SQLite invokes the busy handler. The default
behavior is to return `SQLITE_BUSY` immediately. Applications can:
- Set a busy timeout with `sqlite3_busy_timeout()` or `PRAGMA busy_timeout`,
  causing SQLite to retry with sleeps up to the specified duration.
- Register a custom busy handler callback with `sqlite3_busy_handler()`.

---

## 8. Extension APIs

### 8.1 FTS (Full-Text Search)

**FTS3 and FTS4:** Virtual table modules for full-text search. They create
inverted indexes mapping terms to the documents that contain them.
- Supports tokenizers: `simple`, `porter`, `unicode61`, `icu`.
- Query syntax: `MATCH 'search terms'`, with support for phrase queries
  (`"exact phrase"`), prefix queries (`term*`), AND/OR/NOT operators,
  column filters, and NEAR queries.
- FTS4 adds: `matchinfo()`, `offsets()`, `snippet()` auxiliary functions,
  content tables, compression support, and `languageid` option.

**FTS5:** A complete rewrite of full-text search with:
- More efficient storage format.
- Custom tokenizer API.
- BM25 ranking function built in.
- `highlight()` and `snippet()` auxiliary functions.
- `fts5vocab` virtual table for inspecting the index vocabulary.
- Extensible auxiliary function API.
- Support for prefix indexes, detail modes (`full`, `column`, `none`).

### 8.2 R-Tree

A virtual table module implementing an R-tree spatial index. Used for efficient
range queries on multi-dimensional data (2-5 dimensions).
- Each entry has an integer ID and bounding coordinates (minX, maxX, minY, maxY, ...).
- Efficient queries for: containment, overlap, nearest-neighbor.
- Custom query geometry callbacks supported.

**Geopoly:** An alternative interface to R-tree that stores GeoJSON-style
polygons and supports spatial operations like `geopoly_overlap()`,
`geopoly_within()`, `geopoly_area()`, `geopoly_contains_point()`, etc.

### 8.3 JSON

The JSON extension provides functions for creating and querying JSON data.
JSON is stored as text; the extension parses it on the fly (or uses a binary
format called JSONB internally for efficiency).

**Key functions:**
- `json(X)`: Validate and minify JSON.
- `json_array(...)`: Create a JSON array.
- `json_object(...)`: Create a JSON object.
- `json_extract(JSON, PATH)` / `->` / `->>`: Extract a value from JSON.
- `json_set(JSON, PATH, VALUE, ...)`: Set values in JSON.
- `json_insert(JSON, PATH, VALUE, ...)`: Insert (do not replace) values.
- `json_replace(JSON, PATH, VALUE, ...)`: Replace (do not insert) values.
- `json_remove(JSON, PATH, ...)`: Remove entries from JSON.
- `json_type(JSON)` / `json_type(JSON, PATH)`: Return the type of a JSON value.
- `json_valid(X)`: Return 1 if X is well-formed JSON.
- `json_quote(X)`: Quote a value as a JSON literal.
- `json_array_length(JSON)` / `json_array_length(JSON, PATH)`: Return array length.
- `json_each(JSON)` / `json_tree(JSON)`: Table-valued functions for iterating over JSON.
- `json_group_array(X)`: Aggregate function to collect values into a JSON array.
- `json_group_object(KEY, VALUE)`: Aggregate function to collect key-value pairs into a JSON object.
- `json_patch(JSON1, JSON2)`: Apply an RFC 7396 merge patch.
- `jsonb(X)`: Convert to internal binary JSON format.

### 8.4 Session / Changeset

The session extension records changes made to a database and can produce
changesets (binary format describing the changes).

- `sqlite3session_create()`: Begin recording changes.
- `sqlite3session_changeset()`: Generate a changeset blob.
- `sqlite3changeset_apply()`: Apply a changeset to another database.
- `sqlite3changeset_invert()`: Invert a changeset (for undo).
- `sqlite3changeset_concat()`: Combine two changesets.
- `sqlite3changegroup`: Group multiple changesets together.
- `sqlite3changeset_iter`: Iterate over the contents of a changeset.

Changesets contain the old and new values for every modified row, enabling
conflict detection and resolution during application.

### 8.5 ICU

Integration with the International Components for Unicode (ICU) library:
- `icu_load_collation(LOCALE, NAME)`: Create a collation sequence based on
  an ICU locale (e.g., `de_DE`).
- Case-insensitive LIKE using ICU case folding.
- ICU tokenizer for FTS.

### 8.6 Miscellaneous Extensions

| Extension | Description |
|-----------|-------------|
| `generate_series(START, STOP, STEP)` | Table-valued function generating a sequence of integers. |
| `dbstat` | Virtual table showing page-level storage statistics for all tables and indexes. |
| `csv` | Virtual table for reading CSV files. |
| `carray` | Table-valued function that binds a C array as a virtual table. |
| `closure` | Transitive closure virtual table. |
| `fileio` | Functions for reading/writing files: `readfile()`, `writefile()`, `fsdir()`. |
| `completion` | Provides SQL keyword and table/column name completions. |
| `stmt` | Virtual table listing all prepared statements. |
| `unionvtab` | Union multiple tables/databases through a single virtual table. |
| `decimal` | Arbitrary-precision decimal arithmetic: `decimal_add()`, `decimal_mul()`, etc. |
| `ieee754` | Functions for decomposing and constructing IEEE 754 floats. |
| `series` | Another name for generate_series in some builds. |
| `sha1` / `sha3` | Cryptographic hash functions. |
| `uuid` | UUID generation and conversion functions. |
| `regexp` | Provides `REGEXP` operator implementation. |

---

## 9. Limits

All configurable limits with their default values, as defined in `sqliteLimit.h`.

| Constant | Default Value | Description |
|----------|---------------|-------------|
| `SQLITE_MAX_LENGTH` | 1,000,000,000 (1 billion) | Maximum length of a TEXT or BLOB value in bytes. Also limits the size of a single row. Minimum settable value: 30. |
| `SQLITE_MAX_COLUMN` | 2,000 | Maximum number of columns in a table, index, view, result set, GROUP BY, ORDER BY, or INSERT VALUES clause. Hard upper limit: 32,767. |
| `SQLITE_MAX_SQL_LENGTH` | 1,000,000,000 (1 billion) | Maximum length of a single SQL statement in bytes. Cannot be disabled. Hard upper limit: 2,147,482,624. |
| `SQLITE_MAX_EXPR_DEPTH` | 1,000 | Maximum depth of an expression tree. A value of 0 disables the limit. |
| `SQLITE_MAX_PARSER_DEPTH` | 2,500 | Maximum depth of the LALR(1) parser stack. Prior to 3.45.0 this was hard-coded to 100. |
| `SQLITE_MAX_COMPOUND_SELECT` | 500 | Maximum number of terms in a compound SELECT (UNION, INTERSECT, EXCEPT). 0 disables the limit. |
| `SQLITE_MAX_VDBE_OP` | 250,000,000 | Maximum number of opcodes in a VDBE program. Not currently enforced at runtime. |
| `SQLITE_MAX_FUNCTION_ARG` | 1,000 | Maximum number of arguments to an SQL function. Hard upper limit: 32,767. |
| `SQLITE_DEFAULT_CACHE_SIZE` | -2,000 | Default suggested page cache size. Negative means limit by KiB (so -2000 = about 2 MB). |
| `SQLITE_DEFAULT_WAL_AUTOCHECKPOINT` | 1,000 | Default number of WAL frames before auto-checkpoint. |
| `SQLITE_MAX_ATTACHED` | 10 | Maximum number of attached databases. Hard upper limit: 125 (because the value must fit in a signed 8-bit integer minus 2 for "main" and "temp"). |
| `SQLITE_MAX_VARIABLE_NUMBER` | 32,766 | Maximum index for a `?NNN` parameter placeholder. |
| `SQLITE_MAX_PAGE_SIZE` | 65,536 | Maximum database page size. This is a hard limit and cannot be changed at compile time. |
| `SQLITE_DEFAULT_PAGE_SIZE` | 4,096 | Default page size for new databases. |
| `SQLITE_MAX_DEFAULT_PAGE_SIZE` | 8,192 | Maximum page size that SQLite will choose automatically based on device characteristics. |
| `SQLITE_MAX_PAGE_COUNT` | 4,294,967,294 (0xFFFFFFFE) | Maximum number of pages in a database file. This is the default for `PRAGMA max_page_count`. |
| `SQLITE_MAX_LIKE_PATTERN_LENGTH` | 50,000 | Maximum length of a LIKE or GLOB pattern in bytes. |
| `SQLITE_MAX_TRIGGER_DEPTH` | 1,000 | Maximum depth of trigger recursion. 0 means no triggers may execute. |
| `SQLITE_MAX_ALLOCATION_SIZE` | 2,147,483,391 | Maximum size of a single memory allocation (slightly under 2 GiB, with a 256-byte safety margin). |

### 9.1 Derived Limits

Some limits are not separately configurable but are derived from other values:

- **Maximum database file size:** `SQLITE_MAX_PAGE_COUNT * SQLITE_MAX_PAGE_SIZE` = approximately 281 terabytes (with default settings).
- **Maximum number of tables/indexes:** Limited by the number of pages and the schema representation, practically in the tens of thousands.
- **Maximum number of rows:** Limited by available rowids (2^63 - 1 for integer primary keys).
- **Maximum row size:** Governed by `SQLITE_MAX_LENGTH`.

---

## 10. Collation Sequences

SQLite provides three built-in collation sequences:

| Collation | Description |
|-----------|-------------|
| `BINARY` | Compare strings byte-by-byte using `memcmp()`. This is the default. |
| `NOCASE` | Like BINARY, but the 26 uppercase ASCII letters are folded to lowercase before comparison. Only handles ASCII; does not handle Unicode case folding. |
| `RTRIM` | Like BINARY, but trailing spaces are ignored. |

Custom collation sequences can be registered via `sqlite3_create_collation()`.

---

## 11. Transaction Semantics

### 11.1 ACID Properties

SQLite provides full ACID (Atomicity, Consistency, Isolation, Durability)
transaction semantics:

- **Atomicity:** A transaction either commits entirely or rolls back entirely.
  In the event of a crash, the rollback journal or WAL ensures that partial
  writes are undone.
- **Consistency:** Schema constraints (NOT NULL, UNIQUE, CHECK, FOREIGN KEY)
  are enforced at the appropriate times.
- **Isolation:** In rollback journal mode, writers block readers and vice versa
  (serialized access). In WAL mode, readers see a snapshot and are not blocked
  by writers.
- **Durability:** When `synchronous = FULL` (the default in DELETE journal mode),
  committed transactions survive power failures and crashes. `synchronous = NORMAL`
  in WAL mode provides the same guarantee for most scenarios.

### 11.2 Transaction Types

- **DEFERRED** (default): No locks are acquired until the first read or write
  operation. A read acquires SHARED; a write acquires RESERVED.
- **IMMEDIATE:** A RESERVED lock is acquired immediately when the transaction
  begins. This prevents other connections from writing but allows readers.
- **EXCLUSIVE:** An EXCLUSIVE lock is acquired immediately. No other connections
  can read or write.

### 11.3 Savepoints

Savepoints provide nested transaction control:
- `SAVEPOINT name`: Creates a savepoint marker.
- `RELEASE name`: Commits all changes since the savepoint (does not commit
  the outer transaction).
- `ROLLBACK TO name`: Reverts changes since the savepoint but keeps the
  savepoint active for further use.

Savepoints can be nested to arbitrary depth. Releasing or rolling back to a
savepoint also affects all more-recent savepoints.

### 11.4 Auto-Commit Mode

When no explicit transaction is active, each statement runs in its own implicit
transaction that is automatically committed upon completion. This is auto-commit
mode. `BEGIN` takes the connection out of auto-commit mode; `COMMIT` or
`ROLLBACK` returns it.

---

## 12. Virtual Table Interface

Virtual tables allow SQLite to query external data sources using the familiar
SQL interface. A virtual table module must implement the following methods:

| Method | Description |
|--------|-------------|
| `xCreate` | Called when `CREATE VIRTUAL TABLE` is executed. Allocate resources. |
| `xConnect` | Called when a connection opens and the virtual table already exists. |
| `xBestIndex` | The query planner calls this to ask the module about its query capabilities and estimated costs. |
| `xOpen` | Open a new cursor for scanning the virtual table. |
| `xFilter` | Begin a filtered scan. Receives the arguments chosen by xBestIndex. |
| `xNext` | Advance the cursor to the next row. |
| `xEof` | Return true if the cursor has passed the last row. |
| `xColumn` | Return the value of a specific column for the current row. |
| `xRowid` | Return the rowid of the current row. |
| `xClose` | Close the cursor. |
| `xDisconnect` | Called when a connection disconnects from the virtual table. |
| `xDestroy` | Called when `DROP TABLE` is executed. Free all resources. |
| `xUpdate` | Handle INSERT, UPDATE, and DELETE operations. |
| `xRename` | Handle `ALTER TABLE ... RENAME TO`. |
| `xFindFunction` | Overload SQL functions for the virtual table. |
| `xBegin` / `xSync` / `xCommit` / `xRollback` | Transaction lifecycle callbacks. |
| `xSavepoint` / `xRelease` / `xRollbackTo` | Savepoint support. |

---

## 13. Error Codes

SQLite uses a structured error code system:

### 13.1 Primary Result Codes

| Code | Value | Meaning |
|------|-------|---------|
| `SQLITE_OK` | 0 | Success. |
| `SQLITE_ERROR` | 1 | Generic error. |
| `SQLITE_INTERNAL` | 2 | Internal logic error. |
| `SQLITE_PERM` | 3 | Access permission denied. |
| `SQLITE_ABORT` | 4 | Callback routine requested an abort. |
| `SQLITE_BUSY` | 5 | Database file is locked. |
| `SQLITE_LOCKED` | 6 | A table in the database is locked. |
| `SQLITE_NOMEM` | 7 | Memory allocation failed. |
| `SQLITE_READONLY` | 8 | Attempt to write a read-only database. |
| `SQLITE_INTERRUPT` | 9 | Operation terminated by `sqlite3_interrupt()`. |
| `SQLITE_IOERR` | 10 | I/O error. |
| `SQLITE_CORRUPT` | 11 | Database disk image is malformed. |
| `SQLITE_NOTFOUND` | 12 | Unknown opcode in `sqlite3_file_control()`. |
| `SQLITE_FULL` | 13 | Database or disk is full. |
| `SQLITE_CANTOPEN` | 14 | Unable to open the database file. |
| `SQLITE_PROTOCOL` | 15 | Lock protocol error. |
| `SQLITE_EMPTY` | 16 | Not currently used. |
| `SQLITE_SCHEMA` | 17 | The schema changed. |
| `SQLITE_TOOBIG` | 18 | String or BLOB exceeds size limit. |
| `SQLITE_CONSTRAINT` | 19 | Constraint violation. |
| `SQLITE_MISMATCH` | 20 | Data type mismatch. |
| `SQLITE_MISUSE` | 21 | Library used incorrectly. |
| `SQLITE_NOLFS` | 22 | OS features not available. |
| `SQLITE_AUTH` | 23 | Authorization denied. |
| `SQLITE_FORMAT` | 24 | Not currently used. |
| `SQLITE_RANGE` | 25 | Parameter index out of range. |
| `SQLITE_NOTADB` | 26 | File is not a database. |
| `SQLITE_NOTICE` | 27 | Notification from `sqlite3_log()`. |
| `SQLITE_WARNING` | 28 | Warning from `sqlite3_log()`. |
| `SQLITE_ROW` | 100 | `sqlite3_step()` has another row ready. |
| `SQLITE_DONE` | 101 | `sqlite3_step()` has finished executing. |

### 13.2 Extended Result Codes

Extended codes add specificity via the formula `primary + (N * 256)`. Examples:
- `SQLITE_IOERR_READ` (266): Error reading from disk.
- `SQLITE_IOERR_WRITE` (778): Error writing to disk.
- `SQLITE_BUSY_RECOVERY` (261): Busy because another process is recovering the WAL.
- `SQLITE_BUSY_SNAPSHOT` (517): Busy because a WAL snapshot is in use.
- `SQLITE_CONSTRAINT_UNIQUE` (2067): UNIQUE constraint violated.
- `SQLITE_CONSTRAINT_PRIMARYKEY` (1555): PRIMARY KEY constraint violated.
- `SQLITE_CONSTRAINT_FOREIGNKEY` (787): FOREIGN KEY constraint violated.
- `SQLITE_CONSTRAINT_NOTNULL` (1299): NOT NULL constraint violated.
- `SQLITE_CONSTRAINT_CHECK` (275): CHECK constraint violated.
- `SQLITE_READONLY_RECOVERY` (264): Cannot write because WAL recovery is needed.
- `SQLITE_CORRUPT_VTAB` (267): Content in a virtual table is corrupt.

---

## 14. C API Surface Area (Key Entry Points)

The public C API defines the interface that any compatible implementation must
support. The most critical entry points are:

### 14.1 Connection Lifecycle

- `sqlite3_open()` / `sqlite3_open_v2()` / `sqlite3_open16()`: Open a database connection.
- `sqlite3_close()` / `sqlite3_close_v2()`: Close a connection.
- `sqlite3_exec()`: One-shot execute an SQL string with callback.

### 14.2 Prepared Statements

- `sqlite3_prepare_v2()` / `sqlite3_prepare_v3()`: Compile SQL into a prepared statement.
- `sqlite3_bind_*()`: Bind values to parameters (`?`, `?NNN`, `:name`, `@name`, `$name`).
- `sqlite3_step()`: Execute one step (returns `SQLITE_ROW` or `SQLITE_DONE`).
- `sqlite3_column_*()`: Read column values from the current result row.
- `sqlite3_reset()`: Reset a prepared statement for re-execution.
- `sqlite3_finalize()`: Destroy a prepared statement.

### 14.3 Extension Points

- `sqlite3_create_function_v2()`: Register custom SQL functions.
- `sqlite3_create_collation_v2()`: Register custom collation sequences.
- `sqlite3_create_module_v2()`: Register virtual table modules.
- `sqlite3_set_authorizer()`: Register a callback to authorize SQL operations.
- `sqlite3_trace_v2()`: Register trace/profile callbacks.
- `sqlite3_update_hook()`: Register a callback on row changes.
- `sqlite3_commit_hook()` / `sqlite3_rollback_hook()`: Transaction lifecycle callbacks.
- `sqlite3_wal_hook()`: WAL commit callback.
- `sqlite3_progress_handler()`: Long-running query interrupt callback.

### 14.4 Memory and Configuration

- `sqlite3_malloc()` / `sqlite3_realloc()` / `sqlite3_free()`: Memory allocation.
- `sqlite3_config()`: Global configuration (threading mode, memory allocator, etc.).
- `sqlite3_db_config()`: Per-connection configuration.
- `sqlite3_limit()`: Query or change per-connection limits at runtime.

### 14.5 Backup API

- `sqlite3_backup_init()`: Begin an online backup.
- `sqlite3_backup_step()`: Copy pages incrementally.
- `sqlite3_backup_finish()`: Complete the backup.
- `sqlite3_backup_remaining()` / `sqlite3_backup_pagecount()`: Progress reporting.

### 14.6 Blob I/O

- `sqlite3_blob_open()`: Open a handle to a BLOB for incremental read/write.
- `sqlite3_blob_read()` / `sqlite3_blob_write()`: Read or write portions of the BLOB.
- `sqlite3_blob_bytes()`: Return the size of the BLOB.
- `sqlite3_blob_reopen()`: Move the handle to a different row.
- `sqlite3_blob_close()`: Close the handle.

---

## 15. Threading Model

SQLite supports three threading modes, chosen at compile time or runtime:

| Mode | Description |
|------|-------------|
| **Single-thread** | All mutexes are disabled. Only safe if the application ensures no more than one thread uses SQLite at a time. |
| **Multi-thread** | SQLite can be used by multiple threads, but each database connection must be used by only one thread at a time. |
| **Serialized** (default) | SQLite can be safely used by multiple threads with no restrictions. Internal mutexes serialize access to each connection. |

The threading mode is set via `sqlite3_config(SQLITE_CONFIG_SINGLETHREAD)`,
`sqlite3_config(SQLITE_CONFIG_MULTITHREAD)`, or
`sqlite3_config(SQLITE_CONFIG_SERIALIZED)`.

Within WAL mode, multiple connections (from different threads or processes) can
read concurrently while one connection writes. In rollback journal mode,
writers exclude all readers and vice versa.
