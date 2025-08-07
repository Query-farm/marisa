# DuckDB Marisa Extension by [Query.Farm](https://query.farm)

The **Marisa** extension, developed by **[Query.Farm](https://query.farm)**, adds [MARISA](https://github.com/s-yata/marisa-trie) (Matching Algorithm with Recursively Implemented StorAge) trie functionality for DuckDB. [MARISA](https://github.com/s-yata/marisa-trie) is a static and space-efficient trie data structure that enables fast string lookups, prefix searches, and predictive text operations.

## Use Cases

MARISA tries are particularly useful for:
- **Autocomplete/Type-ahead functionality**: Use `marisa_predictive()` to find all completions for a partial string
- **Spell checking**: Use `marisa_lookup()` to verify if words exist in a dictionary
- **URL routing**: Efficiently match URL patterns and extract parameters
- **IP address prefix matching**: Network routing and firewall rules
- **String deduplication**: Compact storage of large sets of strings with common prefixes

## Installation

**`marisa` is a [DuckDB Community Extension](https://github.com/duckdb/community-extensions).**

You can now use this by using this SQL:

```sql
install marisa from community;
load marisa;
```

The `marisa` extension provides several functions for working with MARISA tries:

### Creating a Trie
Use the `marisa_trie()` aggregate function to create a trie from string data:
```sql
CREATE TABLE employees(name TEXT);
INSERT INTO employees VALUES('Alice'), ('Bob'), ('Charlie'), ('David'), ('Eve'), ('Frank'), ('Mallory'), ('Megan'), ('Oscar'), ('Melissa');

-- Create a trie from the employee names
CREATE TABLE employees_trie AS SELECT marisa_trie(name) AS trie FROM employees;
```

### Lookup Function
Check if a string exists in the trie using `marisa_lookup()`:
```sql
-- Check if 'Alice' exists in the trie (returns true)
SELECT marisa_lookup(trie, 'Alice') FROM employees_trie;

-- Check if 'Unknown' exists in the trie (returns false)
SELECT marisa_lookup(trie, 'Unknown') FROM employees_trie;
```

### Common Prefix Search
Find all strings in the trie that are prefixes of a given string using `marisa_common_prefix()`:
```sql
CREATE TABLE countries(name TEXT);
INSERT INTO countries VALUES ('U'), ('US'), ('USA');
CREATE TABLE countries_trie AS SELECT marisa_trie(name) AS trie FROM countries;

-- Find all prefixes of 'USA' (returns ['U', 'US', 'USA'])
SELECT marisa_common_prefix(trie, 'USA', 10) FROM countries_trie;
```

### Predictive Search
Find all strings in the trie that start with a given prefix using `marisa_predictive()`:
```sql
-- Find all names starting with 'Me' (returns ['Megan', 'Melissa'])
SELECT marisa_predictive(trie, 'Me', 10) FROM employees_trie;
```

## Function Reference

### `marisa_trie(column)`
**Type:** Aggregate Function
**Description:** Creates a MARISA trie from string values in a column.
**Parameters:**
- `column` (VARCHAR): Column containing strings to build the trie from

**Example:**
```sql
SELECT marisa_trie(name) FROM employees;
```

### `marisa_lookup(trie, search_string)`
**Type:** Scalar Function
**Description:** Checks if a string exists in the trie.
**Parameters:**
- `trie` (BLOB): The trie created by `marisa_trie()`
- `search_string` (VARCHAR): String to search for

**Returns:** BOOLEAN (true if found, false otherwise)

**Example:**
```sql
SELECT marisa_lookup(trie, 'Alice') FROM employees_trie;
```

### `marisa_common_prefix(trie, search_string, max_results)`
**Type:** Scalar Function
**Description:** Finds all strings in the trie that are prefixes of the search string.
**Parameters:**
- `trie` (BLOB): The trie created by `marisa_trie()`
- `search_string` (VARCHAR): String to find prefixes for
- `max_results` (INTEGER): Maximum number of results to return

**Returns:** LIST(VARCHAR) - List of prefix matches

**Example:**
```sql
SELECT marisa_common_prefix(trie, 'USA', 10) FROM countries_trie;
-- Returns: ['U', 'US', 'USA']
```

### `marisa_predictive(trie, prefix, max_results)`
**Type:** Scalar Function
**Description:** Finds all strings in the trie that start with the given prefix.
**Parameters:**
- `trie` (BLOB): The trie created by `marisa_trie()`
- `prefix` (VARCHAR): Prefix to search for
- `max_results` (INTEGER): Maximum number of results to return

**Returns:** LIST(VARCHAR) - List of strings starting with the prefix

**Example:**
```sql
SELECT marisa_predictive(trie, 'Me', 10) FROM employees_trie;
-- Returns: ['Megan', 'Melissa']
```
