# Trove

A JSON document database with indexed storage built on top of [dream](https://github.com/mentalblood0/dream) index.

## Overview

Trove provides a transactional storage for JSON documents with the following features:

- **Document Storage**: Store JSON documents with auto-generated UUID v7 identifiers
- **Path-Based Access**: Access nested values using intuitive paths like `["user", "profile", "name"]`
- **Indexed Queries**: Query objects based on path-value presence/absence conditions
- **ACID Transactions**: ACID-compliant transactions for both read and write operations
- **Flat Storage**: Efficient flat storage with nested reconstruction

## Architecture

Trove is built using:

| Component | Purpose |
|-----------|---------|
| [dream](https://github.com/mentalblood0/dream) | Inverted indexing for fast queries |
| [bincode](https://github.com/servo/bincode) | Efficient binary encoding |
| [serde_json](https://github.com/serde-rs/json) | JSON handling and serialization |
| [xxhash-rust](https://github.com/DoumanAsh/xxhash-rust) | Fast digest computation |

## Installation

Add trove to your `Cargo.toml`:

```toml
[dependencies]
trove = { git = "https://github.com/mentalblood0/trove" }
```

## Key Concepts

### ObjectId

Each document in trove by default has a unique identifier generated using UUID v7 for time-ordered uniqueness:

```rust
let id = tx.insert(json!({"data": "value"}))?;
println!("Object ID: {:?}", id.value);
```

### Paths

Access nested values using paths constructed with the `path_segments!` macro:

```rust
// Simple object key
let path1 = path_segments!("name");

// Nested object key  
let path2 = path_segments!("user", "profile");

// Array access
let path3 = path_segments!("items", 0, "id");

// Mixed
let path4 = path_segments!("users", 0, "address", "city");
```

### Transactions

Trove provides two types of transactions:

#### Write Transactions

For inserting, updating, and removing data:

```rust
chest.lock_all_and_write(|tx| {
    // Insert a new object
    let id = tx.insert(json!({"name": "Alice"}))?;
    
    // Update a nested value
    tx.update(id.clone(), path_segments!("age"), json!(30))?;
    
    // Push to array
    tx.push(&id, &path_segments!("tags"), json!("premium"))?;
    
    // Remove a value
    tx.remove(&id, &path_segments!("temp"))?;
    
    Ok(())
})?;
```

#### Read Transactions

For querying data without modification:

```rust
chest.lock_all_writes_and_read(|tx| {
    // Iterate all objects
    for obj in tx.objects()?.collect::<Vec<_>>()? {
        println!("Object: {:?}", obj.id);
        println!("Value: {:?}", obj.value);
    }
    
    // Get specific value
    let value = tx.get(&id, &path_segments!("name"))?;
    
    // Select by conditions
    let matches = tx.select(
        &vec![(IndexRecordType::Direct, path_segments!("status"), json!("active"))],
        &vec![(IndexRecordType::Direct, path_segments!("deleted"), json!(true))],
        None,
    )?.collect::<Vec<ObjectId>>()?;
    
    Ok(())
})?;
```

### Indexed Queries

Use the inverted index to efficiently query objects:

```rust
// Find all objects with status = "active"
let active_users = tx.select(
    &vec![(IndexRecordType::Direct, path_segments!("status"), json!("active"))],
    &vec![],
    None,
)?;

// Find all objects with tag = "premium" (array search)
let premium_users = tx.select(
    &vec![(IndexRecordType::Array, path_segments!("tags"), json!("premium"))],
    &vec![],
    None,
)?;

// Find objects with conditions (AND logic)
let users = tx.select(
    &vec![
        (IndexRecordType::Direct, path_segments!("status"), json!("active")),
        (IndexRecordType::Direct, path_segments!("role"), json!("admin")),
    ],
    &vec![(IndexRecordType::Direct, path_segments!("banned"), json!(true))],
    None,
)?;
```

### Array Operations

Trove supports efficient array operations:

```rust
// Get array length
let len = tx.len(&id, &path_segments!("items"))?;

// Get last element
let last = tx.last(&id, &path_segments!("items"))?;

// Check if array contains element
let has_value = tx.contains_element(&id, &path_segments!("items"), &Value::String("test".into()))?;

// Push to array (returns index)
let index = tx.push(&id, &path_segments!("items"), json!("new item"))?;
```

## Data Model

### Value Types

Trove stores primitive values:

| Type | Description |
|------|-------------|
| `Null` | JSON null |
| `Integer(i64)` | 64-bit signed integer |
| `Float(f64)` | 64-bit floating point |
| `Bool(bool)` | Boolean |
| `String(String)` | UTF-8 string |

Note: Arrays and objects are not stored directly - they are flattened into path-value pairs.

### Index Record Types

- **`Direct`**: For object properties and scalar values. Matches exact path-value pairs.
- **`Array`**: For values inside arrays.

## Testing

Run tests with:

```bash
cargo test
```

## License

This project is licensed under the MIT License.
