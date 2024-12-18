# Gongo: Lightweight Embedded Document Database with mongo style pipeline querying for Go

## Overview

MarcoDB is a simple, flexible embedded document database for Golang, inspired by MongoDB and PoloDB and built on top of Badger. Designed for lightweight applications, MarcoDB provides an intuitive document storage and querying experience with MongoDB-like aggregation capabilities.

## Key Features

- **MongoDB-like Aggregation Framework**
  - Support for stages like `$match`, `$project`, `$group`, `$lookup`, `$sort`, `$limit`, and more
  - Flexible querying capabilities without the complexity of a full database server

- **Efficient Storage**
  - Leverages Badger as a high-performance key-value store backend
  - Supports in-memory and persistent storage configurations
  - Supports encryption at REST with AES-256-GCM

- **Schema-less Documents**
  - Store and query JSON-like documents without a predefined schema
  - Automatic indexing for efficient querying
  - Support for document references and nested fields

- **ACID Transactions**
  - Guarantees data consistency and integrity with ACID transactions
  - Supports multi-document transactions for complex operations

- **Advanced Data Handling**
  - Recursive graph traversal for resolving document references
  - Flexible type handling and nested field support

## Installation

```bash
go get github.com/marcgauthier/marco
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/user/marco"
    "github.com/dgraph-io/badger/v3"
)

func main() {
    // Open a Gongo database
    opts := badger.DefaultOptions("./data").WithInMemory(true)
    db, err := marco.Open(opts)
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Insert a document
    user := map[string]interface{}{
        "name": "John Doe", 
        "age": 30
    }
    id, err := db.Put("users", "", user)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Inserted user with ID: %s\n", id)

    // Retrieve all users
    users, err := db.Collection("users")
    if err != nil {
        panic(err)
    }
    fmt.Printf("Users: %v\n", users)
}
```

## Core Operations

### Document Management
- `Put(collection, id string, value map[string]interface{})`: Insert or update documents
- `Get(collection, id string)`: Retrieve a document by Collection and ID
- `GetID(id string)`: Retrieve a document by its unique ID
- `Delete(collection, id string)`: Remove a document
- `Collection(collection string)`: List all documents in a collection
- `Query(collection string, query map[string]interface{})`: Query documents based on mongo style queries


### Advanced Querying

MarcoDB supports MongoDB-like aggregation stages:

- `$match`: Filter documents
- `$project`: Transform document structure
- `$group`: Aggregate and group data
- `$lookup`: Perform cross-collection joins
- `$sort`: Order results
- `$limit` and `$skip`: Paginate results
- `$facet`: Run multiple pipelines
- `$unwind`: Deconstruct arrays
- `$skip`: Skip a number of documents

#### Example Aggregation

```go
pipeline := []map[string]interface{}{
    {"$match": {"age": {"$gt": 25}}},
    {"$group": {
        "_id": "$city", 
        "averageAge": {"$avg": "$age"}
    }}
}
results := db.Aggregate("users", pipeline)
```

## Badger Integration

All Badger features are accessible through the low-level Badger object:

```go
badgerDB := db.Badger()
// Use Badger's advanced features like backup, encryption, etc.
```

## Performance and Optimization using badger KeyValue storage engine.

- In-memory and persistent storage options
- Compression support (ZSTD)
- Configurable caching
- Encryption key rotation

## Current Limitations

- No built-in indexing (planned for future versions)
- Queries require full collection iteration, so this is not suitable for large collections (planned for future versions)

## Contributing

Contributions are welcome! Please submit pull requests or open issues on the GitHub repository.

## License

MIT License. See `LICENSE` for details.

## Inspirations

MarcoDB is designed to bring to Golang what PoloDB offers to Rust—a straightforward, embedded document database with robust querying capabilities. Inspired by the explorer Marco Polo, the name MarcoDB symbolizes the goal to deliver PoloDB-like functionality to the Go ecosystem.