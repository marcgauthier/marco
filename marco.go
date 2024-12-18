package marco

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
)

// DB wraps a Badger database instance and offers convenience methods
// for CRUD operations, secondary indexing, and recursive graph traversal.
type DB struct {
	db *badger.DB
}

// Open initializes a new DB instance using the given badger.Options.
func Open(opts badger.Options) (*DB, error) {
	db := new(DB)

	var err error
	db.db, err = badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// Badger returns the raw *badger.DB instance for advanced usage.
func (db *DB) Badger() *badger.DB {
	return db.db
}

// Close gracefully shuts down the underlying Badger database.
func (db *DB) Close() error {
	return db.db.Close()
}

// Put inserts or updates a document in the specified collection.
//
// If 'id' is empty, a new UUID is generated. If provided, 'id' must be a valid
// UUID (string form). Internally, we store that UUID as a 16-byte binary key.
//
// For each document stored:
//   - Primary key = collection prefix + ":" + [16-byte binary UUID]
//   - Secondary key = [16-byte binary UUID], pointing to the primary key.
func (db *DB) Put(collection, id string, value map[string]interface{}) (string, error) {
	if collection == "" {
		return "", fmt.Errorf("collection name is empty, cannot insert document ID: %s", id)
	}

	// Generate or parse UUID
	var u uuid.UUID
	var err error
	if id == "" {
		u = uuid.New()
		id = u.String() // Return the string form to caller, though stored as binary
	} else {
		// Validate user-provided ID
		u, err = uuid.Parse(id)
		if err != nil {
			return "", fmt.Errorf("invalid UUID provided: %s", id)
		}
	}

	// Convert UUID to its 16-byte binary form
	uBytes, err := u.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("unable to marshal UUID to binary: %v", err)
	}

	// Construct the primary key
	// Format: collection + ":" + 16-byte UUID
	primaryKey := append([]byte(collection+":"), uBytes...)

	// Transaction to store the data
	err = db.db.Update(func(txn *badger.Txn) error {
		// Convert the document to JSON
		val, err := json.Marshal(value)
		if err != nil {
			return err
		}

		// Set the primary key in Badger with the JSON value
		if err := txn.Set(primaryKey, val); err != nil {
			return err
		}

		// Secondary key is the 16-byte UUID only
		secondaryKey := uBytes
		return txn.Set(secondaryKey, primaryKey)
	})

	if err != nil {
		return "", err
	}
	return id, nil
}

// Get retrieves a document by (collection, id).
//
// Internally, the primary key is `collection + ":" + binary-16-byte-UUID`.
func (db *DB) Get(collection, id string) (map[string]interface{}, error) {
	var doc map[string]interface{}

	// Parse the string UUID to binary
	u, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID: %s", id)
	}
	uBytes, _ := u.MarshalBinary()

	// Construct the primary key
	primaryKey := append([]byte(collection+":"), uBytes...)

	err = db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(primaryKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return errors.New("document not found")
			}
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &doc)
		})
	})
	if err != nil {
		return nil, err
	}
	return doc, nil
}

// GetID retrieves a document using only the secondary key (which is the 16-byte binary UUID).
// 1. Looks up `uBytes` -> primaryKey (collection + ":" + uBytes).
// 2. Uses that primaryKey to fetch the actual document.
func (db *DB) GetID(id string) (map[string]interface{}, error) {
	var doc map[string]interface{}

	// Parse the string UUID to binary
	u, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID for GetID: %s", id)
	}
	uBytes, _ := u.MarshalBinary()

	err = db.db.View(func(txn *badger.Txn) error {
		// Lookup the primary key via the secondary index
		item, err := txn.Get(uBytes)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return errors.New("secondary key not found")
			}
			return err
		}

		// Copy out the primaryKey bytes
		var primaryKey []byte
		err = item.Value(func(val []byte) error {
			primaryKey = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}

		// Fetch the document using the primary key
		item, err = txn.Get(primaryKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return errors.New("primary key not found")
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &doc)
		})
	})
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// Collection returns all documents of the specified collection by prefix scanning.
// The prefix is simply `collection + ":"` in ASCII, followed by 16 bytes of UUID data.
func (db *DB) Collection(collection string) ([]map[string]interface{}, error) {
	prefix := []byte(collection + ":")
	var docs []map[string]interface{}

	err := db.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			var doc map[string]interface{}
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &doc)
			}); err != nil {
				return err
			}

			docs = append(docs, doc)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return docs, nil
}

// DropAll deletes all keys and data from the Badger database.
func (db *DB) DropAll() error {
	return db.db.DropAll()
}

// Delete removes a single document by (collection, id), along with its associated
// secondary key. We compute the same key format in binary form.
func (db *DB) Delete(collection, id string) error {
	u, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("invalid UUID for Delete: %s", id)
	}
	uBytes, _ := u.MarshalBinary()
	primaryKey := append([]byte(collection+":"), uBytes...)

	err = db.db.Update(func(txn *badger.Txn) error {
		// Delete the primary key
		if err := txn.Delete(primaryKey); err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("item with ID %s not found in collection %s", id, collection)
			}
			return err
		}

		// Delete the secondary key (the 16-byte UUID)
		if err := txn.Delete(uBytes); err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("secondary key with ID %s not found", id)
			}
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to delete item and its secondary key: %w", err)
	}
	return nil
}

// DropCollection removes all documents in a specified collection by prefix-scanning
// and also removes their corresponding secondary keys (the trailing 16 bytes).
func (db *DB) DropCollection(collection string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		collectionPrefix := []byte(collection + ":")

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		var deletionErr error

		for it.Seek(collectionPrefix); it.ValidForPrefix(collectionPrefix); it.Next() {
			item := it.Item()
			primaryKey := item.KeyCopy(nil)

			// The last 16 bytes should be the binary UUID part
			if len(primaryKey) < len(collectionPrefix)+16 {
				// Malformed or unexpected key format, skip
				continue
			}
			uBytes := primaryKey[len(collectionPrefix):]

			// Delete the secondary key first
			if err := txn.Delete(uBytes); err != nil {
				if err != badger.ErrKeyNotFound {
					deletionErr = fmt.Errorf("failed to delete secondary key %x: %w", uBytes, err)
					continue
				}
			}

			// Delete the primary key
			if err := txn.Delete(primaryKey); err != nil {
				if err != badger.ErrKeyNotFound {
					deletionErr = fmt.Errorf("failed to delete primary key %x: %w", primaryKey, err)
				}
			}
		}

		if deletionErr != nil {
			return deletionErr
		}
		return nil
	})
}

// RecursiveGraphTraversal fetches a document by 'id', then recursively processes its fields
// to see if they contain UUID references to other documents. If a reference is found, it is replaced
// with the referenced content, up to 'maxRecursive' levels.
//
// If 'maxRecursive < 0', unlimited recursion is allowed.
//
// Example scenario:
//   - If a field is a UUID string, we fetch that doc and optionally repeat up to maxRecursive levels.
func (db *DB) RecursiveGraphTraversal(id string, maxRecursive int) (map[string]interface{}, error) {
	// Fetch the top-level document by secondary key
	item, err := db.GetID(id)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, fmt.Errorf("no data found for ID %s", id)
	}

	// Recursively process the item with an initial depth of 0
	processed := db.processObjectWithLevel(item, 0, maxRecursive)
	return processed, nil
}

// processObjectWithLevel traverses 'obj' checking each field.
// - If the field is a string and a valid UUID, fetch the referenced object if recursion allows.
// - If the field is a slice or nested map, recurse deeper if within maxRecursive limit.
// - maxLevel < 0 => infinite recursion. If currentLevel >= maxLevel, we don't recurse further.
func (db *DB) processObjectWithLevel(obj map[string]interface{}, currentLevel, maxLevel int) map[string]interface{} {
	for key, value := range obj {
		switch v := value.(type) {

		case string:
			// Check if recursion is allowed
			if maxLevel < 0 || currentLevel < maxLevel {
				obj[key] = db.fetchAndProcessUUIDWithLevel(v, currentLevel, maxLevel)
			}

		case []interface{}:
			// If recursion is allowed, process elements
			if maxLevel < 0 || currentLevel < maxLevel {
				for i, elem := range v {
					switch elemVal := elem.(type) {
					case string:
						v[i] = db.fetchAndProcessUUIDWithLevel(elemVal, currentLevel, maxLevel)
					case map[string]interface{}:
						v[i] = db.processObjectWithLevel(elemVal, currentLevel+1, maxLevel)
					}
				}
			}
			obj[key] = v

		case map[string]interface{}:
			if maxLevel < 0 || currentLevel < maxLevel {
				obj[key] = db.processObjectWithLevel(v, currentLevel+1, maxLevel)
			}

		case []map[string]interface{}:
			if maxLevel < 0 || currentLevel < maxLevel {
				for i, submap := range v {
					v[i] = db.processObjectWithLevel(submap, currentLevel+1, maxLevel)
				}
				obj[key] = v
			}
		}
	}
	return obj
}

// fetchAndProcessUUIDWithLevel attempts to parse 's' as a UUID. If valid and the doc is found,
// it recursively processes the doc if 'maxLevel' recursion is allowed.
// Otherwise, returns the original string.
func (db *DB) fetchAndProcessUUIDWithLevel(s string, currentLevel, maxLevel int) interface{} {
	// Check if 's' is a valid UUID
	if _, err := uuid.Parse(s); err != nil {
		return s // Not a valid UUID; return original string
	}

	// Attempt to fetch the object by this UUID
	fetchedObj, err := db.GetID(s)
	if err != nil || fetchedObj == nil {
		return s // Return the original string if not found
	}

	// If recursion is not exceeded, process the fetched object further
	if maxLevel < 0 || currentLevel < maxLevel {
		return db.processObjectWithLevel(fetchedObj, currentLevel+1, maxLevel)
	}

	// If we've reached the limit, just return the fetched document as-is
	return fetchedObj
}
