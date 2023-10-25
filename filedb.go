package filedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rogpeppe/go-internal/lockedfile"
)

// DB is a file-based database.
type DB[D any] struct {
	root string
}

type dbEntry[D any] struct {
	version int
	doc     *D
}

func keyPath(root string, key string) string {
	return filepath.Join(root, filepath.ToSlash(key))
}

func tmpFile(root string, key string) (*os.File, error) {
	key = filepath.ToSlash(key)
	f, err := os.CreateTemp(filepath.Join(root, filepath.Dir(key)), filepath.Base(key)+"...tmp.*")
	if err != nil {
		return nil, err
	}
	return f, nil
}

func lockPath(root string, key string) string {
	return filepath.Join(root, filepath.ToSlash(key)+"...lock")
}

// Open opens/initializes a database at the given path.
func Open[D any](root string) (*DB[D], error) {

	initPath := filepath.Join(root, ".filedb")

	ents, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		if err := os.Mkdir(initPath, 0600); err != nil {
			return nil, fmt.Errorf("cannot create .filedb: %w", err)
		}
	} else {
		if _, err := os.Stat(initPath); err != nil {
			return nil, errors.New("root not empty and cannot read .filedb")
		}
	}

	return &DB[D]{root}, nil
}

func (db *DB[D]) get(key string) (dbEntry[D], error) {
	f, err := os.Open(keyPath(db.root, key))
	if err != nil {
		return dbEntry[D]{}, err
	}
	defer f.Close()

	var e dbEntry[D]
	if err := json.NewDecoder(f).Decode(&e); err != nil {
		return dbEntry[D]{}, err
	}
	return e, nil
}

// Get returns the document for the given key, or nil if it does not exist.
func (db *DB[D]) Get(key string) (*D, error) {
	e, err := db.get(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	return e.doc, nil
}

// List returns the keys in the database that have the given prefix.
func (db *DB[D]) List(prefix string) ([]string, error) {
	// TODO add prefix santitization (no slashes before or after, no .., etc.)
	ents, err := os.ReadDir(filepath.Join(db.root, filepath.ToSlash(prefix)))
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, ent := range ents {
		if ent.IsDir() || strings.HasSuffix(ent.Name(), "...tmp") || strings.HasSuffix(ent.Name(), "...lock") {
			continue
		}
		key := ent.Name()
		keys = append(keys, key)
	}
	return keys, nil
}

// Set sets the document for the given key.
func (db *DB[D]) Set(key string, doc *D) error {
	_, err := db.Update(key, func(_ *D) (*D, error) { return doc, nil })
	return err
}

// Delete deletes the document for the given key.
func (db *DB[D]) Delete(key string) error {
	_, err := db.Update(key, func(_ *D) (*D, error) { return nil, nil })
	return err
}

// Update updates the document for the given key, using the given function to
// apply the update. The function will be called with the current document for
// the key, or nil if the key does not exist. If the function returns nil, the
// key will be deleted.
func (db *DB[D]) Update(key string, apply func(existing *D) (*D, error)) (*D, error) {

	var errConcurrentMod = errors.New("concurrent modification")

	do := func() (*D, error) {
		// Get the current doc for its version. Non-existent files will end with
		// version 0.

		old, err := db.get(key)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}

		// Apply the update function to get the new doc.

		newDoc, err := apply(old.doc)
		if err != nil {
			return nil, err
		}
		if newDoc == nil {
			if err := os.Remove(keyPath(db.root, key)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("failed to remove key '%s': %w", key, err)
			}
			return nil, nil
		}

		f, err := tmpFile(db.root, key)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		defer os.Remove(f.Name())

		if err := json.NewEncoder(f).Encode(dbEntry[D]{old.version + 1, newDoc}); err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}

		// Lock writing to the key.

		unlock, err := lockedfile.MutexAt(lockPath(db.root, key)).Lock()
		if err != nil {
			return nil, err
		}
		defer unlock()

		neww, err := db.get(key)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
		if neww.version != old.version {
			return nil, errConcurrentMod
		}

		return newDoc, os.Rename(f.Name(), keyPath(db.root, key))
	}

	for {
		doc, err := do()
		if err == nil || err != errConcurrentMod {
			return doc, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}
