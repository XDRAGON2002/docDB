package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Driver struct {
	mutex      sync.Mutex
	mutexes    map[string]*sync.Mutex
	dir        string
	collection string
}

func New(dir string) (*Driver, error) {
	dir = filepath.Clean(dir)

	driver := &Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
	}

	if _, err := os.Stat(dir); err == nil {
		return driver, nil
	}

	return driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Collection(collection string) *Driver {
	d.collection = collection
	return d
}

func (d *Driver) Write(key string, v interface{}) error {
	if d.collection == "" {
		return errors.New("Collection not defined")
	}

	mutex := d.getCollectionMutex()

	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, d.collection)

	finalPath := filepath.Join(dir, fmt.Sprintf("%s.json", key))
	tempPath := fmt.Sprintf("%s.tmp", finalPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	if err := os.WriteFile(finalPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tempPath, finalPath)
}

func (d *Driver) Read(key string, v interface{}) error {
	if d.collection == "" {
		return errors.New("Collection not defined")
	}

	record := filepath.Join(d.dir, d.collection, key)

	if _, err := stat(record); err != nil {
		return err
	}

	b, err := os.ReadFile(fmt.Sprintf("%s.json", record))
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll() ([]string, error) {
	if d.collection == "" {
		return nil, errors.New("Collection not defined")
	}

	dir := filepath.Join(d.dir, d.collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var records []string

	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return records, err
		}
		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(key string) error {
	if d.collection == "" {
		return errors.New("Collection not defined")
	}

	record := filepath.Join(d.dir, d.collection, key)

	mutex := d.getCollectionMutex()
	
	mutex.Lock()
	defer mutex.Unlock()

	switch fi, err := stat(record); {
	case fi == nil, err != nil:
		return errors.New("Record doesn't exist")
	case fi.Mode().IsDir():
		return os.RemoveAll(record)
	case fi.Mode().IsRegular():
		return os.RemoveAll(fmt.Sprintf("%s.json", record))
	}

	return nil
}

func (d *Driver) getCollectionMutex() *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	mutex, ok := d.mutexes[d.collection]
	if !ok {
		mutex = &sync.Mutex{}
		d.mutexes[d.collection] = mutex
	}
	return mutex
}

func stat(path string) (os.FileInfo, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.Stat(fmt.Sprintf("%s.json", path))
	}
	return os.Stat(path)
}