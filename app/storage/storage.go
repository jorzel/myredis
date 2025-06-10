package storage

import (
	"fmt"
	"sync"
	"time"
)

type KVRecord struct {
	Value    string
	ExpireAt *time.Time
}

var _ Storage = (*DefaultStorage)(nil)

type Storage interface {
	Get(key string) (*KVRecord, error)
	Set(key string, value *KVRecord) error
	Del(key string) error
}

type DefaultStorage struct {
	db sync.Map
}

func NewStorage() *DefaultStorage {
	return &DefaultStorage{
		db: sync.Map{},
	}
}

func (s *DefaultStorage) Get(key string) (*KVRecord, error) {
	record, ok := s.db.Load(key)
	if !ok {
		return nil, nil
	}
	deserializedRecord, ok := record.(*KVRecord)
	if !ok {
		return nil, fmt.Errorf("failed to deserialize record for key%s, record=%v", key, record)
	}
	return deserializedRecord, nil
}

func (s *DefaultStorage) Set(key string, value *KVRecord) error {
	s.db.Store(key, value)
	return nil
}

func (s *DefaultStorage) Del(key string) error {
	if _, ok := s.db.Load(key); !ok {
		return fmt.Errorf("key %s does not exist", key)
	}
	s.db.Delete(key)
	return nil
}
