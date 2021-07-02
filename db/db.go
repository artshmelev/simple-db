package db

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

const (
	dbLogFileName     = "log.db"
	hashIndexFileName = "hash.index"
)

type DB interface {
	Set(k, v string) error
	Get(k string) (string, error)
	Close()
}

type db struct {
	fd   *os.File
	hmap map[string]int64
}

func NewDB() (DB, error) {
	fd, err := os.OpenFile(dbLogFileName, os.O_RDWR|os.O_CREATE, 0744)
	if err != nil {
		return nil, err
	}
	db := &db{
		fd:   fd,
		hmap: make(map[string]int64),
	}
	db.readHashIndex()

	return db, nil
}

type keyValue struct {
	key   string
	value string
}

func (db *db) Set(k, v string) error {
	offset, err := db.fd.Seek(0, 2)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	bufVarint := make([]byte, 2*binary.MaxVarintLen64)
	bufVarintK := bufVarint[:binary.MaxVarintLen64]
	bufVarintV := bufVarint[binary.MaxVarintLen64:]

	n := binary.PutVarint(bufVarintK, int64(len(k)))
	buf.Write(bufVarintK[:n])
	buf.WriteString(k)
	n = binary.PutVarint(bufVarintV, int64(len(v)))
	buf.Write(bufVarintV[:n])
	buf.WriteString(v)

	if _, err := buf.WriteTo(db.fd); err != nil {
		return err
	}
	if err := db.fd.Sync(); err != nil {
		return err
	}
	db.hmap[k] = offset
	return nil
}

func (db *db) Get(k string) (string, error) {
	offset, ok := db.hmap[k]
	if !ok {
		return "", nil
	}

	bufVarint := make([]byte, 2*binary.MaxVarintLen64)
	bufVarintK := bufVarint[:binary.MaxVarintLen64]
	bufVarintV := bufVarint[binary.MaxVarintLen64:]

	n, err := db.fd.ReadAt(bufVarintK, offset)
	if err != nil {
		if err != io.EOF || n <= 0 {
			return "", err
		}
	}
	lenK, n := binary.Varint(bufVarintK)
	offset += lenK + int64(n)

	n, err = db.fd.ReadAt(bufVarintV, offset)
	if err != nil {
		if err != io.EOF || n <= 0 {
			return "", err
		}
	}
	lenV, n := binary.Varint(bufVarintV)
	offset += int64(n)

	buf := make([]byte, lenV)
	_, err = db.fd.ReadAt(buf, offset)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func (db *db) Close() {
	db.writeHashIndex()
	db.fd.Close()
}

func (db *db) readHashIndex() error {
	fd, err := os.Open(hashIndexFileName)
	if err != nil {
		return hmap, err
	}
}

func putVarintString(buf *bytes.Buffer, s string) {
	bufVarint := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(bufVarint, int64(len(s)))
	buf.Write(bufVarint[:n])
	buf.WriteString(s)
}

func (db *db) writeHashIndex() error {
	var buf bytes.Buffer
	for k, v := range db.hmap {
		putVarintString(&buf, k)
		putVarintString(&buf, v)
	}
}
