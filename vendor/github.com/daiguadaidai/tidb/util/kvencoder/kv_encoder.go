// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kvenc

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/daiguadaidai/parser/mysql"
	"github.com/daiguadaidai/tidb/domain"
	"github.com/daiguadaidai/tidb/kv"
	"github.com/daiguadaidai/tidb/meta"
	"github.com/daiguadaidai/tidb/meta/autoid"
	"github.com/daiguadaidai/tidb/session"
	"github.com/daiguadaidai/tidb/store/mockstore"
	"github.com/daiguadaidai/tidb/tablecodec"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
)

var _ KvEncoder = &kvEncoder{}
var mockConnID uint64

// KvPair is a key-value pair.
type KvPair struct {
	// Key is the key of the pair.
	Key []byte
	// Val is the value of the pair. if the op is delete, the len(Val) == 0
	Val []byte
}

// KvEncoder is an encoder that transfer sql to key-value pairs.
type KvEncoder interface {
	// Encode transfers sql to kv pairs.
	// Before use Encode() method, please make sure you already created schame by calling ExecDDLSQL() method.
	// NOTE: now we just support transfers insert statement to kv pairs.
	// (if we wanna support other statement, we need to add a kv.Storage parameter,
	// and pass tikv store in.)
	// return encoded kvs array that generate by sql, and affectRows count.
	Encode(sql string, tableID int64) (kvPairs []KvPair, affectedRows uint64, err error)

	// PrepareStmt prepare query statement, and return statement id.
	// Pass stmtID into EncodePrepareStmt to execute a prepare statement.
	PrepareStmt(query string) (stmtID uint32, err error)

	// EncodePrepareStmt transfer prepare query to kv pairs.
	// stmtID is generated by PrepareStmt.
	EncodePrepareStmt(tableID int64, stmtID uint32, param ...interface{}) (kvPairs []KvPair, affectedRows uint64, err error)

	// ExecDDLSQL executes ddl sql, you must use it to create schema infos.
	ExecDDLSQL(sql string) error

	// EncodeMetaAutoID encode the table meta info, autoID to coresponding key-value pair.
	EncodeMetaAutoID(dbID, tableID, autoID int64) (KvPair, error)

	// SetSystemVariable set system variable name = value.
	SetSystemVariable(name string, value string) error

	// GetSystemVariable get the system variable value of name.
	GetSystemVariable(name string) (string, bool)

	// Close cleanup the kvEncoder.
	Close() error
}

var (
	// refCount is used to ensure that there is only one domain.Domain instance.
	refCount    int64
	mu          sync.Mutex
	storeGlobal kv.Storage
	domGlobal   *domain.Domain
)

type kvEncoder struct {
	se    session.Session
	store kv.Storage
	dom   *domain.Domain
}

// New new a KvEncoder
func New(dbName string, idAlloc autoid.Allocator) (KvEncoder, error) {
	kvEnc := &kvEncoder{}
	mu.Lock()
	defer mu.Unlock()
	if refCount == 0 {
		if err := initGlobal(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	err := kvEnc.initial(dbName, idAlloc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	refCount++
	return kvEnc, nil
}

func (e *kvEncoder) Close() error {
	e.se.Close()
	mu.Lock()
	defer mu.Unlock()
	refCount--
	if refCount == 0 {
		e.dom.Close()
		if err := e.store.Close(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *kvEncoder) Encode(sql string, tableID int64) (kvPairs []KvPair, affectedRows uint64, err error) {
	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	defer e.se.RollbackTxn(context.Background())

	_, err = e.se.Execute(context.Background(), sql)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	return e.getKvPairsInMemBuffer(tableID)
}

func (e *kvEncoder) getKvPairsInMemBuffer(tableID int64) (kvPairs []KvPair, affectedRows uint64, err error) {
	txn, err := e.se.Txn(true)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	txnMemBuffer := txn.GetMemBuffer()
	kvPairs = make([]KvPair, 0, txnMemBuffer.Len())
	err = kv.WalkMemBuffer(txnMemBuffer, func(k kv.Key, v []byte) error {
		if bytes.HasPrefix(k, tablecodec.TablePrefix()) {
			k = tablecodec.ReplaceRecordKeyTableID(k, tableID)
		}
		kvPairs = append(kvPairs, KvPair{Key: k, Val: v})
		return nil
	})

	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return kvPairs, e.se.GetSessionVars().StmtCtx.AffectedRows(), nil
}

func (e *kvEncoder) PrepareStmt(query string) (stmtID uint32, err error) {
	stmtID, _, _, err = e.se.PrepareStmt(query)
	return
}

func (e *kvEncoder) EncodePrepareStmt(tableID int64, stmtID uint32, param ...interface{}) (kvPairs []KvPair, affectedRows uint64, err error) {
	e.se.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, true)
	defer e.se.RollbackTxn(context.Background())

	_, err = e.se.ExecutePreparedStmt(context.Background(), stmtID, param...)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	return e.getKvPairsInMemBuffer(tableID)
}

func (e *kvEncoder) EncodeMetaAutoID(dbID, tableID, autoID int64) (KvPair, error) {
	mockTxn := kv.NewMockTxn()
	m := meta.NewMeta(mockTxn)
	k, v := m.GenAutoTableIDIDKeyValue(dbID, tableID, autoID)
	return KvPair{Key: k, Val: v}, nil
}

func (e *kvEncoder) ExecDDLSQL(sql string) error {
	_, err := e.se.Execute(context.Background(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *kvEncoder) SetSystemVariable(name string, value string) error {
	name = strings.ToLower(name)
	if e.se != nil {
		return e.se.GetSessionVars().SetSystemVar(name, value)
	}
	return errors.Errorf("e.se is nil, please new KvEncoder by kvencoder.New().")
}

func (e *kvEncoder) GetSystemVariable(name string) (string, bool) {
	name = strings.ToLower(name)
	if e.se == nil {
		return "", false
	}

	return e.se.GetSessionVars().GetSystemVar(name)
}

func newMockTikvWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

func (e *kvEncoder) initial(dbName string, idAlloc autoid.Allocator) (err error) {
	se, err := session.CreateSession(storeGlobal)
	if err != nil {
		err = errors.Trace(err)
		return
	}

	se.SetConnectionID(atomic.AddUint64(&mockConnID, 1))
	_, err = se.Execute(context.Background(), fmt.Sprintf("create database if not exists %s", dbName))
	if err != nil {
		err = errors.Trace(err)
		return
	}
	_, err = se.Execute(context.Background(), fmt.Sprintf("use %s", dbName))
	if err != nil {
		err = errors.Trace(err)
		return
	}

	se.GetSessionVars().IDAllocator = idAlloc
	se.GetSessionVars().LightningMode = true
	se.GetSessionVars().SkipUTF8Check = true
	e.se = se
	e.store = storeGlobal
	e.dom = domGlobal
	return nil
}

// initGlobal modify the global domain and store
func initGlobal() error {
	// disable stats update.
	session.SetStatsLease(0)
	var err error
	storeGlobal, domGlobal, err = newMockTikvWithBootstrap()
	if err == nil {
		return nil
	}

	if storeGlobal != nil {
		if err1 := storeGlobal.Close(); err1 != nil {
			log.Error(errors.ErrorStack(err1))
		}
	}
	if domGlobal != nil {
		domGlobal.Close()
	}
	return errors.Trace(err)
}
