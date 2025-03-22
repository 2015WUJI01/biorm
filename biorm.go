package biorm

import (
	"context"
	"fmt"
	"log"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
)

type Config struct {

	// 每次请求的间隔时间，单位为毫秒，默认为 1s
	RequestInterval time.Duration
}

type DB struct {
	cli *lark.Client
	*Config

	// op values
	Statement Statement

	AppToken string
	TableId  string
	ViewId   string

	ApiResp   *larkcore.ApiResp
	CodeError *larkcore.CodeError
	Error     error
}

func NewDB(cli *lark.Client) *DB {
	db := &DB{
		cli: cli,
		Config: &Config{
			RequestInterval: 1 * time.Second,
		},
	}
	db.Statement = Statement{
		DB:         db,
		Context:    context.Background(),
		UserIdType: "open_id",
	}
	return db
}

// getInstance 返回一个当前 DB 的副本，实现链式调用的隔离
func (db *DB) getInstance() *DB {
	// 直接使用Clone方法，保持一致性
	return db.Clone()
}

// Clone 克隆当前DB实例，包括条件设置
func (db *DB) Clone() *DB {
	if db.hasError() {
		return db
	}

	log.Printf("[Clone调试] 开始克隆实例")

	newDb := &DB{
		cli:       db.cli,
		Config:    db.Config,
		AppToken:  db.AppToken,
		TableId:   db.TableId,
		ViewId:    db.ViewId,
		ApiResp:   db.ApiResp,
		CodeError: db.CodeError,
		Error:     db.Error,
	}

	// 创建新的 Statement，并复制所有条件
	newDb.Statement = Statement{
		DB:              newDb,
		Context:         db.Statement.Context,
		AppToken:        db.Statement.AppToken,
		TableId:         db.Statement.TableId,
		UserIdType:      db.Statement.UserIdType,
		AutomaticFields: db.Statement.AutomaticFields,
		Idempotent:      db.Statement.Idempotent,
		ClientToken:     db.Statement.ClientToken,
		Dest:            db.Statement.Dest,
		Selects:         make([]string, len(db.Statement.Selects)),
	}

	// 复制 Selects
	if len(db.Statement.Selects) > 0 {
		copy(newDb.Statement.Selects, db.Statement.Selects)
	}

	// 复制 Filter
	if db.Statement.Filter.Conjunction != nil {
		conjunction := *db.Statement.Filter.Conjunction
		newDb.Statement.Filter.Conjunction = &conjunction

		// 深度复制所有条件
		if len(db.Statement.Filter.Conditions) > 0 {
			newDb.Statement.Filter.Conditions = make([]*larkbitable.Condition, len(db.Statement.Filter.Conditions))
			for i, cond := range db.Statement.Filter.Conditions {
				if cond != nil {
					newCond := &larkbitable.Condition{}
					if cond.FieldName != nil {
						fieldName := *cond.FieldName
						newCond.FieldName = &fieldName
					}
					if cond.Operator != nil {
						operator := *cond.Operator
						newCond.Operator = &operator
					}
					if len(cond.Value) > 0 {
						newCond.Value = make([]string, len(cond.Value))
						copy(newCond.Value, cond.Value)
					}
					newDb.Statement.Filter.Conditions[i] = newCond
				}
			}
		}
	} else {
		// 初始化空的过滤条件
		and := "and"
		newDb.Statement.Filter = larkbitable.FilterInfo{
			Conjunction: &and,
			Conditions:  make([]*larkbitable.Condition, 0),
		}
	}

	// 复制Sort
	if len(db.Statement.Sort) > 0 {
		newDb.Statement.Sort = make([]*larkbitable.Sort, len(db.Statement.Sort))
		for i, s := range db.Statement.Sort {
			if s != nil {
				newSort := &larkbitable.Sort{}
				if s.FieldName != nil {
					fieldName := *s.FieldName
					newSort.FieldName = &fieldName
				}
				if s.Desc != nil {
					desc := *s.Desc
					newSort.Desc = &desc
				}
				newDb.Statement.Sort[i] = newSort
			}
		}
	}

	log.Printf("[Clone调试] 克隆完成，Filter: conjunction=%v, conditions长度=%d",
		newDb.Statement.Filter.Conjunction, len(newDb.Statement.Filter.Conditions))
	for i, cond := range newDb.Statement.Filter.Conditions {
		if cond != nil && cond.FieldName != nil && cond.Operator != nil {
			log.Printf("[Clone调试] 条件[%d]: 字段=%s, 操作符=%s, 值=%v",
				i, *cond.FieldName, *cond.Operator, cond.Value)
		}
	}

	return newDb
}

// Finalize 释放DB实例中的资源，帮助垃圾回收
func (db *DB) Finalize() {
	if db == nil {
		return
	}

	log.Printf("[Finalize调试] 开始释放实例资源")

	// 清理Statement中的资源
	if len(db.Statement.Selects) > 0 {
		db.Statement.Selects = nil
	}

	if len(db.Statement.Filter.Conditions) > 0 {
		for i := range db.Statement.Filter.Conditions {
			db.Statement.Filter.Conditions[i] = nil
		}
		db.Statement.Filter.Conditions = nil
	}

	if len(db.Statement.Sort) > 0 {
		for i := range db.Statement.Sort {
			db.Statement.Sort[i] = nil
		}
		db.Statement.Sort = nil
	}

	// 清理其他可能的引用
	db.Statement.Dest = nil
	db.ApiResp = nil

	log.Printf("[Finalize调试] 实例资源释放完成")
}

// hasError 检查是否有错误，避免在有错误的情况下继续操作
func (db *DB) hasError() bool {
	return db.Error != nil
}

func (db *DB) ErrorString() string {
	s := db.Error.Error()
	if db.ApiResp != nil {
		s += fmt.Sprintf("\n[ApiResp] %s", db.ApiResp.String())
	}
	if db.CodeError != nil {
		s += fmt.Sprintf("\n[CodeError] %s", db.CodeError.ErrorResp())
	}
	return s
}
