package biorm

import (
	"context"
	"log"
	"strings"

	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
	larkwiki "github.com/larksuite/oapi-sdk-go/v3/service/wiki/v2"
)

// Base 选择飞书表格
func (db *DB) Base(appToken string, args ...interface{}) (tx *DB) {
	log.Printf("[Base调试] 设置appToken=%s", appToken)
	tx = db.getInstance()
	tx.AppToken = appToken
	return tx
}

// BaseTable 使用拼接后的 ID 值作为表格标识符
func (db *DB) BaseTable(combinedId string, args ...interface{}) (tx *DB) {
	parts := strings.Split(combinedId, ".")
	if len(parts) != 2 {
		db.Error = ErrParseAppTokenAndTableId
		return db
	}
	return db.Base(parts[0]).Table(parts[1])
}

// Wiki 选择飞书表格
func (db *DB) Wiki(appToken string, args ...interface{}) (tx *DB) {
	log.Printf("[Wiki调试] 开始处理appToken=%s", appToken)
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if appToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}

	req := larkwiki.NewGetNodeSpaceReqBuilder().Token(appToken).ObjType(`wiki`).Build()

	// 发起请求
	resp, err := tx.cli.Wiki.V2.Space.GetNode(context.Background(), req)

	// 处理错误
	if err != nil {
		tx.Error = err
		if resp != nil {
			tx.ApiResp = resp.ApiResp
			tx.CodeError = &resp.CodeError
		}
		return
	}
	if resp == nil {
		tx.Error = ErrResponseIsNil
		return
	}

	if *resp.Data.Node.ObjType != "bitable" {
		tx.Error = ErrObjTypeNotBitable
		return
	}

	// 处理业务
	tx.AppToken = *resp.Data.Node.ObjToken
	log.Printf("[Wiki调试] 设置appToken=%s", tx.AppToken)
	return tx
}

// WikiTable 使用拼接后的 ID 值作为表格标识符
func (db *DB) WikiTable(combinedId string, args ...interface{}) (tx *DB) {
	log.Printf("[WikiTable调试] 接收combinedId=%s", combinedId)

	parts := strings.Split(combinedId, ".")
	if len(parts) != 2 {
		db.Error = ErrParseAppTokenAndTableId
		return db
	}

	log.Printf("[WikiTable调试] 解析成功: appToken=%s, tableId=%s", parts[0], parts[1])
	tx = db.Wiki(parts[0]).Table(parts[1])

	// 检查实例状态
	if tx != nil && tx.Statement.Filter.Conjunction != nil {
		log.Printf("[WikiTable调试] 返回的实例: appToken=%s, tableId=%s, filter.conjunction=%s, filter.conditions长度=%d",
			tx.AppToken, tx.TableId, *tx.Statement.Filter.Conjunction, len(tx.Statement.Filter.Conditions))
	} else if tx != nil {
		log.Printf("[WikiTable调试] 返回的实例: appToken=%s, tableId=%s, filter.conjunction=nil",
			tx.AppToken, tx.TableId)
	}

	return tx
}

func (db *DB) Table(tableId string, args ...interface{}) (tx *DB) {
	log.Printf("[Table调试] 设置tableId=%s", tableId)
	tx = db.getInstance()
	tx.TableId = tableId
	return tx
}

// Idempotent 描述：格式为标准的 uuid，操作的唯一标识，用于幂等的进行更新操作。此值为空表示将发起一次新的请求，此值非空表示幂等的进行更新操作。
// 示例值：fe599b60-450f-46ff-b2ef-9f6675625b97
func (db *DB) Idempotent(clientToken string) (tx *DB) {
	if clientToken != "" {
		db.Statement.Idempotent = true
		db.Statement.ClientToken = clientToken
	}
	return db
}

func (db *DB) Scope(scopes ...func(*DB) *DB) (tx *DB) {
	tx = db.getInstance()
	for _, fn := range scopes {
		tx = fn(tx)
	}
	return
}

func (db *DB) Select(args ...string) (tx *DB) {
	tx = db.getInstance()
	tx.Statement.Selects = args
	return
}

func (db *DB) Order(fieldName string, desc ...bool) (tx *DB) {
	tx = db.getInstance()
	isDesc := len(desc) > 0 && desc[0]
	tx.Statement.Sort = append(tx.Statement.Sort, &larkbitable.Sort{FieldName: &fieldName, Desc: &isDesc})
	return
}

//func (db *DB) Where(fieldName string, operator string, value *[]string) (tx *DB) {
//	tx = db.getInstance()
//	if conds := tx.Statement.BuildCondition(query, args...); len(conds) > 0 {
//		tx.Statement.AddClause(clause.Where{Exprs: conds})
//	}
//	return
//}

// Where 描述：查询条件，支持使用 SQL 语法。
// Usage:
//
//	// 查询 职位 为 "初级销售员" 的记录
//	db.Where("职位 = ?", "初级销售员")
//	// 查询 name 为 "jinzhu" 且 age 不为 20 的记录
//	db.Where("name = ?", "jinzhu").Where("age <> ?", "20")
func (db *DB) Where(query string, args ...interface{}) (tx *DB) {
	log.Printf("[Where调试] 原始db.Filter条件 Conjunction=%v, Conditions长度=%d",
		db.Statement.Filter.Conjunction, len(db.Statement.Filter.Conditions))

	tx = db.getInstance()
	if tx.hasError() {
		return tx
	}

	// 确保默认是 AND 连接条件
	and := "and"
	tx.Statement.Filter.Conjunction = &and

	log.Printf("[Where调试] 设置db.Filter.Conjunction=%s", *tx.Statement.Filter.Conjunction)
	log.Printf("[Where调试] Query=%s, Args=%v", query, args)

	tx.Statement.BuildCondition(query, args...)

	log.Printf("[Where调试] 构建完条件后 tx.Statement.Filter.Conditions长度=%d",
		len(tx.Statement.Filter.Conditions))
	for i, cond := range tx.Statement.Filter.Conditions {
		if cond != nil && cond.FieldName != nil && cond.Operator != nil {
			log.Printf("[Where调试] 条件[%d]: 字段=%s, 操作符=%s, 值=%v",
				i, *cond.FieldName, *cond.Operator, cond.Value)
		}
	}

	return tx
}

// Or 描述：查询条件，支持使用 SQL 语法。
// Usage:
//
//	// 查询 职位 为 "初级销售员" 的记录
//	db.Or("职位 = ?", "初级销售员")
//	// 查询 name 为 "jinzhu" 且 age 不为 20 的记录
//	db.Or("name = ?", "jinzhu").Or("age <> ?", "20").First(&user)
func (db *DB) Or(query string, args ...interface{}) (tx *DB) {
	log.Printf("[Or调试] 原始db.Filter条件 Conjunction=%v, Conditions长度=%d",
		db.Statement.Filter.Conjunction, len(db.Statement.Filter.Conditions))

	tx = db.getInstance()
	if tx.hasError() {
		return tx
	}

	// 设置为 OR 连接条件
	or := "or"
	tx.Statement.Filter.Conjunction = &or

	log.Printf("[Or调试] 设置db.Filter.Conjunction=%s", *tx.Statement.Filter.Conjunction)
	log.Printf("[Or调试] Query=%s, Args=%v", query, args)

	tx.Statement.BuildCondition(query, args...)

	log.Printf("[Or调试] 构建完条件后 tx.Statement.Filter.Conditions长度=%d",
		len(tx.Statement.Filter.Conditions))
	for i, cond := range tx.Statement.Filter.Conditions {
		if cond != nil && cond.FieldName != nil && cond.Operator != nil {
			log.Printf("[Or调试] 条件[%d]: 字段=%s, 操作符=%s, 值=%v",
				i, *cond.FieldName, *cond.Operator, cond.Value)
		}
	}

	return tx
}

// View 指定视图ID
func (db *DB) View(viewId string) *DB {
	tx := db.getInstance()
	tx.Statement.ViewId = viewId
	return tx
}

// AutomaticFields 是否返回模板中所有字段
func (db *DB) AutomaticFields(automaticFields bool) *DB {
	tx := db.getInstance()
	tx.Statement.AutomaticFields = automaticFields
	return tx
}

// BatchGet 通过记录ID批量查询记录
// recordIds 是记录ID的字符串数组。一次最多传入1000个recordId，超出部分将会被忽略
func (db *DB) BatchGetRecords(recordIds []string) ([]*larkbitable.AppTableRecord, *DB) {
	return db.BatchGet(recordIds)
}

// Model 指定要操作的模型
// 实现DB.Model(&User{})
