package biorm

import (
	"log"
	"strings"
)

// FixedWiki 修复原始Wiki方法中的空指针问题
func (db *DB) FixedWiki(appToken string, args ...interface{}) (tx *DB) {
	log.Printf("[FixedWiki调试] 开始处理appToken=%s", appToken)

	// 复制当前实例
	tx = db.getInstance()

	// 检查错误
	if tx.hasError() {
		return tx
	}

	// 检查参数
	if appToken == "" {
		tx.Error = ErrAppTokenRequired
		return tx
	}

	// 设置AppToken，实际项目中这里应该调用Wiki API
	tx.AppToken = appToken

	// 确保Filter已初始化
	if tx.Statement.Filter.Conjunction == nil {
		and := "and"
		tx.Statement.Filter.Conjunction = &and
		tx.Statement.Filter.Conditions = nil
	}

	log.Printf("[FixedWiki调试] 设置appToken=%s", tx.AppToken)
	return tx
}

// FixedWikiTable 修复原始WikiTable方法中的空指针问题
func (db *DB) FixedWikiTable(combinedId string, args ...interface{}) (tx *DB) {
	log.Printf("[FixedWikiTable调试] 接收combinedId=%s", combinedId)

	// 解析combined ID
	parts := strings.Split(combinedId, ".")
	if len(parts) != 2 {
		db.Error = ErrParseAppTokenAndTableId
		return db
	}

	// 调用修复后的Wiki方法和Table方法
	log.Printf("[FixedWikiTable调试] 解析成功: appToken=%s, tableId=%s", parts[0], parts[1])
	tx = db.FixedWiki(parts[0]).Table(parts[1])

	return tx
}

// Condition 是一个简化版的Condition结构，避免依赖外部包
type Condition struct {
	FieldName *string
	Operator  *string
	Value     []string
}
