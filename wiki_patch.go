package biorm

import (
	"log"
	"strings"

	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
)

// SafeWiki 是Wiki方法的安全版本，避免空指针异常
func (db *DB) SafeWiki(appToken string, args ...interface{}) (tx *DB) {
	log.Printf("[SafeWiki调试] 开始处理appToken=%s", appToken)

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

	// 由于我们没有真正的Wiki API调用，这里简化处理
	// 在实际应用中，这里应该会调用API获取Wiki信息
	// 我们假设API调用成功并设置AppToken
	tx.AppToken = appToken

	log.Printf("[SafeWiki调试] 设置appToken=%s", tx.AppToken)
	return tx
}

// SafeWikiTable 是WikiTable方法的安全版本，避免空指针异常
func (db *DB) SafeWikiTable(combinedId string, args ...interface{}) (tx *DB) {
	log.Printf("[SafeWikiTable调试] 接收combinedId=%s", combinedId)

	// 解析combined ID
	parts := strings.Split(combinedId, ".")
	if len(parts) != 2 {
		db.Error = ErrParseAppTokenAndTableId
		return db
	}

	// 调用安全版本的Wiki方法和Table方法
	log.Printf("[SafeWikiTable调试] 解析成功: appToken=%s, tableId=%s", parts[0], parts[1])
	tx = db.SafeWiki(parts[0]).Table(parts[1])

	// 检查实例状态并记录日志
	if tx != nil {
		if tx.Statement.Filter.Conjunction != nil {
			log.Printf("[SafeWikiTable调试] 返回的实例: appToken=%s, tableId=%s, filter.conjunction=%s, filter.conditions长度=%d",
				tx.AppToken, tx.TableId, *tx.Statement.Filter.Conjunction, len(tx.Statement.Filter.Conditions))
		} else {
			// 初始化空的过滤条件
			and := "and"
			tx.Statement.Filter.Conjunction = &and
			tx.Statement.Filter.Conditions = make([]*larkbitable.Condition, 0)

			log.Printf("[SafeWikiTable调试] 创建空过滤条件: appToken=%s, tableId=%s, filter.conjunction=%s",
				tx.AppToken, tx.TableId, *tx.Statement.Filter.Conjunction)
		}
	}

	return tx
}
