package biorm

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
)

var _client *lark.Client

var _baseAppToken = os.Getenv("BASE_APP_TOKEN")
var _wikiAppToken = os.Getenv("WIKI_APP_TOKEN")
var _tableId = os.Getenv("TABLE_ID")
var _recordId = os.Getenv("RECORD_ID")

func TestMain(m *testing.M) {
	_client = lark.NewClient(
		os.Getenv("CLIENT_APP_ID"), os.Getenv("CLIENT_APP_SECRET"),
		// 可选配置
		// lark.WithLogLevel(larkcore.LogLevelDebug),
		// lark.WithLogReqAtDebug(true),
	)
	m.Run()
}

// 示例代码
func TestExample(t *testing.T) {
	// 创建biorm实例
	db := NewDB(_client) // 使用_避免未使用变量的警告

	// 示例1：获取多维表格元数据
	fmt.Println("=== 示例1：获取多维表格元数据 ===")
	// meta, tx := db.Base(_baseAppToken).Meta()
	meta, tx := db.Wiki(_wikiAppToken).Meta()
	if tx.Error != nil {
		log.Println("获取多维表格元数据失败：", tx.ErrorString())
	} else {
		log.Println("多维表格名称:", *meta.App.Name)
	}

	// 示例2：创建记录
	fmt.Println("\n=== 示例2：创建记录 ===")
	// 也可以使用 db.WikiTable(_wikiAppToken + `.` + _tableId)
	createdRecords, tx := db.Wiki(_wikiAppToken).Table(_tableId).Create(
		map[string]interface{}{
			"uuid": fmt.Sprintf("%d-1", time.Now().UnixMilli()),
			"文本字段": "测试文本 1",
		}, map[string]interface{}{
			"uuid": fmt.Sprintf("%d-2", time.Now().UnixMilli()),
			"更新时间": time.Now().UnixMilli(),
		},
	)
	if tx.Error != nil {
		log.Println("创建记录失败：", tx.ErrorString())
	} else {
		log.Println("创建成功：", createdRecords)
	}

	// 示例3：条件查询
	fmt.Println("\n=== 示例3：条件查询 ===")
	records, tx := db.Wiki(_wikiAppToken).Table(_tableId).
		Select("uuid", "文本字段", "更新时间").
		Where("更新时间 = ?", "Today").
		Order("创建时间", true). // 降序排列
		Records()
	if tx.Error != nil {
		log.Println("查询记录失败：", tx.ErrorString())
	} else {
		log.Printf("查询到%d条记录\n", len(records))
	}

	fmt.Println("\n示例代码执行完成，请修改代码中的token和配置后再运行")
}
