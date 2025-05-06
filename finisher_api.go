package biorm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
)

// Records 获取记录
func (db *DB) Records() (data []*larkbitable.AppTableRecord, tx *DB) {
	tx = db.Clone()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}

	// 打印调试信息
	log.Printf("条件调试 - AppToken: %s, TableId: %s", tx.AppToken, tx.TableId)
	if tx.Statement.Filter.Conjunction != nil {
		log.Printf("条件调试 - Conjunction: %s", *tx.Statement.Filter.Conjunction)
	} else {
		log.Printf("条件调试 - Conjunction: nil")
	}
	for i, cond := range tx.Statement.Filter.Conditions {
		if cond != nil && cond.FieldName != nil && cond.Operator != nil {
			log.Printf("条件调试 - 条件[%d]: 字段=%s, 操作符=%s, 值=%v",
				i, *cond.FieldName, *cond.Operator, cond.Value)
		}
	}

	var pageToken string
	for {
		if pageToken != "" {
			time.Sleep(tx.Config.RequestInterval)
		}

		//bodyBuilder := larkbitable.NewSearchAppTableRecordReqBodyBuilder().
		//	AutomaticFields(tx.Statement.AutomaticFields)

		body := make(map[string]interface{})
		if tx.Statement.ViewId != "" {
			//bodyBuilder.ViewId(tx.Statement.ViewId)
			body["view_id"] = tx.Statement.ViewId
		}
		if tx.Statement.Selects != nil && len(tx.Statement.Selects) > 0 {
			//	bodyBuilder.FieldNames(tx.Statement.Selects)
			body["field_names"] = tx.Statement.Selects
		}
		if tx.Statement.Sort != nil && len(tx.Statement.Sort) > 0 {
			//	bodyBuilder.Sort(tx.Statement.Sort)
			body["sort"] = tx.Statement.Sort
		}
		if tx.Statement.Filter.Conjunction != nil && len(tx.Statement.Filter.Conditions) > 0 {
			//	bodyBuilder.Filter(larkbitable.NewFilterInfoBuilder().
			//		Conjunction(*tx.Statement.Filter.Conjunction).
			//		Conditions(tx.Statement.Filter.Conditions).
			//		Build(),
			//	)
			conditions := make([]map[string]interface{}, 0, len(tx.Statement.Filter.Conditions))
			for i, condition := range tx.Statement.Filter.Conditions {
				if condition == nil || condition.FieldName == nil || condition.Operator == nil {
					log.Printf("条件调试 - 条件[%d]是无效的，跳过", i)
					continue
				}

				condMap := map[string]interface{}{
					"field_name": *condition.FieldName,
					"operator":   *condition.Operator,
					"value":      condition.Value,
				}
				conditions = append(conditions, condMap)
				log.Printf("条件调试 - 添加条件到请求: field_name=%s, operator=%s, value=%v",
					*condition.FieldName, *condition.Operator, condition.Value)
			}

			if len(conditions) > 0 {
				body["filter"] = map[string]interface{}{
					"conjunction": *tx.Statement.Filter.Conjunction,
					"conditions":  conditions,
				}
				log.Printf("条件调试 - 设置filter: conjunction=%s, conditions数量=%d",
					*tx.Statement.Filter.Conjunction, len(conditions))

				// 打印整个filter内容
				filterJSON, _ := json.Marshal(body["filter"])
				log.Printf("条件调试 - 完整filter: %s", string(filterJSON))
			} else {
				log.Printf("条件调试 - 没有有效条件，不设置filter")
			}
		} else {
			log.Printf("条件调试 - Filter条件为空，Conjunction=%v, Conditions长度=%d",
				tx.Statement.Filter.Conjunction, len(tx.Statement.Filter.Conditions))
		}
		body["automatic_fields"] = tx.Statement.AutomaticFields

		//req := larkbitable.NewSearchAppTableRecordReqBuilder().
		//	AppToken(tx.AppToken).TableId(tx.TableId).
		//	UserIdType(tx.Statement.UserIdType).
		//	PageToken(pageToken).
		//	PageSize(500). // 分页大小。最大值为 500
		//	Body(bodyBuilder.Build()).
		//	Build()

		// 发起请求
		apiReq := larkcore.ApiReq{
			HttpMethod: http.MethodPost,
			ApiPath:    "https://open.feishu.cn/open-apis/bitable/v1/apps/:app_token/tables/:table_id/records/search",
			Body:       body,
			QueryParams: larkcore.QueryParams{
				"user_id_type": []string{tx.Statement.UserIdType},
				"page_token":   []string{pageToken},
				"page_size":    []string{"500"},
			},
			PathParams: larkcore.PathParams{
				"app_token": tx.AppToken,
				"table_id":  tx.TableId,
			},
			SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeTenant},
		}

		resp, err := tx.cli.Do(context.Background(), &apiReq)
		// 处理错误
		if err != nil {
			tx.Error = err
			if resp != nil {
				tx.ApiResp = resp
			}
			return
		}
		if resp == nil {
			tx.Error = ErrResponseIsNil
			return
		}

		if resp.RawBody == nil {
			tx.Error = fmt.Errorf("response body is nil: %w", ErrResponseIsNil)
			return
		}

		var response larkbitable.SearchAppTableRecordResp
		err = json.Unmarshal(resp.RawBody, &response)
		if err != nil {
			tx.Error = fmt.Errorf("json unmarshal response body failed: %w", err)
			return
		}
		if response.Data == nil {
			tx.Error = fmt.Errorf("response data is nil: %w", ErrResponseIsNil)
			tx.ApiResp = resp
			return
		}

		for _, record := range response.Data.Items {
			data = append(data, record)
		}

		if !*response.Data.HasMore {
			break
		}
		pageToken = *response.Data.PageToken
	}

	// 清理无需保留的资源，帮助垃圾回收
	tx.Finalize()

	return
}

// BatchGet 通过记录ID批量查询记录
// recordIds 是记录ID的字符串数组。一次最多传入100个recordId，超出部分将会被忽略
func (db *DB) BatchGet(recordIds []string) (data []*larkbitable.AppTableRecord, tx *DB) {
	tx = db.Clone()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}
	if len(recordIds) == 0 {
		tx.Error = errors.New("recordIds cannot be empty")
		return
	}

	// 记录ID数量限制
	if len(recordIds) > 100 {
		log.Printf("警告: 批量查询的记录ID超过100个，将截取前100个")
		recordIds = recordIds[:100]
	}

	// 构建请求体
	body := map[string]interface{}{
		"record_ids":       recordIds,
		"user_id_type":     tx.Statement.UserIdType,
		"automatic_fields": tx.Statement.AutomaticFields,
	}

	// 添加可选字段参数
	if tx.Statement.Selects != nil && len(tx.Statement.Selects) > 0 {
		body["field_names"] = tx.Statement.Selects
	}

	// 添加视图参数
	if tx.Statement.ViewId != "" {
		body["view_id"] = tx.Statement.ViewId
	}

	// 构建请求
	req := larkbitable.NewBatchGetAppTableRecordReqBuilder().
		AppToken(tx.AppToken).
		TableId(tx.TableId).
		Body(larkbitable.NewBatchGetAppTableRecordReqBodyBuilder().
			RecordIds(recordIds).
			UserIdType(tx.Statement.UserIdType).
			// WithSharedUrl(tx.Statement.WithSharedUrl).
			AutomaticFields(tx.Statement.AutomaticFields).
			Build()).
		Build()

	// 发起请求
	resp, err := tx.cli.Bitable.V1.AppTableRecord.BatchGet(context.Background(), req)

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

	if resp.RawBody == nil {
		tx.Error = fmt.Errorf("response body is nil: %w", ErrResponseIsNil)
		return
	}

	var response larkbitable.BatchGetAppTableRecordResp
	err = json.Unmarshal(resp.RawBody, &response)
	if err != nil {
		tx.Error = fmt.Errorf("json unmarshal response body failed: %w", err)
		return
	}
	if response.Data == nil {
		tx.Error = fmt.Errorf("response data is nil: %w", ErrResponseIsNil)
		tx.ApiResp = resp.ApiResp
		tx.CodeError = &resp.CodeError
		return
	}

	data = response.Data.Records

	// 清理无需保留的资源，帮助垃圾回收
	tx.Finalize()

	return
}

// Create inserts record, returning the inserted data's primary key in value's id
func (db *DB) Create(records ...map[string]interface{}) (data []*larkbitable.AppTableRecord, tx *DB) {
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}

	if len(records) == 0 {
		return
	} else if len(records) == 1 {
		var datum *larkbitable.AppTableRecord
		datum, tx = tx.createSingle(records[0])
		return []*larkbitable.AppTableRecord{datum}, tx
	} else {
		return tx.createInBatch(records)
	}
}

// Update
func (db *DB) Update(recordId string, fields map[string]interface{}) (data *larkbitable.UpdateAppTableRecordRespData, tx *DB) {
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}
	if recordId == "" {
		tx.Error = ErrRecordIdRequired
		return
	}

	req := larkbitable.NewUpdateAppTableRecordReqBuilder().
		AppToken(tx.AppToken).TableId(tx.TableId).RecordId(recordId).
		UserIdType(tx.Statement.UserIdType).
		AppTableRecord(larkbitable.NewAppTableRecordBuilder().
			Fields(fields).
			Build()).
		Build()

	// 发起请求
	resp, err := tx.cli.Bitable.V1.AppTableRecord.Update(context.Background(), req)

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

	return resp.Data, tx
}

// Delete
func (db *DB) Delete(recordId string) (data *larkbitable.DeleteAppTableRecordRespData, tx *DB) {
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}
	if recordId == "" {
		tx.Error = ErrRecordIdRequired
		return
	}

	req := larkbitable.NewDeleteAppTableRecordReqBuilder().
		AppToken(tx.AppToken).TableId(tx.TableId).RecordId(recordId).
		Build()

	// 发起请求
	resp, err := tx.cli.Bitable.V1.AppTableRecord.Delete(context.Background(), req)

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

	return resp.Data, tx
}

func (db *DB) Meta() (data *larkbitable.GetAppRespData, tx *DB) {
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}

	// 创建请求对象
	req := larkbitable.NewGetAppReqBuilder().AppToken(tx.AppToken).Build()

	// 发起请求
	resp, err := tx.cli.Bitable.V1.App.Get(context.Background(), req)

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

	return resp.Data, tx
}

// Create inserts record, returning the inserted data's primary key in value's id
func (db *DB) createSingle(fields map[string]interface{}) (data *larkbitable.AppTableRecord, tx *DB) {
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}

	req := larkbitable.NewCreateAppTableRecordReqBuilder().
		AppToken(tx.AppToken).TableId(tx.TableId).
		AppTableRecord(larkbitable.NewAppTableRecordBuilder().
			Fields(fields).
			Build()).
		Build()

	// 发起请求
	resp, err := tx.cli.Bitable.V1.AppTableRecord.Create(context.Background(), req)

	// 处理错误
	if err != nil {
		tx.Error = err
		if resp != nil {
			tx.ApiResp = resp.ApiResp
			tx.CodeError = &resp.CodeError
		}
		return
	}
	if resp == nil || resp.Data == nil {
		tx.Error = ErrResponseIsNil
		return
	}

	return resp.Data.Record, tx
}

func (db *DB) createInBatch(records []map[string]interface{}) (data []*larkbitable.AppTableRecord, tx *DB) {
	tx = db.getInstance()
	if tx.hasError() {
		return
	}

	if tx.AppToken == "" {
		tx.Error = ErrAppTokenRequired
		return
	}
	if tx.TableId == "" {
		tx.Error = ErrTableIdRequired
		return
	}

	// 创建请求对象
	list := make([]*larkbitable.AppTableRecord, 0, len(records))
	for _, r := range records {
		list = append(list, larkbitable.NewAppTableRecordBuilder().Fields(r).Build())
	}
	req := larkbitable.NewBatchCreateAppTableRecordReqBuilder().
		AppToken(tx.AppToken).TableId(tx.TableId).
		UserIdType(tx.Statement.UserIdType).
		ClientToken(tx.Statement.ClientToken).
		Body(larkbitable.NewBatchCreateAppTableRecordReqBodyBuilder().
			Records(list).
			Build()).
		Build()

	// 发起请求
	resp, err := tx.cli.Bitable.V1.AppTableRecord.BatchCreate(context.Background(), req)

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

	// 业务处理
	if resp.Data == nil {
		tx.Error = ErrResponseIsNil
		return
	}
	return resp.Data.Records, tx
}
