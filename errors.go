package biorm

import "errors"

var (
	// ErrRecordNotFound record not found error
	ErrRecordNotFound = errors.New("record not found")

	// ErrResponseIsNil 响应为空错误
	ErrResponseIsNil = errors.New("response is nil")

	// ErrAppTokenRequired AppToken必须提供
	ErrAppTokenRequired = errors.New("appToken required")

	// ErrTableIdRequired TableId必须提供
	ErrTableIdRequired = errors.New("tableId required")

	// ErrRecordIdRequired RecordId必须提供
	ErrRecordIdRequired = errors.New("recordId required")

	// ErrParseAppTokenAndTableId 无法解析 appToken 和 tableId
	ErrParseAppTokenAndTableId = errors.New("parse appToken and tableId failed")

	// ErrObjTypeNotBitable 文档不是多维表格
	ErrObjTypeNotBitable = errors.New("document is not bitable")

	// ErrInvalidWhereParamsLength 长度不合法
	ErrInvalidWhereParamsLength = errors.New("where condition params length is invalid")
)
