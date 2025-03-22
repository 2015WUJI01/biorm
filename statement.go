package biorm

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	larkbitable "github.com/larksuite/oapi-sdk-go/v3/service/bitable/v1"
)

// Statement 存储查询操作的各种参数和状态
type Statement struct {
	*DB

	Context context.Context

	AppToken string
	TableId  string
	Selects  []string // selected columns
	Filter   larkbitable.FilterInfo
	Sort     []*larkbitable.Sort
	scopes   []func(*DB) *DB

	// 常用的查询类型
	UserIdType string

	// 特殊场景使用的字段
	AutomaticFields bool // 是否返回自动计算的字段。默认为 false，表示不返回。

	Idempotent  bool   // 是否幂等
	ClientToken string // 幂等 uuid

	// 考虑需要
	Dest interface{}
}

// BuildCondition 根据查询参数构建过滤条件
func (stmt *Statement) BuildCondition(query interface{}, args ...interface{}) {
	if stmt.Filter.Conditions == nil {
		stmt.Filter.Conditions = make([]*larkbitable.Condition, 0)
	}

	if s, ok := query.(string); ok {
		// if it is a number, then treats it as primary key
		if s == "" && len(args) == 0 {
			return
		}

		if len(args) == 0 {
			//re := regexp.MustCompile(`(?m)(?P<field>[^ ]+)\s+(?P<operator>isEmpty|isNotEmpty|isGreater|isGreaterEqual|isLess|isLessEqual|like|in|=|!=|>|<|>=|<=|<>|is null|is empty|is not null|is not empty|is|isNot|contains|doesNotContain|)\s*`)
			re := regexp.MustCompile(`(?m)(?P<field>[^ ]+)\s+(?P<operator>isEmpty|isNotEmpty|is null|is not null|is empty|is not empty|isGreaterEqual|isLessEqual|isGreater|isLess|isNot|contains|doesNotContain|=|!=|>|<|>=|<=|<>|is|like|in)\s*`)
			var match = re.FindStringSubmatch(s)
			if len(match) == 0 {
				stmt.Error = ErrInvalidWhereParamsLength
				return
			}
			field := match[re.SubexpIndex("field")]
			op := match[re.SubexpIndex("operator")]

			switch op {
			case "is", "=":
				op = "is"
			case "isNot", "!=", "<>": // 不等于（不支持日期字段，了解如何查询日期字段，参考日期字段填写说明）
				op = "isNot"
			case "contains": // 包含（不支持日期字段）
				op = "contains"
			case "doesNotContain": // 不包含（不支持日期字段）
				op = "doesNotContain"
			case "isEmpty", "is empty", "is null": // 为空
				op = "isEmpty"
			case "isNotEmpty", "is not empty", "is not null": // 不为空
				op = "isNotEmpty"
			case "isGreater", ">":
				op = "isGreater"
			case "isGreaterEqual", ">=":
				op = "isGreaterEqual"
			case "isLess", "<":
				op = "isLess"
			case "isLessEqual", "<=":
				op = "isLessEqual"
			case "like":
				op = "like"
			case "in":
				op = "in"
			default:
				stmt.Error = fmt.Errorf("无法解析的 where condition operation: %s", op)
			}
			cond := &larkbitable.Condition{FieldName: &field, Operator: &op, Value: make([]string, 0)}
			switch op {
			case "is", "isNot", "contains", "doesNotContain", "isEmpty", "isNotEmpty", "isGreater", "isGreaterEqual", "isLess", "isLessEqual", "like", "in":
			}
			stmt.Filter.Conditions = append(stmt.Filter.Conditions, cond)
			return
		}

		// 支持 ? 方式传参
		if len(args) > 0 && strings.Contains(s, "?") {
			// 问号需要和参数数量一致
			if strings.Count(s, "?") != len(args) {
				stmt.Error = ErrInvalidWhereParamsLength
				return
			}

			if strings.Count(s, "?") == 1 {
				// 单个问号，直接替换

				// 正则匹配：
				// 要求满足以下 case 都能提取相同的结果 ["年龄", "=", "?"]:
				// "年龄=?"
				// "年龄 = ?"
				// "年龄 =?"
				// "年龄= ?"
				// "年龄  = ?"
				// " 年龄=? "
				// 要求满足以下 case 都能提取相同的结果 ["linkin", "=", "?"]:
				// "linkin=?"
				// 中间的操作符可能满足如下情况
				re := regexp.MustCompile(`(?m)(?P<field>[^ ]+)\s+(?P<operator>=|!=|>|<|>=|<=|<>|is null|is empty|is not null|is not empty|is|isNot|contains|doesNotContain|isEmpty|isNotEmpty|isGreater|isGreaterEqual|isLess|isLessEqual|like|in)\s+\?`)
				var match = re.FindStringSubmatch(s)
				if len(match) == 0 {
					stmt.Error = ErrInvalidWhereParamsLength
					return
				}
				field := match[re.SubexpIndex("field")]
				op := match[re.SubexpIndex("operator")]
				value := args[0]

				switch op {
				case "is", "=":
					op = "is"
				case "isNot", "!=", "<>": // 不等于（不支持日期字段，了解如何查询日期字段，参考日期字段填写说明）
					op = "isNot"
				case "contains": // 包含（不支持日期字段）
					op = "contains"
				case "doesNotContain": // 不包含（不支持日期字段）
					op = "doesNotContain"
				case "isEmpty", "is empty", "is null": // 为空
					op = "isEmpty"
				case "isNotEmpty", "is not empty", "is not null": // 不为空
					op = "isNotEmpty"
				case "isGreater", ">":
					op = "isGreater"
				case "isGreaterEqual", ">=":
					op = "isGreaterEqual"
				case "isLess", "<":
					op = "isLess"
				case "isLessEqual", "<=":
					op = "isLessEqual"
				case "like":
					op = "like"
				case "in":
					op = "in"
				default:
					stmt.Error = fmt.Errorf("无法解析的 where condition operation: %s", op)
				}
				cond := &larkbitable.Condition{FieldName: &field, Operator: &op, Value: []string{}}
				if value != nil {
					switch value.(type) {
					case string:
						cond.Value = []string{value.(string)}
					case []string:
						cond.Value = value.([]string)
					case []byte:
						cond.Value = []string{string(value.([]byte))}
					case int:
						cond.Value = []string{fmt.Sprintf("%d", value.(int))}
					case int8:
						cond.Value = []string{fmt.Sprintf("%d", value.(int8))}
					case int16:
						cond.Value = []string{fmt.Sprintf("%d", value.(int16))}
					case int32:
						cond.Value = []string{fmt.Sprintf("%d", value.(int32))}
					case int64:
						cond.Value = []string{fmt.Sprintf("%d", value.(int64))}
					case uint:
						cond.Value = []string{fmt.Sprintf("%d", value.(uint))}
					case uint8:
						cond.Value = []string{fmt.Sprintf("%d", value.(uint8))}
					case uint16:
						cond.Value = []string{fmt.Sprintf("%d", value.(uint16))}
					case uint32:
						cond.Value = []string{fmt.Sprintf("%d", value.(uint32))}
					case uint64:
						cond.Value = []string{fmt.Sprintf("%d", value.(uint64))}
					case float32:
						cond.Value = []string{fmt.Sprintf("%f", value.(float32))}
					case float64:
						cond.Value = []string{fmt.Sprintf("%f", value.(float64))}
					case bool:
						cond.Value = []string{fmt.Sprintf("%t", value.(bool))}
					case time.Time:
						// 日期筛选时，operator 仅支持 is、isEmpty、isNotEmpty、isGreater、isLess 五个值。
						if op == "isEmpty" || op == "isNotEmpty" {
							// 当 operator 为 isEmpty或isNotEmpty 时，value 需填空值 "value":[]。
							cond.Value = []string{}
						} else if op == "is" || op == "isGreater" || op == "isLess" {
							// 当 operator 为 is、isGreater 或 isLess 时，参考下表填写日期字段。
							// 第二个元素虽然是时间戳，但是实际筛选时，会被转为文档时区当天的零点。
							// 对于公式日期字段，第二个元素需要填写 yyyy/MM/dd 格式的日期文本，例如 2025/01/07
							cond.Value = []string{"ExactDate", fmt.Sprintf("%d", value.(time.Time).UnixMilli())}
						} else {
							stmt.Error = fmt.Errorf("无法解析的 where condition operation: %s", op)
						}
					case map[string]interface{}:
						if jsonValue, err := json.Marshal(value.(map[string]interface{})); err == nil {
							cond.Value = []string{string(jsonValue)}
						} else {
							cond.Value = []string{fmt.Sprintf("%v", value)}
						}
					case struct{}:
						if jsonValue, err := json.Marshal(value); err == nil {
							cond.Value = []string{string(jsonValue)}
						} else {
							cond.Value = []string{fmt.Sprintf("%v", value)}
						}
					default:
						cond.Value = []string{fmt.Sprintf("%v", value)}
					}
				}
				stmt.Filter.Conditions = append(stmt.Filter.Conditions, cond)
				return
			} else {
				stmt.Error = fmt.Errorf("查询 query 暂不支持多个 ? 符号：%s", query)
				return
			}
		}

	}
	stmt.Error = fmt.Errorf("暂不支持的 query 类型：%s", query)
	return
}
