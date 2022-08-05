package xbuilder_ozzo_dbx

import (
	"fmt"
	dbx "github.com/go-ozzo/ozzo-dbx"
	"github.com/go-xbuilder/config"
	"github.com/go-xbuilder/config/xbuilder"
	"net/url"
	"strings"
	"time"
)

func cleanParams(params url.Values) url.Values {
	for k, values := range params {
		cleanV := make([]string, 0)
		for _, value := range values {
			s := strings.TrimSpace(value)
			if s != "" {
				cleanV = append(cleanV, s)
			}
		}
		if len(cleanV) == 0 {
			delete(params, k)
		} else {
			params[k] = cleanV
		}
	}
	return params
}

type XBuilder struct {
	config config.Config
}

func NewBuilder(config config.Config) *XBuilder {
	return &XBuilder{
		config: config,
	}
}

func (xb XBuilder) Expressions(table string, params url.Values, operation string) dbx.Expression {
	var w dbx.Expression
	params = cleanParams(params)
	if len(params) == 0 {
		return w
	}

	where := xb.config.Resource(table).Where
	if len(where) == 0 {
		return w
	}

	expressions := make([]dbx.Expression, 0)
	// string to interface, used for `IN`, `NOT IN` expression
	stringToInterfaceFunc := func(values []string) []interface{} {
		iValues := make([]interface{}, 0)
		for _, v := range values {
			for _, s := range strings.Split(v, ",") {
				s = strings.TrimSpace(s)
				if s != "" {
					iValues = append(iValues, s)
				}
			}
		}
		return iValues
	}

	for k, values := range params {
		singleValue := len(values) == 1
		if op, ok := where[k]; ok {
			k = fmt.Sprintf("%s.%s", table, k) // table.field
			var exp dbx.Expression
			switch op {
			case xbuilder.Eq:
				likeValues := make([]string, 0)
				for _, v := range values {
					// 支持 fieldName = '', fieldName IS NULL
					if strings.EqualFold(v, xb.config.Alias.EmptyValue) {
						expressions = append(expressions, dbx.HashExp{k: ""})
					} else if strings.EqualFold(v, xb.config.Alias.NotEmptyValue) {
						expressions = append(expressions, dbx.NewExp(k+" != ''"))
					} else if strings.EqualFold(v, xb.config.Alias.NullValue) {
						expressions = append(expressions, dbx.HashExp{k: nil})
					} else if strings.EqualFold(v, xb.config.Alias.NotNullValue) {
						expressions = append(expressions, dbx.NewExp(k+" IS NOT NULL"))
					} else {
						likeValues = append(likeValues, v)
					}
				}
				if len(likeValues) > 0 {
					exp = dbx.Like(k, likeValues...)
				}
			case xbuilder.NotLike:
				exp = dbx.NotLike(k, values...)
			case xbuilder.In, xbuilder.NotIn:
				inValues := stringToInterfaceFunc(values)
				if op == xbuilder.In {
					exp = dbx.In(k, inValues...)
				} else {
					exp = dbx.NotIn(k, inValues...)
				}
			case xbuilder.Neq:
				if singleValue {
					exp = dbx.Not(dbx.HashExp{k: values[0]})
				} else {
					exp = dbx.NotIn(k, stringToInterfaceFunc(values)...)
				}
			case xbuilder.Between, xbuilder.NotBetween:
				for _, s := range values {
					if index := strings.Index(s, "~"); index != -1 {
						fromValueIsDate := false
						toValueIsDate := false
						from := strings.TrimSpace(s[0:index])
						to := strings.TrimSpace(s[index+1:])
						layout := "2006-01-02"
						if _, e := time.Parse(layout, from); e == nil {
							fromValueIsDate = true
							from += " 00:00:00"
						}
						if _, e := time.Parse(layout, to); e == nil {
							toValueIsDate = true
							to += " 23:59:59"
						}
						if fromValueIsDate || toValueIsDate {
							if from == "" {
								from = to + " 00:00:00"
							}
							if to == "" {
								from = from + " 23:59:59"
							}
						}
						if from != "" && to != "" {
							if op == xbuilder.Between {
								exp = dbx.Between(k, from, to)
							} else {
								exp = dbx.NotBetween(k, from, to)
							}
						} else if from == "" {
							exp = dbx.HashExp{k: to}
						} else if to == "" {
							exp = dbx.HashExp{k: from}
						}
					} else {
						exp = dbx.HashExp{k: s}
					}
					expressions = append(expressions, exp)
				}
				exp = nil
			default:
				// EQ
				if singleValue {
					// 支持 fieldName = '', fieldName IS NULL
					if strings.EqualFold(values[0], xb.config.Alias.EmptyValue) {
						exp = dbx.HashExp{k: ""}
					} else if strings.EqualFold(values[0], xb.config.Alias.NotEmptyValue) {
						exp = dbx.NewExp(k + " != ''")
					} else if strings.EqualFold(values[0], xb.config.Alias.NullValue) {
						exp = dbx.HashExp{k: nil}
					} else if strings.EqualFold(values[0], xb.config.Alias.NotNullValue) {
						exp = dbx.NewExp(k + " IS NOT NULL")
					} else {
						exp = dbx.HashExp{k: values[0]}
					}
				} else {
					exp = dbx.In(k, stringToInterfaceFunc(values)...)
				}
			}
			if exp != nil {
				expressions = append(expressions, exp)
			}
		}
	}

	if len(expressions) > 0 {
		if operation == xbuilder.AND {
			w = dbx.And(expressions...)
		} else if operation == xbuilder.OR {
			w = dbx.Or(expressions...)
		}
	}

	return w
}

// OrderBy 生成排序参数数据
func (xb XBuilder) OrderBy(table string, params url.Values, defaultOrderBy ...string) []string {
	cols := make([]string, 0)
	params = cleanParams(params)
	if len(params) == 0 && len(defaultOrderBy) == 0 {
		return cols
	}

	orderByFields := xb.config.Resource(table).OrderFields
	if len(orderByFields) == 0 {
		if len(defaultOrderBy) != 0 {
			cols = defaultOrderBy
		}
		return cols
	}

	orders := make(map[string]string, 0)
	for _, s := range strings.Split(params.Get("orderBy"), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		fieldName := s
		idx := strings.Index(s, ".")
		if idx != -1 {
			fieldName = s[0:idx]
		}
		if !orderByFields.In(fieldName) {
			continue
		}

		direction := "ASC"
		if idx != -1 {
			if strings.ToLower(s[idx+1:]) == "desc" {
				direction = "DESC"
			}
		}

		if _, exists := orders[fieldName]; !exists {
			orders[fieldName] = direction
		}
	}

	if len(orders) == 0 {
		return defaultOrderBy
	}
	for fieldName, direction := range orders {
		cols = append(cols, fmt.Sprintf("%s.%s %s", table, fieldName, direction))
	}
	return cols
}
