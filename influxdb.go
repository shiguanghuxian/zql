package zql

import (
	"errors"
	"fmt"
	"strings"
)

// GetInfluxdbQuery 获得转换后的查询语句
func (zql *Zql) GetInfluxdbQuery(suffix string) (string, error) {
	if zql.Select == "" || zql.From == "" {
		return "", errors.New("Query string does not exist 'select|from'")
	}
	query := ""
	if suffix != "" {
		// 替换avg平均值函数 MEDIAN
		zql.Select = strings.Replace(zql.Select, "avg(", "MEDIAN(", 1)
		query = "SELECT " + zql.Select + fmt.Sprintf(" FROM \"%s%s%s\"", zql.Prefix, zql.From, suffix)
	} else {
		query = "SELECT " + zql.Select + fmt.Sprintf(" FROM \"%s%s\"", zql.Prefix, zql.From)
	}
	// where
	if zql.Where != "" {
		query += " WHERE " + InfluxdbWhereLike(zql.Where)
	}
	// group by
	if zql.GroupBy != "" {
		query += " GROUP BY " + zql.GroupBy
	}
	// order by
	if zql.OrderBy != "" {
		query += " ORDER BY " + zql.OrderBy
	}
	// limit
	if zql.Limit != "" {
		// 是否有offset情况
		limits := strings.Split(zql.Limit, ",")
		if len(limits) == 1 {
			query += " LIMIT " + zql.Limit
		} else if len(limits) == 2 {
			query += fmt.Sprintf(" LIMIT %s OFFSET %s ", limits[1], limits[0])
		} else {
			return query, errors.New("limit keyword error")
		}
	}
	return query, nil
}

// 处理like 正则
func InfluxdbWhereLike(str string) string {
	key := strings.Index(str, " like ")
	if key == -1 {
		return str
	}
	//	fmt.Println(key)
	key1 := strings.Index(strings.TrimSpace(str[key+6:])[1:], "'")
	//	fmt.Println(key1)
	// like val 去空格后
	likeVal := strings.TrimSpace(str[key+6 : key+key1+8])
	// 替除 val
	str = strings.Replace(str, likeVal, strings.Trim(likeVal, "'"), 1)
	// 替换第一个like
	str = strings.Replace(str, " like ", " =~ ", 1)

	return InfluxdbWhereLike(str)
}
