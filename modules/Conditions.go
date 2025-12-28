package modules

import (
	"fmt"
	"reflect"
	"strings"
)

// ConditionType defines the type of SQL condition (e.g., IN, BETWEEN, LIKE).
type ConditionType string

const (
	ConditionIn        ConditionType = "IN"
	ConditionBetween   ConditionType = "BETWEEN"
	ConditionIsNull    ConditionType = "IS NULL"
	ConditionIsNotNull ConditionType = "IS NOT NULL"
	ConditionLike      ConditionType = "LIKE"
	ConditionGt        ConditionType = ">"
	ConditionLt        ConditionType = "<"
	ConditionGte       ConditionType = ">="
	ConditionLte       ConditionType = "<="
	ConditionNeq       ConditionType = "!="
)

// Condition represents a complex SQL condition used in WHERE clauses.
type Condition struct {
	Type   ConditionType
	Values []interface{}
}

// ToSQL generates the SQL fragment and arguments for the condition.
// It expects the column name to be already quoted if necessary.
func (c Condition) ToSQL(col string, argIndex *int) (string, []interface{}) {
	var args []interface{}
	var sql string

	switch c.Type {
	case ConditionIn:
		inArgs := []string{}
		valSlice := c.Values[0]
		rv := reflect.ValueOf(valSlice)
		if rv.Kind() == reflect.Slice {
			if rv.Len() == 0 {
				return "1=0", nil
			}
			for i := 0; i < rv.Len(); i++ {
				inArgs = append(inArgs, fmt.Sprintf("$%d", *argIndex))
				args = append(args, rv.Index(i).Interface())
				*argIndex++
			}
			sql = fmt.Sprintf("%s IN (%s)", col, strings.Join(inArgs, ", "))
		} else {
			inArgs = append(inArgs, fmt.Sprintf("$%d", *argIndex))
			args = append(args, valSlice)
			*argIndex++
			sql = fmt.Sprintf("%s IN (%s)", col, strings.Join(inArgs, ", "))
		}

	case ConditionBetween:
		sql = fmt.Sprintf("%s BETWEEN $%d AND $%d", col, *argIndex, *argIndex+1)
		args = append(args, c.Values[0], c.Values[1])
		*argIndex += 2

	case ConditionIsNull:
		sql = fmt.Sprintf("%s IS NULL", col)

	case ConditionIsNotNull:
		sql = fmt.Sprintf("%s IS NOT NULL", col)

	case ConditionLike:
		sql = fmt.Sprintf("%s ILIKE $%d", col, *argIndex)
		args = append(args, c.Values[0])
		*argIndex++

	case ConditionGt:
		sql = fmt.Sprintf("%s > $%d", col, *argIndex)
		args = append(args, c.Values[0])
		*argIndex++

	case ConditionLt:
		sql = fmt.Sprintf("%s < $%d", col, *argIndex)
		args = append(args, c.Values[0])
		*argIndex++

	case ConditionGte:
		sql = fmt.Sprintf("%s >= $%d", col, *argIndex)
		args = append(args, c.Values[0])
		*argIndex++

	case ConditionLte:
		sql = fmt.Sprintf("%s <= $%d", col, *argIndex)
		args = append(args, c.Values[0])
		*argIndex++

	case ConditionNeq:
		sql = fmt.Sprintf("%s != $%d", col, *argIndex)
		args = append(args, c.Values[0])
		*argIndex++
	}

	return sql, args
}

// In returns a Condition checking if a column's value is within a set of values.
// Usage: In([]interface{}{1, 2, 3}) or In([]int{1, 2, 3})
func In(values interface{}) Condition {
	return Condition{Type: ConditionIn, Values: []interface{}{values}}
}

// Between returns a Condition checking if a column's value is within a range (inclusive).
// Usage: Between(10, 20)
// If to is nil, it behaves like Gte(from).
// If from is nil, it behaves like Lte(to).
func Between(from, to interface{}) Condition {
	if from == nil && to == nil {
		return Condition{}
	}
	if from == nil {
		return Condition{Type: ConditionLte, Values: []interface{}{to}}
	}
	if to == nil {
		return Condition{Type: ConditionGte, Values: []interface{}{from}}
	}
	return Condition{Type: ConditionBetween, Values: []interface{}{from, to}}
}

// IsNull returns a Condition checking if a column's value is NULL.
// Usage: IsNull()
func IsNull() Condition {
	return Condition{Type: ConditionIsNull, Values: nil}
}

// IsNotNull returns a Condition checking if a column's value is NOT NULL.
// Usage: IsNotNull()
func IsNotNull() Condition {
	return Condition{Type: ConditionIsNotNull, Values: nil}
}

// Like returns a Condition for pattern matching (case-insensitive ILIKE).
// Usage: Like("%pattern%")
func Like(pattern string) Condition {
	return Condition{Type: ConditionLike, Values: []interface{}{pattern}}
}

// Gt returns a Condition checking if a column's value is greater than the target.
// Usage: Gt(10)
func Gt(value interface{}) Condition {
	return Condition{Type: ConditionGt, Values: []interface{}{value}}
}

// Lt returns a Condition checking if a column's value is less than the target.
// Usage: Lt(10)
func Lt(value interface{}) Condition {
	return Condition{Type: ConditionLt, Values: []interface{}{value}}
}

// Gte returns a Condition checking if a column's value is greater than or equal to the target.
// Usage: Gte(10)
func Gte(value interface{}) Condition {
	return Condition{Type: ConditionGte, Values: []interface{}{value}}
}

// Lte returns a Condition checking if a column's value is less than or equal to the target.
// Usage: Lte(10)
func Lte(value interface{}) Condition {
	return Condition{Type: ConditionLte, Values: []interface{}{value}}
}

// Neq returns a Condition checking if a column's value is not equal to the target.
// Usage: Neq(10)
func Neq(value interface{}) Condition {
	return Condition{Type: ConditionNeq, Values: []interface{}{value}}
}
