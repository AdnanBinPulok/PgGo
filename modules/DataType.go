package modules

import (
	"fmt"
	"strings"
)

// ColumnDef represents a column definition with its data type and constraints.
// It supports a fluent API for chaining constraints like NotNull(), Unique(), etc.
type ColumnDef struct {
	Type         string
	Length       *int
	Precision    *int
	Scale        *int
	isNotNull    bool
	isUnique     bool
	isPrimaryKey bool
	Default      *string
	Check        *string // CHECK constraint like exam
}

// String returns the complete SQL representation of the column definition,
// including the data type, length/precision, and all constraints.
func (cd *ColumnDef) String() string {
	var parts []string

	// Add the base type
	if cd.Length != nil {
		parts = append(parts, fmt.Sprintf("%s(%d)", cd.Type, *cd.Length))
	} else if cd.Precision != nil && cd.Scale != nil {
		parts = append(parts, fmt.Sprintf("%s(%d,%d)", cd.Type, *cd.Precision, *cd.Scale))
	} else if cd.Precision != nil {
		parts = append(parts, fmt.Sprintf("%s(%d)", cd.Type, *cd.Precision))
	} else {
		parts = append(parts, cd.Type)
	}

	// Add constraints
	if cd.isNotNull {
		parts = append(parts, "NOT NULL")
	}
	if cd.isUnique {
		parts = append(parts, "UNIQUE")
	}
	if cd.isPrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}
	if cd.Default != nil {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", *cd.Default))
	}
	if cd.Check != nil {
		parts = append(parts, fmt.Sprintf("CHECK (%s)", *cd.Check))
	}

	return strings.Join(parts, " ")
}

// NotNull adds the NOT NULL constraint to the column.
func (cd *ColumnDef) NotNull() *ColumnDef {
	cd.isNotNull = true
	return cd
}

// Unique adds the UNIQUE constraint to the column.
func (cd *ColumnDef) Unique() *ColumnDef {
	cd.isUnique = true
	return cd
}

// PrimaryKey adds the PRIMARY KEY constraint to the column.
func (cd *ColumnDef) PrimaryKey() *ColumnDef {
	cd.isPrimaryKey = true
	return cd
}

// DefaultValue sets the default value for the column.
// It supports various types:
// - string: Auto-quoted if it's a text type and not a keyword (NULL, CURRENT_TIMESTAMP) or function call.
// - bool, int, float, etc.: Converted to string representation.
// Examples:
//
//	.DefaultValue("user") -> DEFAULT 'user'
//	.DefaultValue(true)   -> DEFAULT true
//	.DefaultValue("CURRENT_TIMESTAMP") -> DEFAULT CURRENT_TIMESTAMP
func (cd *ColumnDef) DefaultValue(value interface{}) *ColumnDef {
	var strVal string
	if v, ok := value.(string); ok {
		strVal = v
		// Auto-quote string values if they are not already quoted, not NULL, and not function calls
		isQuotedType := false
		switch cd.Type {
		case "text", "varchar", "char", "json", "jsonb", "uuid", "date", "time", "timestamp", "timestamptz", "interval", "inet", "cidr", "macaddr":
			isQuotedType = true
		}

		if isQuotedType {
			upperVal := strings.ToUpper(strVal)
			if !strings.HasPrefix(strVal, "'") && upperVal != "NULL" && !strings.Contains(strVal, "(") && upperVal != "CURRENT_TIMESTAMP" {
				strVal = fmt.Sprintf("'%s'", strVal)
			}
		}
	} else {
		strVal = fmt.Sprintf("%v", value)
	}
	cd.Default = &strVal
	return cd
}

func (cd *ColumnDef) CheckConstraint(constraint string) *ColumnDef {
	// Set the CHECK constraint
	cd.Check = &constraint
	return cd
}

// DataType serves as a factory for creating ColumnDef instances.
// It provides methods for all standard PostgreSQL data types.
type DataType struct{}

// Varchar creates a VARCHAR column with the specified length.
func (dt DataType) Varchar(length int) *ColumnDef {
	return &ColumnDef{Type: "varchar", Length: &length}
}

// Char creates a CHAR column with the specified length.
func (dt DataType) Char(length int) *ColumnDef {
	return &ColumnDef{Type: "char", Length: &length}
}

// Text creates a TEXT column.
func (dt DataType) Text() *ColumnDef {
	return &ColumnDef{Type: "text"}
}

// Integer creates an INTEGER column.
func (dt DataType) Integer() *ColumnDef {
	return &ColumnDef{Type: "integer"}
}

// Bigint creates a BIGINT column.
func (dt DataType) Bigint() *ColumnDef {
	return &ColumnDef{Type: "bigint"}
}

// Smallint creates a SMALLINT column.
func (dt DataType) Smallint() *ColumnDef {
	return &ColumnDef{Type: "smallint"}
}

// Serial creates a SERIAL column (auto-incrementing integer).
func (dt DataType) Serial() *ColumnDef {
	return &ColumnDef{Type: "serial"}
}

// Bigserial creates a BIGSERIAL column (auto-incrementing big integer).
func (dt DataType) Bigserial() *ColumnDef {
	return &ColumnDef{Type: "bigserial"}
}

// Decimal creates a DECIMAL column with precision and scale.
func (dt DataType) Decimal(precision, scale int) *ColumnDef {
	return &ColumnDef{Type: "decimal", Precision: &precision, Scale: &scale}
}

// Numeric creates a NUMERIC column with precision and scale.
func (dt DataType) Numeric(precision, scale int) *ColumnDef {
	return &ColumnDef{Type: "numeric", Precision: &precision, Scale: &scale}
}

// Real creates a REAL column (single precision floating-point).
func (dt DataType) Real() *ColumnDef {
	return &ColumnDef{Type: "real"}
}

// DoublePrecision creates a DOUBLE PRECISION column.
func (dt DataType) DoublePrecision() *ColumnDef {
	return &ColumnDef{Type: "double precision"}
}

// Timestamp creates a TIMESTAMP column.
func (dt DataType) Timestamp() *ColumnDef {
	return &ColumnDef{Type: "timestamp"}
}

// Timestamptz creates a TIMESTAMP WITH TIME ZONE column.
func (dt DataType) Timestamptz() *ColumnDef {
	return &ColumnDef{Type: "timestamptz"}
}

// Date creates a DATE column.
func (dt DataType) Date() *ColumnDef {
	return &ColumnDef{Type: "date"}
}

// Time creates a TIME column.
func (dt DataType) Time() *ColumnDef {
	return &ColumnDef{Type: "time"}
}

// Timetz creates a TIME WITH TIME ZONE column.
func (dt DataType) Timetz() *ColumnDef {
	return &ColumnDef{Type: "timetz"}
}

// Interval creates an INTERVAL column.
func (dt DataType) Interval() *ColumnDef {
	return &ColumnDef{Type: "interval"}
}

// Boolean creates a BOOLEAN column.
func (dt DataType) Boolean() *ColumnDef {
	return &ColumnDef{Type: "boolean"}
}

// Json creates a JSON column.
func (dt DataType) Json() *ColumnDef {
	return &ColumnDef{Type: "json"}
}

// Jsonb creates a JSONB column (binary JSON).
func (dt DataType) Jsonb() *ColumnDef {
	return &ColumnDef{Type: "jsonb"}
}

// Uuid creates a UUID column.
func (dt DataType) Uuid() *ColumnDef {
	return &ColumnDef{Type: "uuid"}
}

// Bytea creates a BYTEA column (binary data).
func (dt DataType) Bytea() *ColumnDef {
	return &ColumnDef{Type: "bytea"}
}

// Array creates an ARRAY column of the specified base type.
func (dt DataType) Array(baseType string) *ColumnDef {
	return &ColumnDef{Type: baseType + "[]"}
}

// Money creates a MONEY column.
func (dt DataType) Money() *ColumnDef {
	return &ColumnDef{Type: "money"}
}

// Point creates a POINT column (geometric).
func (dt DataType) Point() *ColumnDef {
	return &ColumnDef{Type: "point"}
}

// Line creates a LINE column (geometric).
func (dt DataType) Line() *ColumnDef {
	return &ColumnDef{Type: "line"}
}

// Lseg creates a LSEG column (line segment).
func (dt DataType) Lseg() *ColumnDef {
	return &ColumnDef{Type: "lseg"}
}

// Box creates a BOX column (geometric).
func (dt DataType) Box() *ColumnDef {
	return &ColumnDef{Type: "box"}
}

// Path creates a PATH column (geometric).
func (dt DataType) Path() *ColumnDef {
	return &ColumnDef{Type: "path"}
}

// Polygon creates a POLYGON column (geometric).
func (dt DataType) Polygon() *ColumnDef {
	return &ColumnDef{Type: "polygon"}
}

// Circle creates a CIRCLE column (geometric).
func (dt DataType) Circle() *ColumnDef {
	return &ColumnDef{Type: "circle"}
}

// Cidr creates a CIDR column (IP network).
func (dt DataType) Cidr() *ColumnDef {
	return &ColumnDef{Type: "cidr"}
}

// Inet creates an INET column (IP address).
func (dt DataType) Inet() *ColumnDef {
	return &ColumnDef{Type: "inet"}
}

// Macaddr creates a MACADDR column.
func (dt DataType) Macaddr() *ColumnDef {
	return &ColumnDef{Type: "macaddr"}
}

// Macaddr8 creates a MACADDR8 column.
func (dt DataType) Macaddr8() *ColumnDef {
	return &ColumnDef{Type: "macaddr8"}
}

// Bit creates a BIT column with the specified length.
func (dt DataType) Bit(length int) *ColumnDef {
	return &ColumnDef{Type: "bit", Length: &length}
}

// Varbit creates a VARBIT column with the specified length.
func (dt DataType) Varbit(length int) *ColumnDef {
	return &ColumnDef{Type: "varbit", Length: &length}
}

// Tsvector creates a TSVECTOR column (text search).
func (dt DataType) Tsvector() *ColumnDef {
	return &ColumnDef{Type: "tsvector"}
}

// Tsquery creates a TSQUERY column (text search).
func (dt DataType) Tsquery() *ColumnDef {
	return &ColumnDef{Type: "tsquery"}
}

// Xml creates an XML column.
func (dt DataType) Xml() *ColumnDef {
	return &ColumnDef{Type: "xml"}
}

// Int4range creates an INT4RANGE column.
func (dt DataType) Int4range() *ColumnDef {
	return &ColumnDef{Type: "int4range"}
}

// Int8range creates an INT8RANGE column.
func (dt DataType) Int8range() *ColumnDef {
	return &ColumnDef{Type: "int8range"}
}

// Numrange creates a NUMRANGE column.
func (dt DataType) Numrange() *ColumnDef {
	return &ColumnDef{Type: "numrange"}
}

// Tsrange creates a TSRANGE column.
func (dt DataType) Tsrange() *ColumnDef {
	return &ColumnDef{Type: "tsrange"}
}

// Tstzrange creates a TSTZRANGE column.
func (dt DataType) Tstzrange() *ColumnDef {
	return &ColumnDef{Type: "tstzrange"}
}

// Daterange creates a DATERANGE column.
func (dt DataType) Daterange() *ColumnDef {
	return &ColumnDef{Type: "daterange"}
}

// Enum creates a column with a custom ENUM type.
func (dt DataType) Enum(typeName string) *ColumnDef {
	return &ColumnDef{Type: typeName}
}

// Domain creates a column with a custom DOMAIN type.
func (dt DataType) Domain(domainName string) *ColumnDef {
	return &ColumnDef{Type: domainName}
}

// Oid creates an OID column.
func (dt DataType) Oid() *ColumnDef {
	return &ColumnDef{Type: "oid"}
}

// Regproc creates a REGPROC column.
func (dt DataType) Regproc() *ColumnDef {
	return &ColumnDef{Type: "regproc"}
}

// Regprocedure creates a REGPROCEDURE column.
func (dt DataType) Regprocedure() *ColumnDef {
	return &ColumnDef{Type: "regprocedure"}
}

// Regoper creates a REGOPER column.
func (dt DataType) Regoper() *ColumnDef {
	return &ColumnDef{Type: "regoper"}
}

// Regoperator creates a REGOPERATOR column.
func (dt DataType) Regoperator() *ColumnDef {
	return &ColumnDef{Type: "regoperator"}
}

// Regclass creates a REGCLASS column.
func (dt DataType) Regclass() *ColumnDef {
	return &ColumnDef{Type: "regclass"}
}

// Regtype creates a REGTYPE column.
func (dt DataType) Regtype() *ColumnDef {
	return &ColumnDef{Type: "regtype"}
}

// Regconfig creates a REGCONFIG column.
func (dt DataType) Regconfig() *ColumnDef {
	return &ColumnDef{Type: "regconfig"}
}

// Regdictionary creates a REGDICTIONARY column.
func (dt DataType) Regdictionary() *ColumnDef {
	return &ColumnDef{Type: "regdictionary"}
}

// Record creates a RECORD column.
func (dt DataType) Record() *ColumnDef {
	return &ColumnDef{Type: "record"}
}

// Cstring creates a CSTRING column.
func (dt DataType) Cstring() *ColumnDef {
	return &ColumnDef{Type: "cstring"}
}

// Any creates an ANY column.
func (dt DataType) Any() *ColumnDef {
	return &ColumnDef{Type: "any"}
}

// Anyarray creates an ANYARRAY column.
func (dt DataType) Anyarray() *ColumnDef {
	return &ColumnDef{Type: "anyarray"}
}

// Anyelement creates an ANYELEMENT column.
func (dt DataType) Anyelement() *ColumnDef {
	return &ColumnDef{Type: "anyelement"}
}

// Anyenum creates an ANYENUM column.
func (dt DataType) Anyenum() *ColumnDef {
	return &ColumnDef{Type: "anyenum"}
}

// Anynonarray creates an ANYNONARRAY column.
func (dt DataType) Anynonarray() *ColumnDef {
	return &ColumnDef{Type: "anynonarray"}
}

// Anyrange creates an ANYRANGE column.
func (dt DataType) Anyrange() *ColumnDef {
	return &ColumnDef{Type: "anyrange"}
}

// Additional numeric types
func (dt DataType) Float4() *ColumnDef {
	return &ColumnDef{Type: "float4"}
}

func (dt DataType) Float8() *ColumnDef {
	return &ColumnDef{Type: "float8"}
}

func (dt DataType) Int2() *ColumnDef {
	return &ColumnDef{Type: "int2"}
}

func (dt DataType) Int4() *ColumnDef {
	return &ColumnDef{Type: "int4"}
}

func (dt DataType) Int8() *ColumnDef {
	return &ColumnDef{Type: "int8"}
}

// Custom type method for user-defined types
func (dt DataType) Custom(typeName string) *ColumnDef {
	return &ColumnDef{Type: typeName}
}
