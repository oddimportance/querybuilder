package querybuilder

// TLS is often false
// MaxOpenConnections and MaxIdleConnections can be left unset, default is 10
type Credentials struct {
	Host               string
	Port               string
	User               string
	Password           string
	Database           string
	Tls                bool
	MaxOpenConnections int
	MaxIdleConnections int
}

type TableDetails struct {
	Table  string
	Prefix string
}

type ClauseLogic string
type ClauseOperator string
type MySqlFunction string
type JoinType string
type queryType string

const (
	queryTypeSelect    queryType = "select"
	queryTypeSelectRow           = "selectRow"
	queryTypeUpdate              = "update"
	queryTypeDelete              = "delete"
)

const (
	OR  ClauseLogic = "OR"
	AND             = "AND"
	NOT             = "NOT"
)

const (
	Equal              ClauseOperator = "="
	NotEqual                          = "!="
	GreaterThan                       = ">"
	LessThan                          = "<"
	LessThanEqualTo                   = "<="
	GreaterThanEqualTo                = ">="
	Like                              = "LIKE"
	IsNull                            = "IS NULL"
	IsNotNull                         = "IS NOT NULL"
	NotLike                           = "NOT LIKE"
	Between                           = "BETWEEN"
	In                                = "IN"
	NotIn                             = "NOT IN"
	As                                = "AS"
	Match                             = "MATCH(%s)"
	Against                           = "AGAINST(%s)"
)

// Expand with more MySQL Functions as need may grow
const (
	Avg   MySqlFunction = "AVG(%s)"
	Count               = "COUNT(%s)"
	Max                 = "MAX(%s)"
	Min                 = "Min(%s)"
	Sum                 = "SUM(%s)"
	Now                 = "NOW()"
	Year                = "YEAR(%s)"
)

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin           = "LEFT JOIN"
	RightJoin          = "RIGHT JOIN"
)

const Distinct string = "DISTINCT"
const preparationPlaceHolder string = "?"

type Clause struct {
	ClauseLogic                 ClauseLogic
	Column                      string
	ValueAggregatedWithOperator string
}

type Where struct {
	WhereLogic ClauseLogic
	Conditions []Clause
}

type Order string

const (
	Desc Order = "DESC"
	Asc        = "ASC"
)

type OrderBy struct {
	Column string
	Order  Order
}

type limitParams struct {
	Limit  int
	Offset int
}

type join struct {
	JoinType    JoinType
	ForignTable string
	PrimaryKey  string
	ForignKey   string
}
