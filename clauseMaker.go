package querybuilder

import (
	"fmt"
	"strings"
)

func (d *DbAdapter) Join(joinType JoinType, forignTable, primaryKey, forignKey string) *DbAdapter {
	d.joins = append(d.joins, join{JoinType: joinType, ForignTable: forignTable, PrimaryKey: primaryKey, ForignKey: forignKey})
	return d
}

func (d *DbAdapter) Limit(limit int, offset ...int) *DbAdapter {
	if len(offset) == 1 {
		d.queryLimit = limitParams{Limit: limit, Offset: offset[0]}
	} else {
		d.queryLimit = limitParams{Limit: limit, Offset: 0} // Default to 0 if offset is not provided
	}
	return d
}

func (d *DbAdapter) GroupBy(groupBy []string) *DbAdapter {
	d.groupBy = groupBy
	return d
}

func (d *DbAdapter) OrderBy(orderBy OrderBy) *DbAdapter {
	d.orderBy = append(d.orderBy, orderBy)
	return d
}

func (d *DbAdapter) Where(whereGroup Where) *DbAdapter {
	d.whereClauses = append(d.whereClauses, whereGroup)
	return d
}

func (d *DbAdapter) MakeCondition(conditionLogic ClauseLogic, column string, valueAggregatedWithOperator string) Clause {
	return Clause{ClauseLogic: conditionLogic, Column: column, ValueAggregatedWithOperator: valueAggregatedWithOperator}
}

func (d *DbAdapter) MakeWhereGroup(wherLogic ClauseLogic, conditions []Clause) Where {
	return Where{WhereLogic: wherLogic, Conditions: conditions}
}

// Usage: Max(column), Count(*), AVG(column)
func (d *DbAdapter) MakeMySQLFunction(column string, mysqlFunction MySqlFunction) string {
	return fmt.Sprintf(string(mysqlFunction), column)
}

// Fulltext Search Usage: WHERE MATCH(column1, column2) AGAINST('search term');
func (d *DbAdapter) MakeMatchAgainstColumn(columns []string) string {
	return fmt.Sprintf(Match, strings.Join(columns, ", "))
}

// Fulltext Search Usage: WHERE MATCH(column1, column2) AGAINST('search term');
func (d *DbAdapter) MakeMatchAgainstSearchTerm(searchTerm string) string {
	d.setAggregatedValueForClauses(searchTerm)
	return fmt.Sprintf(Against, preparationPlaceHolder)
}

func (d *DbAdapter) MakeAsField(column, asParam string) string {
	return fmt.Sprintf("%s %s %s", column, As, asParam)
}

func (d *DbAdapter) MakeBetween(rangeBegin, rangeEnd interface{}) string {
	d.setAggregatedValueForClauses(rangeBegin)
	d.setAggregatedValueForClauses(rangeEnd)
	return fmt.Sprintf("%s (%s %s %s)", Between, preparationPlaceHolder, AND, preparationPlaceHolder)
}

func (d *DbAdapter) MakeDistinct(column string) string {
	return fmt.Sprintf("%s %s", Distinct, column)
}

func (d *DbAdapter) MakeAggregatedValueWithOperator(operator ClauseOperator, item interface{}) string {
	d.setAggregatedValueForClauses(item)
	return fmt.Sprintf("%s %s", operator, preparationPlaceHolder)
}

func (d *DbAdapter) MakeIn(items []interface{}) string {
	return d.makeInAndNotIn(In, items)
}

func (d *DbAdapter) MakeNotIn(items []interface{}) string {
	return d.makeInAndNotIn(NotIn, items)
}

func (d *DbAdapter) makeInAndNotIn(operator string, items []interface{}) string {
	placeholderSlice := []string{}
	for i := 0; i < len(items); i++ {
		d.setAggregatedValueForClauses(items[i])
		placeholderSlice = append(placeholderSlice, preparationPlaceHolder)
	}
	return fmt.Sprintf("%s (%s)", operator, strings.Join(placeholderSlice, ", "))
}

func (d *DbAdapter) setAggregatedValueForClauses(value interface{}) {
	// d.queryAggregatedValuesPreparedStatement = append(d.queryAggregatedValuesPreparedStatement, value)
	d.clauseValues = append(d.clauseValues, value)
}

func (d *DbAdapter) setAggregatedValueForPreparedStatement(value interface{}) {
	d.queryAggregatedValuesPreparedStatement = append(d.queryAggregatedValuesPreparedStatement, value)
}

func (d *DbAdapter) prepareClauseValuesForPreparedStatement() {
	for i := 0; i < len(d.clauseValues); i++ {
		d.queryAggregatedValuesPreparedStatement = append(d.queryAggregatedValuesPreparedStatement, d.clauseValues[i])
	}
}
