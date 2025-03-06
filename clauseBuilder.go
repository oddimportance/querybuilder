package querybuilder

import (
	"fmt"
	"strings"
	"unicode"
)

// SELECT *
// FROM customers
// WHERE (state = 'California' AND last_name = 'Johnson')
// OR (customer_id > 4500);

// SELECT customer_id, last_name, first_name
// FROM customers
// WHERE (last_name = 'Johnson')
// OR (last_name = 'Anderson' AND state = 'California')
// OR (last_name = 'Smith' AND status = 'Active' AND state = 'Florida');

// SELECT product_id, product_name, category, price
// FROM products
// WHERE (category = 'Electronics' AND price > 500)
//    OR (category = 'Furniture' AND price BETWEEN 100 AND 1000)
//    OR (category = 'Books' AND stock > 0 AND author = 'John Doe');

func (d *DbAdapter) prepareQueryStatement() {
	switch d.queryType {
	case queryTypeSelect, queryTypeSelectRow:
		d.prepareSelectStatement()
	case queryTypeUpdate:
		d.prepareUpdateStatement()
	case queryTypeDelete:
		d.prepareDeleteStatement()
	}
}

func (d *DbAdapter) prepareColumnsForStatement() (string, error) {
	if len(d.queryColumns) == 0 {
		return "", fmt.Errorf("No columns available.")
	}
	return strings.Join(d.queryColumns, ", "), nil
}

func (d *DbAdapter) prepareSelectStatement() {
	queryStringRaw := "SELECT %s FROM %s"
	columnPlaceholder := "*"

	if len(d.queryColumns) != 0 {

		preparedColumns, err := d.prepareColumnsForStatement()
		if err != nil {
			d.handleQueryResultError(err)
			return
		}
		columnPlaceholder = preparedColumns
	}

	d.concatenateQueryString(fmt.Sprintf(queryStringRaw, columnPlaceholder, d.dbTable))
}

func (d *DbAdapter) prepareInsertStatement(valuePlaceHolders []string) {
	queryStringRaw := "INSERT INTO %s (%s) VALUES(%s)"

	preparedColumns, err := d.prepareColumnsForStatement()
	if err != nil {
		d.handleQueryResultError(err)
		return
	}
	columnPlaceholder := preparedColumns
	d.concatenateQueryString(fmt.Sprintf(queryStringRaw, d.dbTable, columnPlaceholder, strings.Join(valuePlaceHolders, ", ")))
}

func (d *DbAdapter) prepareUpdateStatement() {
	lenQueryColumns := len(d.queryColumns)
	lenQueryValues := len(d.queryValues)

	if lenQueryColumns == 0 || lenQueryValues == 0 || lenQueryColumns != lenQueryValues {
		d.handleQueryResultError(fmt.Errorf("Update could not be executed. Columns and values do not pair."))
		return
	}

	queryStringRaw := "UPDATE %s"
	// set the query string globally for Join clause
	d.concatenateQueryString(fmt.Sprintf(queryStringRaw, d.dbTable))

	// Since Joins in UPDATE Statement must be instanctiated
	// prior to SET, we are exceptionally implementing JOINs
	// in the prepare function and setting the d.Joins to nil
	// since InitBuildJoins is called in makeQueryStatement
	d.initBuildJoin()
	// Now that the Join Clauses are take care of,
	// unset Join Clauses
	d.joins = nil

	columnValuePairPlaceholder := []string{}
	for i := 0; i < lenQueryColumns; i++ {
		columnValuePairPlaceholder = append(columnValuePairPlaceholder, fmt.Sprintf("%s %s %s", d.queryColumns[i], Equal, preparationPlaceHolder))
		d.setAggregatedValueForPreparedStatement(d.queryValues[i])
	}

	d.concatenateQueryString(fmt.Sprintf(" SET %s", strings.Join(columnValuePairPlaceholder, ", ")))
}

func (d *DbAdapter) prepareDeleteStatement() {
	queryStringRaw := "DELETE FROM %s"
	d.concatenateQueryString(fmt.Sprintf(queryStringRaw, d.dbTable))
}

func (d *DbAdapter) initBuildWhereClauses() {
	totalWhereGroups := len(d.whereClauses)
	if totalWhereGroups == 0 {
		return
	}
	groupsStatement := ""

	// fmt.Println(d.whereClauses)

	for i := 0; i < totalWhereGroups; i++ {
		conditionStatement := ""
		totalSubClauses := len(d.whereClauses[i].Conditions)
		groupLogic := ""
		// Skip the first logic
		if i != 0 {
			groupLogic = fmt.Sprintf("%s ", d.whereClauses[i].WhereLogic)
		}

		conditions := d.whereClauses[i].Conditions
		for j := 0; j < totalSubClauses; j++ {
			condition := conditions[j]

			conditionLogic := ""

			// Add the logic if it's not the first conditon
			if j < (totalSubClauses - 1) {
				conditionLogic = fmt.Sprintf(" %s ", condition.ClauseLogic)
			}

			conditionStatement += fmt.Sprintf("%s %s%s", condition.Column, condition.ValueAggregatedWithOperator, conditionLogic)
		}

		groupsStatement += fmt.Sprintf("%s(%s) ", groupLogic, conditionStatement)
		// fmt.Println(conditionStatement)

	}
	// fmt.Println(groupsStatement)

	d.concatenateQueryString(fmt.Sprintf("WHERE %s", strings.TrimRightFunc(groupsStatement, unicode.IsSpace)))
}

func (d *DbAdapter) concatenateQueryString(statement string) {
	d.queryString = fmt.Sprintf("%s %s", d.queryString, statement)
}

func (d *DbAdapter) initBuildOrderBy() {
	lengthOrderBy := len(d.orderBy)

	if lengthOrderBy == 0 {
		return
	}

	orderBySequences := []string{}

	for i := 0; i < lengthOrderBy; i++ {
		orderBy := d.orderBy[i]
		orderBySequences = append(orderBySequences, fmt.Sprintf("%s %s", orderBy.Column, orderBy.Order))
	}

	d.concatenateQueryString(fmt.Sprintf("ORDER BY %s", strings.Join(orderBySequences, ", ")))
}

func (d *DbAdapter) initBuildGroupBy() {
	if len(d.groupBy) == 0 {
		return
	}

	d.concatenateQueryString(fmt.Sprintf("GROUP BY %s", strings.Join(d.groupBy, ", ")))
}

func (d *DbAdapter) initBuildLimit() {
	limit := ""
	offset := ""
	if d.queryLimit.Limit > 0 {
		limit = fmt.Sprintf("LIMIT %d", d.queryLimit.Limit)

		if d.queryType == queryTypeSelect || d.queryType == queryTypeSelectRow {
			offset = fmt.Sprintf(" OFFSET %d", d.queryLimit.Offset)
		}
	}

	d.concatenateQueryString(fmt.Sprintf("%s%s", limit, offset))
}

func (d *DbAdapter) initBuildJoin() {
	lengthJoins := len(d.joins)

	if lengthJoins == 0 {
		return
	}

	joinSequences := []string{}

	for i := 0; i < lengthJoins; i++ {
		join := d.joins[i]
		joinSequences = append(joinSequences, fmt.Sprintf("%s %s ON %s %s %s", join.JoinType, join.ForignTable, join.PrimaryKey, Equal, join.ForignKey))
	}

	d.concatenateQueryString(fmt.Sprintf("%s", strings.Join(joinSequences, " ")))
}
