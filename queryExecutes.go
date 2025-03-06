package querybuilder

import (
	"database/sql"
	"log"
)

func (d *DbAdapter) Insert(columns []string, values []interface{}) sql.Result {
	d.setQueryColumns(columns)
	valuesPlaceHolder := []string{}
	for i := 0; i < len(columns); i++ {
		valuesPlaceHolder = append(valuesPlaceHolder, preparationPlaceHolder)
		d.setAggregatedValueForPreparedStatement(values[i])
	}
	d.prepareInsertStatement(valuesPlaceHolder)

	return d.runExec()
}

func (d *DbAdapter) Delete() *DbAdapter {
	d.queryType = queryTypeDelete
	return d
}

func (d *DbAdapter) Update(columnsUpdate []string, values []interface{}) *DbAdapter {
	d.queryType = queryTypeUpdate
	d.setQueryColumns(columnsUpdate)
	d.queryValues = values
	return d
}

func (d *DbAdapter) Select() *DbAdapter {
	d.queryType = queryTypeSelect
	return d
}

func (d *DbAdapter) SelectRow() *DbAdapter {
	d.queryType = queryTypeSelectRow
	return d
}

func (d *DbAdapter) SelectByColumns(columns []string) *DbAdapter {
	d.queryType = queryTypeSelect
	d.setQueryColumns(columns)
	return d
}

func (d *DbAdapter) SelectRowByColumns(columns []string) *DbAdapter {
	d.queryType = queryTypeSelectRow
	d.setQueryColumns(columns)
	return d
}

func (d *DbAdapter) setQueryColumns(columns []string) {
	if columns != nil { // skip the len validation here, as it is taken care in the clauseBuilder
		for i := 0; i < len(columns); i++ {
			d.queryColumns = append(d.queryColumns, columns[i])
		}
	}
}

func (d *DbAdapter) ExecSelect() []map[string]interface{} {
	d.makeQueryStatement()

	d.prepareClauseValuesForPreparedStatement()

	// Set query before execution
	d.setLastExecutedQuery()

	rows, err := d._db.Query(d.queryString, d.queryAggregatedValuesPreparedStatement...)

	defer rows.Close()
	d.unsetQueryParams()

	if err != nil {
		d.handleQueryResultError(err)
		return nil
	}

	return d.scanRows(rows)

}

func (d *DbAdapter) ExecSelectRow() map[string]interface{} {
	result := d.ExecSelect()
	if result != nil && len(result) == 1 {
		return result[0]
	}
	return nil
}

func (d *DbAdapter) ExecUpdate() int64 {
	return d.execUpdateOrDelete()
}

func (d *DbAdapter) ExecDelete() int64 {
	return d.execUpdateOrDelete()
}

func (d *DbAdapter) execUpdateOrDelete() int64 {
	d.makeQueryStatement()
	return d.rowsAffected(d.runExec())
}

func (d *DbAdapter) makeQueryStatement() {

	d.prepareQueryStatement()
	// Skip Join on Update and Delete
	// Join is exceptionally taken care in Update,
	// but skipped completely for Delete
	if d.queryType != queryTypeUpdate && d.queryType != queryTypeDelete {
		d.initBuildJoin()
	}
	d.initBuildWhereClauses()
	d.initBuildOrderBy()
	d.initBuildGroupBy()
	d.initBuildLimit()

}

func (d *DbAdapter) runExec() sql.Result {

	d.prepareClauseValuesForPreparedStatement()

	// Set query before execution
	d.setLastExecutedQuery()

	res, err := d._db.Exec(d.queryString, d.queryAggregatedValuesPreparedStatement...)

	defer d.unsetQueryParams()

	// Handle Query execution error here on global level
	if err != nil {
		d.handleQueryResultError(err)
		return nil
	}
	return res

}

func (d *DbAdapter) rowsAffected(result sql.Result) int64 {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		d.handleQueryResultError(err)
		return 0
	}
	return rowsAffected
}

func (d *DbAdapter) LastInsertedId(result sql.Result) int64 {
	lastInsertedId, err := result.LastInsertId()
	if err != nil {
		d.handleQueryResultError(err)
		return 0
	}
	return lastInsertedId
}

// Handle safely ignorable query execution errors
func (d *DbAdapter) handleQueryResultError(err error) {
	d.PrintLastExecutedQuery()
	log.Println(err)
}

func (d *DbAdapter) scanRows(rows *sql.Rows) []map[string]interface{} {

	// Get column types and count
	columns, err := rows.Columns()
	if err != nil {
		d.PrintLastExecutedQuery()
		log.Fatalf("Failed to get columns: %v", err)
	}

	columnCount := len(columns)
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)

	dataToReturn := []map[string]interface{}{}

	// Prepare to scan each column dynamically
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Iterate through the rows
	for rows.Next() {
		// Scan into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			d.PrintLastExecutedQuery()
			log.Fatalf("Failed to scan row: %v", err)
		}

		// Process each value
		rowData := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Handle NULL values
			if b, ok := val.([]byte); ok {
				rowData[col] = string(b) // Convert byte slice to string
			} else {
				rowData[col] = val
			}
		}

		dataToReturn = append(dataToReturn, rowData)
	}

	if err = rows.Err(); err != nil {
		d.PrintLastExecutedQuery()
		log.Fatalf("Error iterating rows: %v", err)
	}

	return dataToReturn
}
