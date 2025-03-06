package querybuilder

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DbAdapter struct {
	_db                                    *sql.DB
	isConnectedToServer                    bool
	dbTable                                string
	dbTableFieldPrefix                     string
	queryType                              queryType
	queryColumns                           []string
	queryValues                            []interface{}
	queryString                            string
	dbCredentials                          Credentials
	queryHasPotentialThreat                bool
	whereClauses                           []Where
	clauseValues                           []interface{}
	queryAggregatedValuesPreparedStatement []interface{}
	orderBy                                []OrderBy
	groupBy                                []string
	queryLimit                             limitParams
	joins                                  []join
	lastExecutedQuery                      string
	// havingClausesAnd                       []Clause
	// havingClausesOr                        []Clause
	// havingClausesNot                       []Clause
}

func (d *DbAdapter) Connect(dbCredentials Credentials, table TableDetails) {

	d.dbCredentials = dbCredentials
	d.MakeServerCredentials(dbCredentials)

	// Initialize connection string.
	var connectionString = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?allowNativePasswords=true&tls=%t", d.dbCredentials.User, d.dbCredentials.Password, d.dbCredentials.Host, d.dbCredentials.Port, d.dbCredentials.Database, d.dbCredentials.Tls) // tls is often false

	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		d.handleConnectionError(err)
	}

	if err = db.Ping(); err != nil {
		d.handleConnectionError(err)
	}

	d._db = db

	noOfConnection := 10
	maxOpenConnections := noOfConnection
	maxIdleConnections := noOfConnection

	if dbCredentials.MaxOpenConnections > 0 {
		maxOpenConnections = dbCredentials.MaxOpenConnections
	}

	if dbCredentials.MaxIdleConnections > 0 {
		maxIdleConnections = dbCredentials.MaxIdleConnections
	}

	d._db.SetMaxOpenConns(maxOpenConnections)
	d._db.SetMaxIdleConns(maxIdleConnections)
	db.SetConnMaxLifetime(time.Minute * 3)
	d.SetTableAndPrefix(table)

	d.isConnectedToServer = true
}

func (d *DbAdapter) PrintDBDetails() {

	fmt.Println("+++++++++++++ Database Details +++++++++++++")
	fmt.Println("============================================")
	fmt.Printf("Host: %s\n", d.dbCredentials.Host)
	fmt.Printf("Port: %s\n", d.dbCredentials.Port)
	fmt.Printf("User Name: %s\n", d.dbCredentials.User)
	fmt.Printf("Database Name: %s\n", d.dbCredentials.Database)
	fmt.Println("============================================")
	fmt.Println()
}

// Extend the exisisting DBAdapter to use sub tables
// on the main DB Connected thread
// @ param db *sql.DB
// @ return void
func (d *DbAdapter) InitWithoutConnection(db *sql.DB, table TableDetails) {
	d.SetSqlConnection(db)
	d.SetTableAndPrefix(table)
}

func (d *DbAdapter) GetSqlConnection() *sql.DB {
	return d._db
}

func (d *DbAdapter) SetSqlConnection(db *sql.DB) {
	// Make sure connected to sql server
	if db == nil {
		d.isConnectedToServer = false
	} else {
		d.isConnectedToServer = true
	}
	d._db = db
}

// Set the table and its preifx, if you
// have to handle additional tables other
// than the main table.
// @ param dbTable string Name of the table
// @ param tableFieldPrefix string Table prefix
// @ return void
func (d *DbAdapter) SetTableAndPrefix(table TableDetails) {
	d.setDbTable(table.Table)
	d.setDbTableFieldPrefix(table.Prefix)
}

func (d *DbAdapter) setDbTable(dbTable string) {
	d.dbTable = dbTable
}

func (d *DbAdapter) setDbTableFieldPrefix(dbTableFieldPrefix string) {
	d.dbTableFieldPrefix = dbTableFieldPrefix
}

// returns table name
func (d *DbAdapter) GetTableName() string {
	return d.dbTable
}

func (d *DbAdapter) GetDbTableFieldPrefix() string {
	return d.dbTableFieldPrefix
}

// func (d *DbAdapter) MakeServerCredentials(
// 	port,
// 	host,
// 	dbName,
// 	dbUser,
// 	dbPassword string) *Credentials {
func (d *DbAdapter) MakeServerCredentials(credentials Credentials) {

	host := ""
	if credentials.Host == "" {
		host = "localhost"
	}

	port := ""
	if credentials.Port == "" {
		port = "3306"
	}

	// tls := ""
	// if credentials.Tls == "" {
	// 	tls = false
	// }

	// var dbCredentials = new(Credentials)
	credentials.Host = host
	credentials.Port = port
	// credentials.Tls = tls

	d.dbCredentials = credentials

}

func (d *DbAdapter) handleConnectionError(err error) {

	d.isConnectedToServer = false

	if err != nil {
		fmt.Println(err)
	}
	// Finally panic
	panic("Oops! DB connection could be established")
}

func (d *DbAdapter) unsetQueryParams() {

	// _db                       *sql.DB
	// isConnectedToServer       bool
	// dbTable                   string
	// dbTableFieldPrefix        string
	// queryType                 queryType
	// queryColumns              []string
	// queryValues               []string
	// queryString               string
	// dbCredentials             Credentials
	// queryHasPotentialThreat   bool
	// whereClauses              []Where
	// havingClausesAnd          []Clause
	// havingClausesOr           []Clause
	// havingClausesNot          []Clause
	// queryPreparedClauseValues []interface{}
	// orderBy                   []OrderBy
	// groupBy                   []string
	// queryLimit                limitParams
	// joins                     []join
	d.queryType = ""
	d.queryColumns = nil
	d.queryValues = nil
	d.queryString = ""
	d.queryHasPotentialThreat = false
	d.whereClauses = nil
	d.clauseValues = nil
	d.queryAggregatedValuesPreparedStatement = nil
	d.orderBy = nil
	d.groupBy = nil
	d.queryLimit = limitParams{}
	d.joins = nil
	// d.lastExecutedQuery = ""

}

func (d *DbAdapter) setLastExecutedQuery() {
	d.lastExecutedQuery = fmt.Sprintf("\n%s, %v\n", d.queryString, d.queryAggregatedValuesPreparedStatement)
}

func (d *DbAdapter) PrintLastExecutedQuery() {
	queryToPrint := d.lastExecutedQuery
	if queryToPrint == "" {
		queryToPrint = "There was no query to print"
	}
	log.Println(queryToPrint)
}
