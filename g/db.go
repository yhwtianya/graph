package g

import (
	"database/sql"
	"log"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

// TODO 草草的写了一个db连接池,优化下
var (
	dbLock    sync.RWMutex
	dbConnMap map[string]*sql.DB // 简单的mysql连接池
)

// mysql 连接
var DB *sql.DB

// 初始化DB、dbConnMap
func InitDB() {
	var err error
	DB, err = makeDbConn()
	if DB == nil || err != nil {
		log.Fatalln("g.InitDB, get db conn fail", err)
	}

	dbConnMap = make(map[string]*sql.DB)
	log.Println("g.InitDB ok")
}

func GetDbConn(connName string) (c *sql.DB, e error) {
	dbLock.Lock()
	defer dbLock.Unlock()

	var err error
	var dbConn *sql.DB
	dbConn = dbConnMap[connName]
	if dbConn == nil {
		dbConn, err = makeDbConn()
		if dbConn == nil || err != nil {
			closeDbConn(dbConn)
			return nil, err
		}
		dbConnMap[connName] = dbConn
		// 这里可以有个return
	}

	err = dbConn.Ping()
	if err != nil {
		closeDbConn(dbConn)
		delete(dbConnMap, connName)
		return nil, err
	}

	return dbConn, err
}

// 创建一个新的mysql连接
func makeDbConn() (conn *sql.DB, err error) {
	conn, err = sql.Open("mysql", Config().DB.Dsn)
	if err != nil {
		return nil, err
	}

	conn.SetMaxIdleConns(Config().DB.MaxIdle)
	// 连通性
	err = conn.Ping()

	return conn, err
}

// 关闭mysql连接
func closeDbConn(conn *sql.DB) {
	if conn != nil {
		conn.Close()
	}
}
