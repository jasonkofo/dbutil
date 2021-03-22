package dbutil

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"

	_ "github.com/lib/pq" // driver
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// PostgresDBUtils, in other programming languages, would be represented as a
// static class. It exists for the purpose of implementing the visitor design
// pattern
type PostgresDBUtils struct{}

func (pdu *PostgresDBUtils) CreateTable(db *DB) error {
	db2 := *db
	db2.Database = "postgres"

	if sqlDB, err := sql.Open("postgres", pdu.DSN(&db2)); err == nil {
		q := fmt.Sprintf(`CREATE DATABASE "%v" OWNER "%v"`, db.Database, db.User)
		if _, err := sqlDB.Exec(q); err != nil {
			return fmt.Errorf("Could not create Postgres database: %v", err)
		}
	} else {
		return fmt.Errorf("Could not connect to postgres database: %v", err)
	}
	return nil
}

// isDatabaseNotExistError allows the users to distinguish between errors
// that exist because the databases do not exist
func (pdf *PostgresDBUtils) IsDatabaseNotExistError(err error) bool {
	matched, err := regexp.MatchString("database \"\\w+\" does not exist", err.Error())
	if err != nil {
		return false
	}
	return matched
}

// isDatabaseNotExistError allows the users to distinguish between errors
// that exist because the databases do not exist
func (pdf *PostgresDBUtils) IsTableNotExistError(err error) bool {
	matched, err := regexp.MatchString("relation \"\\w+\" does not exist", err.Error())
	if err != nil {
		return false
	}
	return matched
}

func (pdu *PostgresDBUtils) DSN(db *DB) string {
	dsn := ""
	if db.Database != "" {
		dsn += "dbname=" + db.Database + " "
	}
	if db.Host != "" {
		dsn += "host=" + db.Host + " "
	}
	if db.Port != 0 {
		dsn += "port=" + strconv.Itoa(db.Port) + " "
	}
	if db.User != "" {
		dsn += "user=" + db.User + " "
	}
	if db.Password != "" {
		dsn += "password=" + db.Password + " "
	}
	if db.SSLMode == "require" || db.SSLMode == "allow" {
		dsn += "sslmode=" + db.SSLMode + " "
	} else {
		dsn += "sslmode=disable "
	}
	return dsn
}

func (pdu *PostgresDBUtils) MigrationTableExists(db *DB) error {
	if err := db.Conn.Exec("SELECT * from \"" + pdu.MigrationTableName() + "\"").Error; err == nil {
		return nil
	} else {
		return err
	}
}

func (pdu *PostgresDBUtils) CreateMigrationTable(db *DB) error {
	tx := db.Conn.Begin()
	if tx.Error != nil {
		return fmt.Errorf("Could not begin transaction: %v", tx.Error)
	}
	defer tx.Rollback()
	// creates dummy record and then rolls back the transaction
	mv := NewMigrationVersion("the one", "sql")
	if err := tx.Create(mv).Error; err != nil && pdu.IsTableNotExistError(err) {
		// migrations to the schema cannot be performed inside a transaction,
		// hence our desire to create the table on the source gorm.DB object
		if err := db.Conn.AutoMigrate(migrationVersion{}); err != nil {
			return fmt.Errorf("Could not create table: %v", err)
		}
	}
	return nil
}

func (pdu *PostgresDBUtils) OpenConnection(db *DB, flags DBOpenFlags) error {
	var ns schema.Namer
	if flags&SingularTable > 0 {
		ns = schema.NamingStrategy{
			SingularTable: true,
		}
	} else {
		ns = schema.NamingStrategy{}
	}
	cfg := &gorm.Config{
		NamingStrategy: ns,
	}
	if conn, err := gorm.Open(postgres.Open(db.DSN()), cfg); err == nil {
		db.Conn = conn
	} else {
		return fmt.Errorf("Failed to connect to database: %v", err)
	}
	if flags&Migrate > 0 {
		if err := db.Migrate(); err != nil {
			errMsg := fmt.Sprintf("Could not migrate the '%v' database: %v", db.Database, err)
			if flags&PanicOnFailedMigration > 0 {
				panic(errMsg)
			} else {
				return fmt.Errorf(errMsg)
			}
		}
	}
	return nil
}

func (pdu *PostgresDBUtils) CreateIfNotExists(db *DB) error {
	conn, err := gorm.Open(postgres.Open(db.DSN()), &gorm.Config{})
	if err != nil {
		if !pdu.IsDatabaseNotExistError(err) {
			return fmt.Errorf("Database does not exist: %v", err)
		}
		return pdu.CreateTable(db)
	}
	if conn != nil {
		db.Conn = conn
	}
	return nil
}

func (pdu *PostgresDBUtils) MigrationTableName() string {
	return "migration_version"
}
