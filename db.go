package dbutil

import (
	"fmt"
	"os"
	"strconv"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type EnvReadMode uint8

const (
	ReadAll      EnvReadMode = 0
	ReadPassword EnvReadMode = 1 << iota
	ReadHost
	ReadUser
	ReadName
	ReadPort
	ReadSSLMode
	ReadDriver
)

type DBOpenFlags uint8

const (
	None              DBOpenFlags = 0
	CreateIfNotExists DBOpenFlags = 1 << iota
	Migrate
	PanicOnFailedMigration
	SingularTable
	SkipDefaultTransaction
)

type DB struct {
	Database        string
	Port            int
	Driver          string
	Host            string
	SSLMode         string
	User            string
	Password        string
	MigrationFolder string
	Conn            *gorm.DB
	Utils           DBUtil
}

func NewDB(port int, database, driver, host, sslmode, user, password, migrationFolder string) (*DB, error) {
	_db := &DB{
		Port:            port,
		Database:        database,
		Driver:          driver,
		Host:            host,
		SSLMode:         sslmode,
		User:            user,
		Password:        password,
		MigrationFolder: migrationFolder,
	}
	if err := _db.assertDriver(); err != nil {
		return nil, err
	}
	return _db, nil
}

func (db *DB) assertDriver() error {
	var ok bool
	if db.Utils, ok = driverToUtil[db.Driver]; !ok {
		return fmt.Errorf("Database driver '" + db.Driver + "' unsupported")
	}
	return nil
}

func (db *DB) IsSQLServer() bool {
	return db.Driver == "sqlserver"
}

func (db *DB) IsPostgres() bool {
	return db.Driver == "postgres"
}

func (db *DB) DSN() string {
	return db.Utils.DSN(db)
}

// func (db *DB) sqlserverDSN() string {
// 	dsn := fmt.Sprintf("sqlserver://%v:%v@%v",
// 		db.User, url.QueryEscape(db.Password), db.Host,
// 	)

// 	if db.Port != 0 {
// 		dsn += fmt.Sprintf(":%v", db.Port)
// 	}
// 	return dsn + "?database=" + db.Name

// }

// ReadFromOS reads the password credentials from the operating system's
// environment variables. This function returns true if dbconfig information has been read
func (db *DB) ReadFromOS(mode EnvReadMode, overwrite bool) bool {
	var (
		hostOK, pwdOK, nameOK, sslModeOK, portOK, driverOK, userOK bool
		pwd, host, name, sslMode, port, driver, user               string
		_db                                                        = DB{}
		found                                                      = false
	)
	if db == nil {
		*db = DB{}
	}
	if pwd, pwdOK = os.LookupEnv("dbpassword"); pwdOK {
		_db.Password = pwd
		if mode&ReadPassword > 0 {
			found = true
		}
	}
	if host, hostOK = os.LookupEnv("dbhost"); hostOK {
		_db.Host = host
		if mode&ReadHost > 0 {
			found = true
		}
	}
	if name, nameOK = os.LookupEnv("dbname"); nameOK {
		_db.Database = name
		if mode&ReadName > 0 {
			found = true
		}
	}
	if port, portOK = os.LookupEnv("dbport"); portOK {
		var err error
		_db.Port, err = strconv.Atoi(port)
		if err != nil {
			portOK = false
		}
		if mode&ReadPort > 0 {
			found = true
		}
	}
	if sslMode, sslModeOK = os.LookupEnv("dbsslmode"); sslModeOK {
		_db.SSLMode = sslMode
		if mode&ReadSSLMode > 0 {
			found = true
		}
	}
	if user, userOK = os.LookupEnv("dbuser"); userOK {
		_db.User = user
		if mode&ReadUser > 0 {
			found = true
		}
	}
	if driver, driverOK = os.LookupEnv("dbdriver"); driverOK {
		_db.Driver = driver
		if mode&ReadDriver > 0 {
			found = true
		}
	}
	if mode == ReadAll {
		found = sslModeOK || driverOK || pwdOK || hostOK || nameOK || portOK || userOK
	}

	if found && overwrite {
		*db = _db
	} else if found {

	}
	return found
}

func (db *DB) createIfNotExists() error {
	return db.Utils.CreateIfNotExists(db)
}

// OpenDefault allows the user to open with the default options
func (db *DB) OpenDefault() error {
	return db.Open(DefaultOpenFlags(), nil)
}

// OpenDefaultWithSchemaReplacer allows the user to open with the default options
func (db *DB) OpenDefaultWithSchemaReplacer(r schema.Replacer) error {
	return db.Open(DefaultOpenFlags(), r)
}

func (db *DB) Open(flags DBOpenFlags, r schema.Replacer) error {
	if flags&CreateIfNotExists > 0 {
		if err := db.createIfNotExists(); err != nil {
			return fmt.Errorf("Failed to create the db on open: %v", err)
		}
	}

	err := db.Utils.OpenConnection(db, flags, r)
	if err == nil {
		if flags&Migrate > 0 {
			if err = db.Migrate(); err != nil {
				return fmt.Errorf("Could not migrate database '%v': %v", db.Database, err)
			}
		}
	} else {
		return fmt.Errorf("Could not open connection to database: %v", err)
	}

	return nil
}

func (db *DB) Migrate() error {
	nChanges := 0
	files, err := db.getMigrationFiles()
	if err != nil {
		return fmt.Errorf("Could not find migration files: %v", err)
	}
	tx := db.Conn.Begin()
	if tx.Error != nil {
		return fmt.Errorf("Could not begin migration transaction: %v", tx.Error)
	} else {
		defer tx.Rollback()
	}
	if err := db.Utils.CreateMigrationTable(db); err != nil {
		return fmt.Errorf("Error creating migration table in database '%v': %v", db.Database, err)
	}
	for _, fn := range files {
		cfn := getFileName(fn)
		b, err := os.ReadFile(fn)
		if err != nil {
			return fmt.Errorf("Could not read query file: %v", err)
		}
		_mv := migrationVersion{}
		if err := tx.Where(migrationVersion{MigrationName: &cfn}).First(&_mv).Error; err != nil && gorm.ErrRecordNotFound != err {
			return fmt.Errorf("Could not find migration_version record: %v", err)
		}
		if _mv.HasRun() {
			continue
		}
		sql := string(b)
		if err := tx.Exec(sql).Error; err != nil {
			return fmt.Errorf("Could not execute query in '%v': %v", fn, err)
		}
		if err := tx.Create(NewMigrationVersion(fn, sql)).Error; err != nil {
			return fmt.Errorf("Could not create '%v' record: %v", db.Utils.MigrationTableName(), err)
		}
		nChanges++
	}
	if nChanges > 0 {
		if err := tx.Commit().Error; err != nil {
			return fmt.Errorf("Could not commit transaction: %v", err)
		}
	}
	return nil
}
