package dbutil

import (
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm/schema"
)

var inContainer bool
var once sync.Once

var driverToUtil map[string]DBUtil

type migrationVersion struct {
	ID            *int    `gorm:"primaryKey;autoIncrement:true"`
	MigrationName *string `gorm:"primaryKey"`
	Fullpath      *string
	SQL           *string
	Date          *time.Time
}

func NewMigrationVersion(fullpath, sql string) *migrationVersion {
	migrationName := getFileName(fullpath)
	now := time.Now()
	return &migrationVersion{
		Fullpath:      &fullpath,
		SQL:           &sql,
		Date:          &now,
		MigrationName: &migrationName,
	}
}

func (mv *migrationVersion) HasRun() bool {
	return mv.ID != nil && mv.Date != nil
}

// The name of the table that holds the record of migrations.
// This value must no tbe escaped

// register all the supported DBUtil classes
func init() {
	driverToUtil = make(map[string]DBUtil)
	driverToUtil["postgres"] = &PostgresDBUtils{}
}

// DBUtils allows us to
type DBUtil interface {
	CreateTable(db *DB) error
	DSN(db *DB) string
	IsDatabaseNotExistError(err error) bool
	IsTableNotExistError(err error) bool
	MigrationTableExists(db *DB) error
	CreateMigrationTable(db *DB) error
	OpenConnection(db *DB, flags DBOpenFlags, r schema.Replacer) error
	CreateIfNotExists(db *DB) error
	MigrationTableName() string
}

func (db *DB) getMigrationFolder() string {
	if db.MigrationFolder != "" {
		return db.MigrationFolder
	} else if InsideContainer() {
		return "/opt/jkg/schema/"
	} else {
		return "./schema"
	}
}

func (db *DB) getMigrationFiles() ([]string, error) {
	baseDir := db.getMigrationFolder()
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	arr := make([]string, len(entries))
	for i, entry := range entries {
		arr[i] = path.Join(baseDir, entry.Name())
	}
	return arr, nil
}

// getFileName attempts to suck the filename out of a given full path
func getFileName(fullpath string) string {
	re := regexp.MustCompile("[\\/]([A-Za-z0-9_-]+).sql$")
	matches := re.FindAllString(fullpath, -1)
	if len(matches) == 0 {
		return fullpath
	} else {
		return strings.TrimLeft(matches[0], "\\/")
	}
}

// InContainer alerts the caller of whether (or not) this service is running
// inside or outside a docker container
func InsideContainer() bool {
	once.Do(func() {
		if runtime.GOOS != "windows" {
			if _, err := os.Stat("/.dockerenv"); !os.IsNotExist(err) {
				inContainer = true
			} else {
				inContainer = false
			}
		} else {
			inContainer = false
		}
	})
	return inContainer
}
