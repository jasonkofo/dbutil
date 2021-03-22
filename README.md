# dbutil

This repository was orginally created for the sake of very quickly managing the

## Requirements

This repository was originally written with the first dependencies:

- Ubuntu
- Go (version 1.16)
- Postgres 13.2

Extending the dependency to run

## Creating a new util

```go
db, err := dbutil.NewDB(port, dbName, driver, host, sslmode, user, password, migrationFolder)
```

`db` will then be an object representing the database config, without an open database connection. In order to open this connection, you will need to open using

```go
err = db.OpenDefault(flags)
```
