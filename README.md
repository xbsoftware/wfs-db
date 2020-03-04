Web File System - DataBase driver
=========

File system abstraction with access management
This is the DataBase adapter for the [core interface](https://github.com/xbsoftware/wfs)

API provides common file operations for file info stored in a database.
Any operations outside for not owned files will be blocked.
Also, it possible to configure a custom policy for read/write operations.

Can be used as backend for Webix File Manager https://webix.com/filemanager


## API

### Initialization

```go
import (
    "github.com/xbsoftware/wfs"
    db "github.com/xbsoftware/wfs-db"

    _ "github.com/go-sql-driver/mysql"
    "github.com/jmoiron/sqlx"
)


conn, err := sqlx.Connect("mysql", "root:1@(localhost:3306)/files?parseTime=true")

// connection, temp folder, table name, root item, drive config
fs, err := db.NewDBDrive(conn, "/tmp", "entity", 0, nil)
```

### DB Structure

```sql
create table files.entity
(
    id       int auto_increment     primary key,
    name     varchar(255)           not null,
    folder   int                    not null,
    content  varchar(32) default '' not null,
    type     tinyint                not null,
    modified datetime               not null,
    size     int         default 0  not null,
    tree     int                    not null,
    path     varchar(2048)          not null
);

create index entity_path_index
    on files.entity (path);


```

### License 

MIT