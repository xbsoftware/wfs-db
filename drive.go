package db

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/xbsoftware/wfs"
	"github.com/jmoiron/sqlx"
)


type DBDrive struct {
	contentFolder string
	rootID int
	table string
	db *sqlx.DB
}

const (
	_ int = iota
	FileRecord
	FolderRecord
)

type DBFile struct {
	ID int

	FileName string `db:"name"`
	FileSize int64 `db:"size"`
	Type int
	Path string
	Content string
	LastModTime time.Time `db:"modified"`

	Folder int
	Tree int
}
func (df *DBFile) Name() string {
	return df.FileName
}
func (df *DBFile) Size() int64 {
	return df.FileSize
}
func (df *DBFile) Mode() os.FileMode {
	return 0
}
func (df *DBFile) ModTime() time.Time {
	return df.LastModTime
}
func (df *DBFile) IsDir() bool {
	return df.Type == FolderRecord
}
func (df *DBFile) Sys() interface{} {
	return df
}

type FileID struct {
	info *DBFile
}
func (f FileID) GetPath() string {
	return f.info.Path
}
func (f FileID) IsFolder() bool {
	return f.info.IsDir()
}
func (f FileID) ClientID() string {
	return f.info.Path
}
func (f FileID) File() *DBFile {
	return f.info
}
func (f FileID) Contains(t wfs.FileID) bool {
	if strings.HasPrefix(f.GetPath(), t.GetPath()+"/"){
		return true
	}
	return false
}


type localFileInfo struct {
	os.FileInfo
	f wfs.FileID
	Files []wfs.FileInfo
}
func (i *localFileInfo) File() wfs.FileID {
	return i.f
}
func (i *localFileInfo) GetChildren() []wfs.FileInfo{
	return i.Files
}
func (i *localFileInfo) SetChildren(v []wfs.FileInfo){
	i.Files = v
}



func NewDBDrive(db *sqlx.DB, folder, table string, root int, config *wfs.DriveConfig) (wfs.Drive, error) {
	d := DBDrive{
		contentFolder: folder,
		table: table,
		rootID: root,
		db: db,
	}

	return wfs.NewDrive(&d, config), nil
}

func (d *DBDrive) Comply(f wfs.FileID, operation int) bool {
	return true
}

func (d *DBDrive) GetParent(f wfs.FileID) wfs.FileID {
	id := f.(FileID).File().Folder

	var p *DBFile
	var err error
	if id != d.rootID {
		p, err = d.newDBFile(id)
	}

	if err != nil || id == d.rootID {
		return FileID{info:&DBFile{ID:d.rootID, Type:FolderRecord, Folder:d.rootID, Path:"/"}}
	}

	return FileID{info:p}
}
func (d *DBDrive) ToFileID(id string) wfs.FileID {
	f, _ := d.newDBFileByPath(id)
	return FileID{f}
}

func (d *DBDrive) Remove(f wfs.FileID) error {
	df := f.(FileID).File()
	// delete main record
	sql := d.db.Rebind(fmt.Sprintf("DELETE FROM %s WHERE id = ? and tree = ? ", d.table))
	_, err := d.db.Exec(sql, df.ID, d.rootID)
	if err != nil {
		return err
	}

	// delete all kids
	sql = d.db.Rebind(fmt.Sprintf("DELETE FROM %s WHERE path LIKE  ? and tree = ? ", d.table))
	_, err = d.db.Exec(sql, df.Path+"/%", d.rootID)
	if err != nil {
		return err
	}

	return nil
}
func (d *DBDrive) Read(f wfs.FileID) (io.ReadSeeker, error) {
	file, err := os.Open(filepath.Join(d.contentFolder, f.(FileID).File().Content))
	if err != nil {
		return nil, errors.New("Can't open file for reading")
	}
	return file, nil
}
func (d *DBDrive) Write(f wfs.FileID, data io.Reader) error {
	file, err := ioutil.TempFile(d.contentFolder, "c")
	if err != nil {
		return errors.New("Can't open file for writing")
	}
	defer file.Close()
	size, err := io.Copy(file, data)
	if err != nil {
		return errors.New("Can't write data")
	}

	sql := d.db.Rebind(fmt.Sprintf("UPDATE %s SET size = ?, content = ?, modified= ? WHERE id = ? and tree = ?", d.table))
	_, err = d.db.Exec(sql, size, filepath.Base(file.Name()), time.Now() ,f.(FileID).File().ID, d.rootID)
	return err
}

func (d *DBDrive) newDBFile(id int) (*DBFile, error) {
	sql := d.db.Rebind(fmt.Sprintf("SELECT id, name, type, content, size, modified, folder, size, path, tree  from %s where id = ?", d.table))
	t := DBFile{}
	err := d.db.Get(&t, sql, id)
	return &t, err
}
func (d *DBDrive) newDBFileByName(id int, name string) (*DBFile, error) {
	sql := d.db.Rebind(fmt.Sprintf("SELECT id, name, type, content, size, modified, folder, size, path, tree  from  %s where folder = ? && name = ?", d.table))
	t := DBFile{}
	err := d.db.Get(&t, sql, id, name)
	return &t, err
}
func (d *DBDrive) newDBFileByPath(path string) (*DBFile, error) {
	sql := d.db.Rebind(fmt.Sprintf("SELECT id, name, type, content, size, modified, folder, size, path, tree from  %s where path = ?", d.table))
	t := DBFile{ ID:d.rootID, Path:"/" }
	err := d.db.Get(&t, sql, path)
	return &t, err
}



func (d *DBDrive) Make(f wfs.FileID, name string, isFolder bool) (wfs.FileID, error) {
	df := f.(FileID)

	newPath := path.Join(df.File().Path, name)
	fileType := FileRecord
	if isFolder {
		fileType = FolderRecord
	}

	sql := d.db.Rebind(fmt.Sprintf("INSERT INTO %s(name, path, folder, modified, size, type, tree) values(?, ?, ?, now(), 0, ?, ?)", d.table))
	res, err := d.db.Exec(sql, name, newPath, df.File().ID, fileType, d.rootID)
	if err != nil {
		return nil, err
	}

	id, _ := res.LastInsertId()
	nf, err := d.newDBFile(int(id))
	if err != nil {
		return nil, err
	}

	return FileID{nf}, nil
}

func (d *DBDrive) Copy(source, target wfs.FileID, name string, isFolder bool) (wfs.FileID, error) {
	df := source.(FileID).File()
	// copy main record
	ins := d.db.Rebind(fmt.Sprintf("INSERT INTO %s(name, folder, content, type, modified, size, tree, path) values(?,?,?,?,?,?,?,?)", d.table))
	full := path.Join(target.GetPath(), name)
	newRec, err := d.db.Exec(ins, name, df.Folder, df.Content, df.Type, df.LastModTime, df.FileSize, df.Tree, full)
	if err != nil {
		return nil, err
	}

	newId, _ := newRec.LastInsertId()

	// update path for all kids
	sql := d.db.Rebind(fmt.Sprintf("SELECT id, name, path, type, modified, size, content FROM %s WHERE folder = ? and tree = ? ", d.table))
	err = d.copyRec(df.ID, int(newId), ins, sql, full)
	if err != nil {
		return nil, err
	}

	info, err := d.newDBFile(int(newId))
	return FileID{info}, err
}

func (d *DBDrive) copyRec(from, to int, ins, sel, full string) error {
	files := make([]DBFile, 0)
	err := d.db.Select(&files, sel, from, d.rootID)
	if err != nil {
		return err
	}

	for _, f := range files {
		fixedPath := path.Join(full, f.FileName)
		newRec, err := d.db.Exec(ins, f.FileName, to, f.Content, f.Type, f.LastModTime, f.FileSize, d.rootID, fixedPath)
		if err != nil {
			return err
		}
		newId, _ := newRec.LastInsertId()

		if f.Type == FolderRecord {
			err = d.copyRec(f.ID, int(newId), ins, sel, fixedPath)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Move renames(moves) a file or a folder
func (d *DBDrive) Move(source, target wfs.FileID, name string, isFolder bool) (wfs.FileID, error) {
	dsf := source.(FileID).File()
	dtf := target.(FileID).File()

	// move main record
	upd := d.db.Rebind(fmt.Sprintf("UPDATE %s SET name = ?, folder = ?, path = ? WHERE id = ?", d.table))
	full := path.Join(target.GetPath(), name)
	_, err := d.db.Exec(upd, name, dtf.ID, full, dsf.ID)
	if err != nil {
		return nil, err
	}

	// update path for all kids
	files := make([]DBFile, 0)
	sql := d.db.Rebind(fmt.Sprintf("SELECT id, path FROM %s WHERE path like ? and tree = ? ", d.table))
	err = d.db.Select(&files, sql, dsf.Path+"/%", d.rootID)
	if err != nil {
		return nil, err
	}

	cupd := d.db.Rebind(fmt.Sprintf("UPDATE %s SET path = ? WHERE id = ?", d.table))
	for _, f := range files {
		d.db.Exec(cupd, strings.Replace(f.Path, source.GetPath(), full, 1), f.ID)
	}

	info, err := d.newDBFile(dsf.ID)
	return FileID{info}, err
}

// Info returns info about a single file
func (d *DBDrive) Info(f wfs.FileID) (wfs.FileInfo, error) {
	return &localFileInfo{f.(FileID).File(), f, nil}, nil
}

func (d *DBDrive) Search(f wfs.FileID, s string) ([]wfs.FileInfo, error) {
	dir := make([]DBFile, 0)
		sql := d.db.Rebind(fmt.Sprintf("SELECT  id, name, type, content, size, modified, folder, size, path, tree  FROM %s WHERE tree = ? AND name like ?", d.table))
	err := d.db.Select(&dir, sql, d.rootID, "%"+s+"%")
	if err != nil {
		return nil, err
	}

	info := make([]wfs.FileInfo, 0, len(dir))
	for i:= range dir {
		info = append(info, &localFileInfo{
			&dir[i],
			FileID{&dir[i]},
			nil,
		})
	}

	return info, nil
}

//
func (d *DBDrive) List(f wfs.FileID) ([]wfs.FileInfo, error) {
	df := f.(FileID)

	dir := make([]DBFile, 0)
		sql := d.db.Rebind(fmt.Sprintf("SELECT  id, name, type, content, size, modified, folder, size, path, tree  FROM %s WHERE folder = ?", d.table))
	err := d.db.Select(&dir, sql, df.File().ID)
	if err != nil {
		return nil, err
	}

	info := make([]wfs.FileInfo, 0, len(dir))
	for i:= range dir {
		info = append(info, &localFileInfo{
			&dir[i],
			FileID{&dir[i]},
			nil,
		})
	}

	return info, nil
}

func (d *DBDrive) Exists(f wfs.FileID, name string) bool {
	df := f.(FileID)
	found, err := d.newDBFileByName(df.File().ID, name)

	if err != nil || found.ID == 0 {
		return false
	}
	return true
}

func (d *DBDrive) Stats() (uint64, uint64, error) {
	var count uint64
	err := d.db.Get(&count, fmt.Sprintf("select COALESCE(sum(size),0) from %s", d.table))
	return count, 0, err
}