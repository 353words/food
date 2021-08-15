package main

import (
	"compress/bzip2"
	_ "embed"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jszwec/csvutil"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed schema.sql
var schemaSQL string

//go:embed insert.sql
var insertSQL string

type Row struct {
	Businessname string    `csv:"businessname" db:"businessname"`
	Issdttm      time.Time `csv:"issdttm" db:"issdttm"`
	Expdttm      time.Time `csv:"expdttm" db:"expdttm"`
	Licstatus    string    `csv:"licstatus" db:"licstatus"`
	Result       string    `csv:"result" db:"result"`
	Resultdttm   time.Time `csv:"resultdttm" db:"resultdttm"`
	Violdesc     string    `csv:"violdesc" db:"violdesc"`
	Violdttm     time.Time `csv:"violdttm" db:"violdttm"`
	Violstatus   string    `csv:"violstatus" db:"violstatus"`
	Comments     string    `csv:"comments" db:"comments"`
	Address      string    `csv:"address" db:"address"`
	City         string    `csv:"city" db:"city"`
	State        string    `csv:"state" db:"state"`
	Zip          string    `csv:"zip" db:"zip"`
	Location     string    `csv:"location" db:"location"`
}

func unmarshalTime(data []byte, t *time.Time) error {
	var err error
	*t, err = time.Parse("2006-01-02 15:04:05", string(data))
	return err
}

func etl(csvFile io.Reader, db *sqlx.DB) error {
	r := csv.NewReader(csvFile)
	dec, err := csvutil.NewDecoder(r)
	if err != nil {
		return err
	}
	dec.Register(unmarshalTime)

	if _, err := db.Exec(schemaSQL); err != nil {
		return err
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}

	lnum := 1
	for {
		lnum++
		var row Row
		err = dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error: %d: %s %s", lnum, strings.Join(dec.Record(), ","), err)
			continue
		}
		if _, err := tx.NamedExec(insertSQL, &row); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func redirectLog(logFile string) error {
	file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	log.SetOutput(file)
	return nil
}

func main() {
	if err := redirectLog("etl.log"); err != nil {
		log.Fatal(err)
	}

	file, err := os.Open("boston-food.csv.bz2")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	r := bzip2.NewReader(file)

	db, err := sqlx.Open("sqlite3", "./food.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := etl(r, db); err != nil {
		log.Fatal(err)
	}
}
