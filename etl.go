package main

import (
	"compress/bzip2"
	_ "embed"
	"encoding/csv"
	"io"
	"log"
	"os"
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

func main() {
	file, err := os.Open("boston-food.csv.bz2")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	r := csv.NewReader(bzip2.NewReader(file))
	dec, err := csvutil.NewDecoder(r)
	if err != nil {
		log.Fatal(err)
	}
	dec.Register(unmarshalTime)

	db, err := sqlx.Open("sqlite3", "./food.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.MustExec(schemaSQL)

	tx := db.MustBegin()
	lnum := 1
	for {
		lnum++
		var row Row
		err = dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error: %d: %s", lnum, err)
			continue
		}
		tx.NamedExec(insertSQL, &row)
	}
	tx.Commit()
}
