package main

import (
	"compress/bzip2"
	_ "embed"
	"encoding/csv"
	"fmt"
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

func ETL(csvFile io.Reader, tx *sqlx.Tx) (int, int, error) {
	r := csv.NewReader(csvFile)
	dec, err := csvutil.NewDecoder(r)
	if err != nil {
		return 0, 0, err
	}
	dec.Register(unmarshalTime)
	numRecords := 0
	numErrors := 0

	for {
		numRecords++
		var row Row
		err = dec.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("error: %d: %s", numRecords, err)
			numErrors++
			continue
		}
		if _, err := tx.NamedExec(insertSQL, &row); err != nil {
			return 0, 0, err
		}
	}

	return numRecords, numErrors, nil
}

func main() {
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

	if _, err := db.Exec(schemaSQL); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Beginx()
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	numRecords, numErrors, err := ETL(r, tx)
	duration := time.Since(start)
	if err != nil {
		tx.Rollback()
		log.Fatal(err)
	}

	frac := float64(numErrors) / float64(numRecords)
	if frac > 0.1 {
		tx.Rollback()
		log.Fatalf("too many errors: %d/%d = %f", numErrors, numRecords, frac)
	}
	tx.Commit()
	fmt.Printf("%d records (%.2f errors) in %v\n", numRecords, frac, duration)
}
