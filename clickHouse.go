package main

import (
	"database/sql"
	"github.com/mailru/go-clickhouse"
	"log"
	"time"
)

func generateDmpIdArray(size int64) []string {
	res := make([]string, size)

	i := int64(0)
	for i < size {
		res[i] = "61f0c404-5cb3-11e7-907b-a6006ad3dba0"
		i++
	}

	return res
}

func testClickHouseBatch() {
	//schema://user:password@host[:port]/database?
	connect, err := sql.Open("clickhouse", "http://default:password@localhost:8123/default")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		log.Fatal(err)
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS dmp_results2 (
			type_record FixedString(3),
			seg_id int, 
			dmp_ids   Array(UUID),
			action_day   Date,
			action_time  DateTime
		) engine=MergeTree()
		PARTITION BY toYYYYMMDD(action_day)
        ORDER BY action_day;
	`)

	if err != nil {
		log.Fatal(err)
	}

	tx, err := connect.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO dmp_results2 (
			type_record,
			seg_id,
			dmp_ids,
			action_day,
			action_time
		) VALUES (
			?, ?, ?, ?, ?
		)`)

	if err != nil {
		log.Fatal(err)
	}

	tnow := time.Now()

	for i := 0; i < 1; i++ {
		if _, err := stmt.Exec(
			"ads",
			i,
			clickhouse.Array(generateDmpIdArray(100000)),
			clickhouse.Date(tnow),
			tnow,
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	/*
		rows, err := connect.Query(`
			SELECT
				country_code,
				os_id,
				browser_id,
				categories,
				action_day,
				action_time
			FROM
				example`)

		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var (
				country               string
				os, browser           uint8
				categories            []int16
				actionDay, actionTime time.Time
			)
			if err := rows.Scan(
				&country,
				&os,
				&browser,
				&categories,
				&actionDay,
				&actionTime,
			); err != nil {
				log.Fatal(err)
			}
			log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s",
				country, os, browser, categories, actionDay, actionTime,
			)
		}

		ctx := context.Background()
		rows, err := connect.QueryContext(context.WithValue(ctx, clickhouse.QueryID, "dummy-query-id"), `
			SELECT
				country_code,
				os_id,
				browser_id,
				categories,
				action_day,
				action_time
			FROM
				example`)

		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {
			var (
				country               string
				os, browser           uint8
				categories            []int16
				actionDay, actionTime time.Time
			)
			if err := rows.Scan(
				&country,
				&os,
				&browser,
				&categories,
				&actionDay,
				&actionTime,
			); err != nil {
				log.Fatal(err)
			}
			log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s",
				country, os, browser, categories, actionDay, actionTime,
			)
		}*/

}
