package main

import (
	"github.com/gocql/gocql"
	"log"
	"sync"
	"time"
	//go mod vendor"github.com/jmoiron/sqlx"
)

func main() {
	/*
		cluster := gocql.NewCluster("172.17.0.2", "172.17.0.3", "172.17.0.4")
		cluster.Keyspace = "dmp_db"
		//cluster.Timeout = 6600 * time.Millisecond
		cluster.NumConns = 3
		cluster.Consistency = gocql.Quorum
		session, _ := cluster.CreateSession()
		defer session.Close()

		testInserts(session, 10000, 40)
	*/

	//testBatchInserts(session, 2000, 50, 10)
	testClickHouseBatch()
}

func testBatchInserts(session *gocql.Session, quantityBatches, poolSize, batchSize int) {
	//b.WithTimestamp(micros)
	i := 0
	c := make(chan bool, poolSize)
	var wg sync.WaitGroup
	for i < quantityBatches {
		c <- true
		wg.Add(1)
		go func() {
			defer func() {
				<-c
				wg.Done()
			}()

			micros := time.Now().UnixNano()/1e3 - 1000
			b := session.NewBatch(gocql.LoggedBatch)
			b.WithTimestamp(micros)
			uuid, _ := gocql.RandomUUID()

			k := 0
			for k < batchSize {
				b.Query(`INSERT INTO segments (dmp_id, segment_set) VALUES (?, 
{2: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
21: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
22: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
277: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
23: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
24: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
25: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
26: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
27: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
28: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'}
})`, uuid)
				k++
			}

			// safety for concurrency ?
			if err := session.ExecuteBatch(b); err != nil {
				log.Fatal(err)
			}
		}()

		i++
	}

	wg.Wait()

}

func testInserts(session *gocql.Session, quantityRecords, poolSize int) {
	i := 0
	c := make(chan bool, poolSize)
	var wg sync.WaitGroup
	for i < quantityRecords {
		c <- true
		wg.Add(1)
		go func() {
			defer func() {
				<-c
				wg.Done()
			}()

			uuid, _ := gocql.RandomUUID()
			if err := session.Query(`INSERT INTO segments (dmp_id, segment_set) VALUES (?, 
{2: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
21: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
22: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
277: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
23: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
24: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
25: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
26: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
27: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'},
28: {source: '', probability: 1, recency: 1299038700000, expired_at:1299038700000,value: '0'}
})`, uuid).Exec(); err != nil {
				log.Fatal(err)
			}
		}()

		i++
	}

	wg.Wait()

}
