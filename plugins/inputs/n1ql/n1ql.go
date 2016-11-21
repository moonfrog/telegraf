package n1ql

import (
	"bufio"
	"database/sql"
	/*	"encoding/json"*/
	"fmt"
	_ "github.com/couchbase/go_n1ql"
	"github.com/moonfrog/telegraf"
	"github.com/moonfrog/telegraf/internal/caching"
	"github.com/moonfrog/telegraf/plugins/inputs"
	"log"
	"os"
	"strconv"
	/*	"reflect"*/
	"sync"
	"time"
)

type n1ql struct {
	Servers  []string
	Files    string
	Interval int
	cache    *caching.Caching
}

var sampleConfig = `
  servers = ["http://localhost:8091"]
`

func (r *n1ql) SampleConfig() string {
	return sampleConfig
}

func (r *n1ql) Description() string {
	return "Query Stats from one or many n1ql clusters"
}

// Reads stats from all configured clusters. Accumulates stats.
// Returns one of the errors encountered while gathering stats (if any).
func (r *n1ql) Gather(acc telegraf.Accumulator) error {
	if len(r.Servers) == 0 {
		r.gatherServer("http://localhost:8093/", acc)
		return nil
	}

	var wg sync.WaitGroup

	var outerr error

	for _, serv := range r.Servers {
		wg.Add(1)
		go func(serv string) {
			defer wg.Done()
			outerr = r.gatherServer(serv, acc)
		}(serv)
	}

	wg.Wait()

	return outerr
}

func (r *n1ql) gatherServer(addr string, acc telegraf.Accumulator) error {

	queryLines, err := r.readLines(r.Files)
	if err != nil {
		log.Fatal(" Unable to read from file %s, Error %v", r.Files, err)
	}

	n1ql, err := sql.Open("n1ql", addr)
	if err != nil {
		log.Fatal(err)
	}

	err = n1ql.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// Set query parameters
	os.Setenv("n1ql_timeout", "1000s")
	ac := []byte(`[{"user": "admin:Administrator", "pass": "asdasd"}]`)
	os.Setenv("n1ql_creds", string(ac))

	results := make([]interface{}, 0)
	var diffTime int64
	var value float64

	results, diffTime = r.runQuery(n1ql, queryLines)
	dTime := float64(diffTime)

	for i := range results {
		fields := make(map[string]interface{})
		tags := make(map[string]string)
		table := make(map[string]string)

		res := results[i]
		for k, val := range res.(map[string]interface{}) {
			if len(k) >= 3 && k[0:3] == "val" {
				value, err = strconv.ParseFloat(val.(string), 64)
				fields[k] = value / dTime
			} else if k == "type" {
				table[k] = val.(string)
			} else {
				tags[k] = val.(string)
			}
		}
		acc.AddFields(table["type"], fields, tags)
	}
	return nil
}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func (r *n1ql) readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func (r *n1ql) returnValue(pval *interface{}) interface{} {
	switch v := (*pval).(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return true
		} else {
			return false
		}
	case []byte:
		return string(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999")
	default:
		return v
	}
}

func (r *n1ql) runQuery(client *sql.DB, queryLines []string) ([]interface{}, int64) {

	var wg sync.WaitGroup
	var startTime int64
	results := make([]interface{}, 0)

	//get & set start/end time via cache
	tm, found := r.cache.GetKey("startTime")
	if found {
		startTime, _ = strconv.ParseInt(tm, 10, 64)
	} else {
		startTime = time.Now().Add(-120 * time.Second).Unix()
	}
	endTime := time.Now().Add(-60 * time.Second).Unix()

	diffTime := endTime - startTime

	r.cache.SetWithNoExpiration("startTime", strconv.FormatInt(endTime, 10))

	// run queries to get metrics data
	for i, query := range queryLines {
		wg.Add(1)
		go func(i int, query string) {
			defer wg.Done()

			var rows *sql.Rows
			start := time.Now()
			toExec := fmt.Sprintf(query, startTime, endTime)
			fmt.Println(toExec)
			rows, err := client.Query(toExec)
			fmt.Println(time.Now().Sub(start))
			/*		rows, err = n1ql.Query(query, startTime.Unix(), endTime.Unix())*/
			if err != nil {
				log.Fatal("Error Query Line ", err, query, i)
			}
			defer rows.Close()
			cols, err := rows.Columns()
			if err != nil {
				log.Printf("No columns returned %v", err)
				return
			}
			if cols == nil {
				log.Printf("No columns returned")
				return
			}

			vals := make([]interface{}, len(cols))
			for i := 0; i < len(cols); i++ {
				vals[i] = new(interface{})
			}

			for rows.Next() {
				row := make(map[string]interface{})
				err = rows.Scan(vals...)
				if err != nil {
					fmt.Println(err)
					continue
				}
				for i := 0; i < len(vals); i++ {
					row[cols[i]] = r.returnValue(vals[i].(*interface{}))
				}
				results = append(results, row)

			}
			if rows.Err() != nil {
				log.Printf("Error sanning rows %v", err)
			}
		}(i, query)
	}
	wg.Wait()
	return results, diffTime
}

func init() {
	inputs.Add("n1ql", func() telegraf.Input {
		return &n1ql{
			cache: caching.Newcache(),
		}
	})
}
