package memsql

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/moonfrog/telegraf"
	"github.com/moonfrog/telegraf/plugins/inputs"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type memsql struct {
	DbUser     string   // Memsql username
	DbPassword string   // Memsql password
	DbHost     []string // Memsql host
	Dbport     string   // Memsql port
	DbName     string   // Memsql database name
	Prot       string   // Memsql connection protocol
	Files      string
	Interval   int
	tableMap   map[string]string
}

var sampleConfig = ``

func (m *memsql) SampleConfig() string {
	return sampleConfig
}

func (m *memsql) Description() string {
	return "Query Stats from one or many memsql clusters"
}

// Reads stats from all configured clusters. Accumulates stats.
// Returns one of the errors encountered while gathering stats (if any).
func (m *memsql) Gather(acc telegraf.Accumulator) error {
	if len(m.DbHost) == 0 {
		m.gatherServer("localhost:8093", acc)
		return nil
	}

	var wg sync.WaitGroup

	var outerr error

	for _, serv := range m.DbHost {
		wg.Add(1)
		go func(serv string) {
			defer wg.Done()
			outerr = m.gatherServer(serv, acc)
		}(serv)
	}

	wg.Wait()

	return outerr
}

func (m *memsql) gatherServer(addr string, acc telegraf.Accumulator) error {

	queryLines, err := m.readLines(m.Files)
	if err != nil {
		log.Fatal(" Unable to read from file %s, Error %v", m.Files, err)
	}

	netAddr := fmt.Sprintf("%s(%s:%s)", m.Prot, addr, m.Dbport)
	dsn := fmt.Sprintf("%s:%s@%s/%s?timeout=30s&strict=true&allowAllFiles=true", m.DbUser, m.DbPassword, netAddr, m.DbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	var value float64
	results := make([]interface{}, 0)
	results = m.runQuery(db, queryLines)

	for i := range results {
		fields := make(map[string]interface{})
		tags := make(map[string]string)
		table := make(map[string]string)

		res := results[i]
		for k, val := range res.(map[string]interface{}) {
			if len(k) >= 3 && k[0:3] == "val" {
				value, err = strconv.ParseFloat(val.(string), 64)
				fields[k] = value
			} else if k == "metric" {
				table[k] = val.(string)
			} else if k == "mode" {
				tags[k] = val.(string)
			} else {
				tags[k] = val.(string)
			}
		}
		acc.AddFields(table["metric"], fields, tags)
	}

	return nil

}

// readLines reads a whole file into memory
// and returns a slice of its lines.
func (m *memsql) readLines(path string) ([]string, error) {
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

func (m *memsql) returnValue(pval *interface{}) interface{} {
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

func (m *memsql) runQuery(client *sql.DB, queryLines []string) []interface{} {

	var wg sync.WaitGroup
	var startTime string
	var endTime string
	results := make([]interface{}, 0)

	startTime = strconv.FormatInt(time.Now().Add(-120*time.Second).Unix(), 10)
	endTime = strconv.FormatInt(time.Now().Add(-60*time.Second).Unix(), 10)

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
					row[cols[i]] = m.returnValue(vals[i].(*interface{}))
				}

				results = append(results, row)

			}
			if rows.Err() != nil {
				log.Printf("Error sanning rows %v", err)
			}
		}(i, query)
	}
	wg.Wait()
	return results
}

func init() {
	inputs.Add("memsql", func() telegraf.Input {
		return &memsql{
			tableMap: make(map[string]string),
		}
	})
}
