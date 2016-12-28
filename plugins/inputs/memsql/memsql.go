package memsql

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/moonfrog/go-logs/logs"
	"github.com/moonfrog/telegraf"
	"github.com/moonfrog/telegraf/internal/caching"
	"github.com/moonfrog/telegraf/plugins/inputs"
	"os"
	"strconv"
	"sync"
	"time"
)

type memsql struct {
	DbUser     string   // memsql username
	DbPassword string   // memsql password
	DbHost     []string // memsql host
	Dbport     string   // memsql port
	DbName     string   // memsql database name
	Prot       string   // memsql connection protocol
	Files      string
	Interval   int
	tableMap   map[string]string
	cache      *caching.Caching
	Lag        int64
	Batch      int64
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
		logs.Fatalf(" Unable to read from file %s, Error %v", m.Files, err)
	}

	netAddr := fmt.Sprintf("%s(%s:%s)", m.Prot, addr, m.Dbport)
	dsn := fmt.Sprintf("%s:%s@%s/%s?timeout=30s&strict=true&allowAllFiles=true", m.DbUser, m.DbPassword, netAddr, m.DbName)

	db, err := sql.Open("mysql", dsn)
	defer db.Close()
	if err != nil {
		logs.Fatalf(err)
	}

	var value float64
	results := make([]interface{}, 0)
	results = m.runQueries(db, queryLines)

	for i := range results {
		fields := make(map[string]interface{})
		tags := make(map[string]string)
		table := make(map[string]string)

		res := results[i]
		for k, val := range res.(map[string]interface{}) {
			if len(k) >= 3 && k[0:3] == "val" {
				value, err = strconv.ParseFloat(val.(string), 64)
				fields[k] = value
			} else if k == "metrics" {
				table[k] = val.(string)
			} else if k == "modes" {
				tags[k] = val.(string)
			} else {
				tags[k] = val.(string)
			}
		}
		acc.AddFields(table["metrics"], fields, tags)
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

func (m *memsql) runQueries(client *sql.DB, queryLines []string) []interface{} {

	var wg sync.WaitGroup

	//get & set start/end time via cache
	var startTime string
	var endTime string
	timeNow := time.Now().Unix()
	tm, found := m.cache.GetKey("startTime")
	if found {
		startTime = tm
	} else {
		startTime = fmt.Sprintf("%v", timeNow-(m.Batch+m.Lag))
	}
	endTime = fmt.Sprintf("%v", timeNow-m.Lag)
	m.cache.SetWithNoExpiration("startTime", endTime)

	allResults := make([]*[]interface{}, len(queryLines))

	// run queries to get metrics data
	for i, query := range queryLines {
		wg.Add(1)
		go func(i int, query string) {
			results := make([]interface{}, 0)

			defer wg.Done()

			var rows *sql.Rows
			start := time.Now()
			toExec := fmt.Sprintf(query, startTime, endTime)
			rows, err := client.Query(toExec)
			defer rows.Close()

			logs.Infof(fmt.Sprintf(" Query : [%v] : %v", time.Now().Sub(start), toExec))
			if err != nil {
				logs.Fatalf("Error Query Line ", err, query, i)
			}

			cols, err := rows.Columns()
			if err != nil || cols == nil {
				logs.Errorf("No columns returned %v", err)
				return
			}

			vals := make([]interface{}, len(cols))
			for i := 0; i < len(cols); i++ {
				vals[i] = new(interface{})
			}

			for rows.Next() {
				row := make(map[string]interface{})
				if err := rows.Scan(vals...); err != nil {
					fmt.Println(err)
					continue
				}

				for i := 0; i < len(vals); i++ {
					row[cols[i]] = m.returnValue(vals[i].(*interface{}))
				}

				// TODO :: check is append is thread-safe
				results = append(results, row)

			}
			if rows.Err() != nil {
				logs.Errorf("Error scaning rows %v", err)
			}
			allResults[i] = &results
		}(i, query)
	}
	wg.Wait()

	outResults := make([]interface{}, 0)
	for _, results := range allResults {
		outResults = append(outResults, (*results)...)
	}

	return outResults
}

func init() {
	inputs.Add("memsql", func() telegraf.Input {
		return &memsql{
			tableMap: make(map[string]string),
			cache:    caching.Newcache(),
		}
	})
}
