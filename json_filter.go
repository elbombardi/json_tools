// author : wbelguidoum

// Commande permettant de filtrer un fichiers json
// exemple :  go run filter_json.go --filename=input.json --column=NUMTECDOCCLI --operator="=" --value=196680753 >ouput.json

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"time"
)

var filename string
var column string
var operator string
var value string
var valueAsRegexp *regexp.Regexp

func init() {
	flag.StringVar(&filename, "filename", "", "the name of the target file to be agregated")
	flag.StringVar(&column, "column", "", "name of the column to be used")
	flag.StringVar(&operator, "operator", "=", "name of the logical operator used : =, <, >, <=, >=, <>, exists, not_exists, matches")
	flag.StringVar(&value, "value", "", "the value to be used")
	flag.Parse()

	if operator == "matches" {
		log.Printf("Converting regexp : %v ", value)
		valueAsRegexp = regexp.MustCompile(value)
	}
}

func main() {
	log.Printf("Filtering started at : %v", time.Now())

	var scanner *bufio.Scanner
	if filename != "" {
		log.Printf("Opening file : %v ...", filename)
		dataFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Can't open file : %s", err)
			return
		}
		defer dataFile.Close()
		scanner = bufio.NewScanner(dataFile)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
	}
	scanner.Split(bufio.ScanLines)

	writer := bufio.NewWriter(os.Stdout)

	totalCount := 0
	filteredCount := 0
	for scanner.Scan() {
		var record map[string]string
		jsonBlob := scanner.Text()
		if jsonBlob != "" {
			err := json.Unmarshal([]byte(jsonBlob), &record)
			if err != nil {
				log.Fatal(err)
			}
		}
		totalCount++
		if process(record, jsonBlob, writer) {
			filteredCount++
		}
	}
	writer.Flush()

	log.Printf("Filtering ended at : %v  \n", time.Now())
	log.Printf("Result : %v out of %v \n", filteredCount, totalCount)
}

func process(record map[string]string, jsobBlob string, writer io.Writer) bool {
	if accepted(record, column, operator, value) {
		fmt.Fprint(writer, jsobBlob, "\n")
		return true
	}
	return false
}

func accepted(record map[string]string, column string,
	operator string, value string) bool {
	v, ok := record[column]
	switch operator {
	case "matches":
		return ok && valueAsRegexp.MatchString(v)
	case "not_exist":
		return !ok
	case "exist":
		return ok
	case "=":
		return ok && v == value
	case "<>":
		return ok && v != value
	case "<":
		return ok && v < value
	case ">":
		return ok && v > value
	case ">=":
		return ok && v >= value
	case "<=":
		return ok && v <= value
	default:
		log.Fatalf("ERROR : unknow operator '%v'", operator)
	}
	return false
}
