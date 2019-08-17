// author : wbelguidoum

// Commande permettant d'applatir deux colonnes clé/valeur en plusieurs champs
// exemple :  go run json_agregate.go --filename=input.json --id=NUMTECDOCCLI --key=CODCLEDOC --value=VALCLEDOC >ouput.json

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var filename string
var keyColumn string
var valueColumn string
var idColumn string

func init() {
	flag.StringVar(&filename, "filename", "", "the name of the target file to be agregated")
	flag.StringVar(&idColumn, "id", "", "the name of the id column used to agregate multiple lines")
	flag.StringVar(&keyColumn, "key", "", "the name of the key column to map with value column")
	flag.StringVar(&valueColumn, "value", "", "the name of the value column associated with the key column")
	flag.Parse()

	if valueColumn == "" || keyColumn == "" || idColumn == "" {
		flag.PrintDefaults()
		log.Fatalf("Please define the name of the id, value and key columns")
	}
}

func main() {
	log.Printf("Agregate started at : %v", time.Now())

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

	records := make([]map[string]string, 0)
	totalCount := 0
	for scanner.Scan() {
		var record map[string]string
		jsonBlob := scanner.Text()
		if jsonBlob != "" {
			err := json.Unmarshal([]byte(jsonBlob), &record)
			if err != nil {
				log.Fatal(err)
			}
		}
		if len(records) > 0 && records[len(records)-1][idColumn] != record[idColumn] {
			process(records, writer)
			totalCount++
			records = make([]map[string]string, 0)
		}
		records = append(records, record)
	}

	if len(records) > 0 {
		process(records, writer)
		totalCount++
	}

	writer.Flush()

	log.Printf("Agregate ended at : %v  \n", time.Now())
	log.Printf("Agregates found : %v \n", totalCount)
}

func indexOf(element string, list []string) int {
	for index, e := range list {
		if e == element {
			return index
		}
	}
	return -1
}

func process(records []map[string]string, writer io.Writer) {
	agregate := make(map[string]string)
	for _, record := range records {
		agregate[record[keyColumn]] = record[valueColumn]
	}
	for key, field := range records[0] {
		if key != keyColumn && key != valueColumn {
			agregate[key] = field
		}
	}
	mjson, _ := json.Marshal(agregate)
	fmt.Fprint(writer, string(mjson), "\n")
}
