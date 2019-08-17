// author : wbelguidoum

// Commande permettant de faire une projection sur un fichier json, comme il permet accessoirement de formatter la sorite suivant une template
// exemple :  go run project_json.go --filename=input.json --columns="NUMTECDOCCLI as id, agence_compte as agence, id_compte as compte, code_banque as banque" --pattern="{{.id}};{{.banque}};{{.agence}};{{.compte}}" > output.csv

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"text/template"
	"time"
)

var filename string
var pattern string
var columns string
var outputTemplate *template.Template
var columnsMap map[string]string
var columnsArray []string

func init() {
	flag.StringVar(&filename, "filename", "", "the name of the target file to be agregated")
	flag.StringVar(&pattern, "pattern", "json",
		"pattern after which the output will be modeled. if pattern is 'json', the output will be in json format... if pattern is 'csv' then the output will be in csv format")
	flag.StringVar(&columns, "columns", "", "list of the selected columns, ex. : column1 as c1, column2, column3 as c3")
	flag.Parse()

	if pattern != "" {
		log.Printf("Parsing out pattern : \"%v\" ...", pattern)
		var err interface{}
		outputTemplate, err = template.New("output").Parse(pattern)
		if err != nil {
			log.Fatalf("Error while parsing pattern : %s", err)
			return
		}
	}

	if columns == "" {
		log.Fatal("the argument --columns is mandatory")
		return
	}

	columnsMap = make(map[string]string)
	columnsArray = make([]string, 0)
	for _, column := range strings.Split(columns, ",") {
		column = strings.TrimSpace(column)
		aliasExists := (strings.Index(column, " as ") != -1)
		if aliasExists {
			fields := strings.Split(column, " as ")
			column = strings.TrimSpace(fields[0])
			alias := strings.TrimSpace(fields[1])
			columnsMap[column] = alias
			columnsArray = append(columnsArray, alias)
		} else {
			columnsMap[column] = column
			columnsArray = append(columnsArray, column)
		}
	}
}

func main() {
	log.Printf("Projection started at : %v", time.Now())

	var scanner *bufio.Scanner
	if filename != "" {
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

	if pattern == "csv" {
		index := 0
		for _, col := range columnsArray {
			if index != 0 {
				fmt.Fprint(writer, ";")
			}
			fmt.Fprint(writer, "\"", col, "\"")
			index++
		}
		fmt.Fprint(writer, "\n")
	}

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
		totalCount++
		process(record, writer)
	}
	writer.Flush()

	log.Printf("Projection ended at : %v  \n", time.Now())
	log.Printf("Lines processed : %v  \n", totalCount)
}

func process(record map[string]string, writer io.Writer) {

	var output map[string]string
	if columns != "" {
		output = make(map[string]string)
		for column, alias := range columnsMap {
			output[alias] = record[column]
		}
	} else {
		output = record
	}

	if pattern == "csv" {
		index := 0
		for _, field := range columnsArray {
			if index != 0 {
				fmt.Fprint(writer, ";")
			}
			fmt.Fprint(writer, "\"", output[field], "\"")
			index++
		}
		fmt.Fprint(writer, "\n")
	} else if pattern == "json" {
		mjson, _ := json.Marshal(output)
		fmt.Fprint(writer, string(mjson), "\n")
	} else {
		var buf bytes.Buffer
		err := outputTemplate.Execute(io.Writer(&buf), output)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprint(writer, buf.String(), "\n")
	}
}
