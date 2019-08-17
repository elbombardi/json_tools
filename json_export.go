// author : wbelguidoum

// Commande qui permet de générer des fichiers selon un template et un fichier de données (json)
package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"text/template"
	"time"
)

var filename string
var templateFilename string
var targetFilename string
var namingPattern string
var logEachCount int

func init() {
	flag.StringVar(&filename, "filename", "", "the name of the data file (each line is in json format) ")
	flag.StringVar(&templateFilename, "template", "", "the name of the template file")
	flag.StringVar(&targetFilename, "target", "", "the name of the target zip file where to put the generated files")
	flag.StringVar(&namingPattern, "naming", "", "the pattern to follow when naming the generated files")
	flag.IntVar(&logEachCount, "logEach", 1000, "show log for each x files")
	flag.Parse()
}

func main() {
	log.Printf("Generating started at : %v", time.Now())

	var scanner *bufio.Scanner
	if filename != "" {
		log.Printf("Opening data file : %v ...", filename)
		dataFile, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Can't open file : %v", err)
			return
		}
		defer dataFile.Close()
		scanner = bufio.NewScanner(dataFile)
	} else {
		scanner = bufio.NewScanner(os.Stdin)
	}
	scanner.Split(bufio.ScanLines)

	log.Printf("Opening template file : %v ...", templateFilename)
	buffer, err := ioutil.ReadFile(templateFilename)
	contentTemplate, err := template.New("content").Parse(string(buffer))
	if err != nil {
		log.Fatalf("Error while parsing content template : %s", err)
		return
	}

	log.Printf("Parsing naming template file : \"%v\" ...", namingPattern)
	namingTemplate, err := template.New("naming").Parse(namingPattern)
	if err != nil {
		log.Fatalf("Error while parsing naming template : %s", err)
		return
	}
	targetWriters := make(map[int]*zip.Writer)

	generatedFilesCount := 0
	count := 0
	tranche := 0
	for scanner.Scan() {
		jsonBlob := scanner.Text()
		if jsonBlob != "" {
			var row map[string]string
			err := json.Unmarshal([]byte(jsonBlob), &row)
			if err != nil {
				log.Fatal(err)
			}
			if _, exists := targetWriters[tranche]; !exists {
				targetFile, err := os.Create(fmt.Sprintf("%v_%v_to_%v.zip", targetFilename, tranche, tranche+logEachCount))
				log.Printf("Creating file : %s \n ", targetFile.Name())
				if err != nil {
					log.Fatal(err)
					return
				}
				defer func() {
					targetWriters[tranche].Flush()
					err = targetWriters[tranche].Close()
					if err != nil {
						log.Fatal(err)
					}
				}()
				targetWriters[tranche] = zip.NewWriter(targetFile)
			}
			process(row, namingTemplate, contentTemplate, targetWriters[tranche], generatedFilesCount)
			generatedFilesCount++
			count++
			if count >= logEachCount {
				targetWriters[tranche].Flush()
				err = targetWriters[tranche].Close()
				if err != nil {
					log.Fatal(err)
				}
				tranche += logEachCount
				count = 0
				log.Printf("Files generated : %v \n", generatedFilesCount)
			}
		}
	}

	log.Printf("Generating ended at : %v  \n", time.Now())
	log.Printf("Files generated : %v \n", generatedFilesCount)
}

func process(row map[string]string,
	namingTemplate *template.Template,
	contentTemplate *template.Template,
	w *zip.Writer,
	lineNum int) {
	var nameBuf bytes.Buffer
	err := namingTemplate.Execute(io.Writer(&nameBuf), row)
	if err != nil {
		log.Fatal(err)
	}
	targetWriter, err := w.Create(nameBuf.String())
	if err != nil {
		log.Fatal(err)
	}

	err = contentTemplate.Execute(targetWriter, row)
	if err != nil {
		log.Fatal(err)
	}
}
