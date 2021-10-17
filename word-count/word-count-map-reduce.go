
// Author: Habib Rangoonwala
// Created: 04-May-2016
// Updated: 01-July-2016

package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// collector
type MapCollector chan chan interface{}

// mapper
type MapperFunction func(interface{}, chan interface{})

// Reducer
type ReducerFunction func(chan interface{}, chan interface{})


const (
	MaxThreads = 8
)

func myReadFile(filename string) chan string {
	output := make(chan string)
	reg, _ := regexp.Compile("[^A-Za-z0-9]+")

	go func() {
		fmt.Println("Reading file : " + filename + "\n")
		file, err := os.Open(filename)
		if err != nil {
			return
		}
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanWords)

		// Scan all words from the file.
		for scanner.Scan()  {
			//remove all spaces and special chars
			word := strings.TrimSpace(reg.ReplaceAllString(scanner.Text(),""))
			if len(word) > 0 {
				output <- word
			}
		}
		defer file.Close()

		close(output)
		fmt.Println("Completed file : " + filename + "\n")

	}()
	return output
}

func getFiles(dirname string) chan interface{} {
	output := make(chan interface{})
	go func() {
		filepath.Walk(dirname, func(path string, f os.FileInfo, err error) error {
			if !f.IsDir() {
				output <- path
			}
			return nil
		})
		close(output)
	}()
	return output
}


func reducerDispatcher(collector MapCollector, reducerInput chan interface{}) {

	for output := range collector {
		reducerInput <- <-output
	}
	close(reducerInput)
}

func mapperDispatcher(mapper MapperFunction, input chan interface{}, collector MapCollector) {

	for item := range input {
		taskOutput := make(chan interface{})
		go mapper(item, taskOutput)
		collector <- taskOutput

	}
	close(collector)
}


func mapper(filename interface{}, output chan interface{}) {

	results := map[string]int{}

	// read file word-by-word
	for word := range myReadFile(filename.(string)) {

		// stores the result
		results[strings.ToLower(word)] += 1

	}

	output <- results
}

func reducer(input chan interface{}, output chan interface{}) {

	results := map[string]int{}
	for matches := range input {
		for word,frequency := range matches.(map[string]int)  {
			results[strings.ToLower(word)] += frequency
		}
	}
	output <- results
}

func mapReduce(mapper MapperFunction, reducer ReducerFunction, input chan interface{}) interface{} {

	reducerInput := make(chan interface{})
	reducerOutput := make(chan interface{})
	MapCollector := make(MapCollector, MaxThreads)

	go reducer(reducerInput, reducerOutput)
	go reducerDispatcher(MapCollector, reducerInput)
	go mapperDispatcher(mapper, input, MapCollector)

	return <-reducerOutput
}

func main1() {


	starttime := time.Now()

	fmt.Println("Processing . . .")

	// start the enumeration of files to be processed into a channel
	dirPathStr := "/Users/shashank.balchandra/Documents/data"
	input := getFiles(dirPathStr)

	// this will start the map reduce work
	results := mapReduce(mapper, reducer, input)

	// open the output file (processed.csv)
	// this assumes the the Args[1] is directory and there is no file named processed.csv
	filehandle, err := os.Create(dirPathStr+"/processed.csv")
	if  err != nil  {
		fmt.Println("Error writing to file: ", err)
		return
	}
	fmt.Println("Writing to file:" , filehandle.Name())

	defer filehandle.Close()

	writer := bufio.NewWriter(filehandle)

	for word, frequency := range results.(map[string]int) {
		fmt.Fprintln(writer, word+","+strconv.Itoa(frequency))
	}
	writer.Flush()
	filehandle.Close()

	elapsedtime := time.Since(starttime)
	fmt.Println("Complete!!")
	fmt.Println("Time taken:",elapsedtime)

}
