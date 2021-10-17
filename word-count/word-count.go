package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type WordCount struct {
	word  string
	count int32
}

var countsChan = make(chan WordCount, 10000000)
var wg sync.WaitGroup
var fileCount int

func main() {

	fmt.Println("Please enter data directory")
	reader := bufio.NewReader(os.Stdin)
	dirPathStr, _ := reader.ReadString('\n')
	//dirPathStr := "/Users/shashank.balchandra/Documents/data"
	start := time.Now().UnixNano()
	dirPathStr = strings.ReplaceAll(dirPathStr, "\n", "")
	directory, _ := os.Open(dirPathStr)
	dirStat, _ := directory.Stat()
	defer directory.Close()

	wordCount(dirStat, dirPathStr)
	end := time.Now().UnixNano()
	duration := end - start
	fmt.Println("Time taken=", duration/1000000)
}

func wordCount(dirStat os.FileInfo, dirPathStr string) {
	if dirStat.IsDir() {
		files, _ := ioutil.ReadDir(dirPathStr)
		fileCount = len(files)
		fmt.Println("Total files=", fileCount)
		filepath.Walk(dirPathStr, ParseFile)
	} else {
		ParseFile(dirPathStr, nil, nil)
	}
	countMap := make(map[string]int32)
	for wc := range countsChan {
		countMap[wc.word] += wc.count
	}
	var wordCountArray []WordCount
	for k, v := range countMap {
		wordCountArray = append(wordCountArray, WordCount{k, v})
	}
	sort.Slice(wordCountArray, func(i, j int) bool {
		return wordCountArray[i].count < wordCountArray[j].count
	})
	fmt.Println("The word count histogram=", len(countMap))
	f, _ := os.Create("/Users/shashank.balchandra/Documents/results")
	w := bufio.NewWriter(f)

	for i, _ := range wordCountArray {
		res := wordCountArray[i].word + "=" + string(wordCountArray[i].count) + "\n"
		w.WriteString(res)
		//fmt.Println(wordCountArray[i].word, "=", wordCountArray[i].count)
	}
	w.Flush()
}

func ParseFile(path string, info os.FileInfo, err error) error {
	fileOrDir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fileOrDir.Close()
	stat, _ := fileOrDir.Stat()
	if stat.IsDir() {
		return nil
	}
	wg.Add(1)
	fileCount = fileCount - 1
	go func(fc int) {
		fmt.Println("File being read:", path)
		defer terminate(fc)
		data, err := ioutil.ReadFile(path)
		if err != nil {
			return
		}
		lines := strings.Split(string(data), "\n")
		countMap := make(map[string]int32)
		for _, line := range lines {
			words := strings.Split(line, " ")
			for _, word := range words {
				if word != "" {
					countMap[word] += 1
				}
			}
		}
		for k, v := range countMap {
			countsChan <- WordCount{k, v}
		}
	}(fileCount)
	return nil
}
func ParseFileSequential(path string, info os.FileInfo, err error) error {
	fileOrDir, _ := os.Open(path)
	stat, _ := fileOrDir.Stat()
	if stat.IsDir() {
		return nil
	}
	fmt.Println("File being read:", path)
	data, _ := ioutil.ReadFile(path)
	lines := strings.Split(string(data), "\n")
	countMap := make(map[string]int32)
	for _, line := range lines {
		words := strings.Split(line, " ")
		for _, word := range words {
			if word != "" {
				countMap[word] += 1
			}
		}
	}
	for k, v := range countMap {
		countsChan <- WordCount{k, v}
	}
	fileCount = fileCount - 1
	if fileCount == 0 {
		close(countsChan)
	}
	return nil
}

func terminate(fc int) {
	wg.Done()
	if fc == 0 {
		// the last file to begin parsing may not be the last file to finish counting
		// the last go routine will wait for all other go routines to close the channel
		wg.Wait()
		fmt.Println("Will close channel now since all files have been read")
		close(countsChan)
	}
}
