package main

import "os"
import "fmt"
import "mapreduce"
import (
	"container/list"
	"strings"
	"unicode"
	"regexp"
	"log"
	"strconv"
)

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	wordMap := make(map[string]int)
	list := list.List{}
	words := strings.Fields(value)

	for i := range words {
		if test := unicode.IsLetter(rune(words[i][0])); test {
			if err != nil {
				log.Fatal(err)
			}
			processedWord := reg.ReplaceAllString(words[i], "")
			wordMap[processedWord] = wordMap[processedWord] + 1
		}
	}

	for k, v := range wordMap {
		list.PushBack(mapreduce.KeyValue{k, strconv.Itoa(v)})
	}

	return &list
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	sum := 0
	for e := values.Front(); e != nil; e = e.Next() {
		kv, err := strconv.Atoi(e.Value.(mapreduce.KeyValue).Value)
		if err != nil {
			log.Fatal(err)
		}
		sum += kv
	}
	return strconv.Itoa(sum)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
