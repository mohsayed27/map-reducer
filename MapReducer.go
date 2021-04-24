package main

import (
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const N = 5

type SafeCounter struct {
	mu      sync.Mutex
	counter int
}

type SafeFreqMap struct {
	mu    sync.Mutex
	myMap map[string]int
}

func frequency(words []string, ch chan map[string]int, mySafeCounter *SafeCounter, N int) {
	/*
		takes strip of strings (words), then sends the frequency of each str into a channel (ch)

		params
			words : a list of strings
			ch    : takes a map of freq of strs an the end of function
		returns
			null
	*/
	m := make(map[string]int)
	for _, word := range words {
		m[word] += 1
	}
	ch <- m
	mySafeCounter.mu.Lock()
	mySafeCounter.counter += 1
	if mySafeCounter.counter >= N {
		close(ch)
	}
	mySafeCounter.mu.Unlock()
	return
}

func channelProcessing(ch chan map[string]int) {
	/*
		takes channel ch

		params
			ch    : takes a map of freq of strs an the end of function
		returns
			null
	*/
	mainMapStruct := SafeFreqMap{myMap: make(map[string]int)}
	//x := [5]map[string]int{<-ch, <-ch, <-ch, <-ch, <-ch}

	for i := 0; i < N; i++ {
		x := <-ch
		go reducer(&mainMapStruct, x)
	}
	mainMapStruct.mu.Lock()
	freqs := rankByWordCount(mainMapStruct.myMap)
	mainMapStruct.mu.Unlock()
	freqs = sortPairByValue(freqs)
	s := ""
	for _, pair := range freqs {
		s += strings.ReplaceAll(pair.Key, string(13), "") + " : " + strconv.Itoa(pair.Value) + " \n"
	}
	writeString(s)
}

func reducer(mainMapStruct *SafeFreqMap, subMap map[string]int) {

	mainMapStruct.mu.Lock()
	for k, v := range subMap {
		if val, ok := mainMapStruct.myMap[k]; ok {
			mainMapStruct.myMap[k] = val + v
		} else {
			mainMapStruct.myMap[k] = v
		}
	}
	mainMapStruct.mu.Unlock()
}

func writeString(s string) {

	f, err := os.Create("WordCountOutput.txt")
	if err != nil {
		log.Fatal(err)
	}

	_, err = f.WriteString(s)
	if err != nil {
		log.Fatal(err)
	}

	err = f.Close()
	if err != nil {
		log.Fatal(err)
	}
}

type Pair struct {
	/*
		to access the map contents more easily
		for sorting
	*/
	Key   string
	Value int
}

type PairList []Pair // list of pairs

// utilities for sorting Pair
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func sortPairByValue(pl PairList) PairList {

	for j := 0; j < pl.Len(); j++ {
		for i := 0; i < pl.Len()-1; i++ {
			if pl[i].Value == pl[i+1].Value {
				if pl[i].Key > pl[i+1].Key {
					pl.Swap(i, i+1)
				}
			}
		}
	}
	return pl

}

func rankByWordCount(wordFrequencies map[string]int) PairList {
	pl := make(PairList, len(wordFrequencies))
	i := 0
	for k, v := range wordFrequencies {
		pl[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

func main() {
	ch := make(chan map[string]int, N)

	dat, err := ioutil.ReadFile("input.txt")
	if err != nil {
		log.Fatal(err)
	}

	text := string(dat)
	text = strings.ToLower(text)
	text = strings.ReplaceAll(text, "\n", " ")
	words := strings.Split(text, " ")

	mySafeCounter := SafeCounter{counter: 0}

	size := float32(len(words))
	for i := 0; i < N; i++ {
		// splitting
		start := size * float32(i) / 5.0
		end := size * float32(i+1) / 5.0
		wordsSlice := words[int(start):int(end)]

		// calling go routine for current split
		go frequency(wordsSlice, ch, &mySafeCounter, N)
	}

	channelProcessing(ch)

}
