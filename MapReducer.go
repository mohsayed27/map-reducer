package main

import (
	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const N = 5

type SafeMap struct {
	mu    sync.Mutex
	sfmap map[string]int
}

func frequency(words []string, ch chan map[string]int) {
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
}

func reducer(ch chan map[string]int, wg *sync.WaitGroup) {
	/*
		takes channel ch
		joins all the maps of the channels
		sorts the values
		outputs the file

		params
			ch    : takes a map of freq of strs [part of data] an the end of function
	*/

	ch2 := make(chan int) // Channel to ensure all threads are done joining
	mainMapStruct := SafeMap{sfmap: make(map[string]int)}

	for i := 0; i < N; i++ {
		x := <-ch
		go mapJoin(&mainMapStruct, x, ch2)
	}

	// Like Semaphor
	// Ensures all the threads have finished before proceeding
	for i := 0; i < N; i++ {
		<-ch2 // Receive Done Signal
	}

	freqs := rankByWordCount(mainMapStruct.sfmap)
	freqs = sortPairByValue(freqs)

	s := ""
	for _, pair := range freqs {
		s += pair.Key + " : " + strconv.Itoa(pair.Value) + " \n"
	}
	writeString(s, "WordCountOutput.txt")

	defer wg.Done()
}

func mapJoin(mainMapStruct *SafeMap, subMap map[string]int, ch2 chan int) {
	/*
		function Joins the threaded freuency map to the main map
		params:
		mainMapStruct : The Map of type SafeMap
	*/

	mainMapStruct.mu.Lock()
	for k, v := range subMap {
		if val, ok := mainMapStruct.sfmap[k]; ok {
			mainMapStruct.sfmap[k] = val + v
		} else {
			mainMapStruct.sfmap[k] = v
		}
	}
	mainMapStruct.mu.Unlock()
	ch2 <- 1 // Send Done Signal
}

func writeString(s, pth string) {
	/*
		fuction writes generated string into required txt file
		params:
		s : required string
		pth : text path
	*/

	f, err := os.Create(pth)
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
	// Bubble Sort
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

func getWordsfromtxt(pth string) []string {
	/*
		functions makes a text file into words

		params
		pth : string contains path to the text file

		return
		a words list (slice), contains all the words in the string
		words is define by (strings separated spaces)
		words is not case sentive
	*/
	file, err := os.Open(pth)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	words := []string{}

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.ToLower(line)
		words = append(words, (strings.Split(line, " "))...)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	file.Close()
	return words
}

func main() {
	var wg sync.WaitGroup
	ch := make(chan map[string]int, 5)

	words := getWordsfromtxt("test.txt")

	size := float32(len(words))
	for i := 0; i < N; i++ {
		// splitting
		start := size * float32(i) / 5.0
		end := size * float32(i+1) / 5.0
		wordsSlice := words[int(start):int(end)]

		// calling go routine for current split
		go frequency(wordsSlice, ch)
	}

	wg.Add(1) // *
	/*
		* it is enough to add the number of reducers(1) to wait for,
		as reducers wait for the frequency threads by default
	*/

	go reducer(ch, &wg)

	wg.Wait()

}
