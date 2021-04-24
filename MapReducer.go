package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func Frequency(words []string, ch chan map[string]int) {
	m := make(map[string]int)
	for _, word := range words {
		m[word] += 1
	}
	ch <- m
	return
}

type SafeFreqMap struct {
	mu    sync.Mutex
	myMap map[string]int
}

func addMaps(mainMapStruct *SafeFreqMap, subMap map[string]int) {

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

func reducer(ch chan map[string]int) {
	mainMapStruct := SafeFreqMap{myMap: make(map[string]int)}
	//x := [5]map[string]int{<-ch, <-ch, <-ch, <-ch, <-ch}

	for i := 0; i < 5; i++ {
		x := <-ch
		go addMaps(&mainMapStruct, x)
	}

	//mp := Frequency(words[:5])
	//fmt.Println("out")
	//fmt.Print(mainMapStruct.myMap)

	my_pl := rankByWordCount(mainMapStruct.myMap)
	my_pl = sortPairByValue(my_pl)
	s := ""
	for _, pair := range my_pl {
		if pair.Key == "" {
			continue
		}

		s += strings.ReplaceAll(pair.Key, string(13), "") + " : " + strconv.Itoa(pair.Value) + " \n"
	}
	writeString(s)
}

func writeString(s string) {
	f, err := os.Create("WordCountOutput.txt")
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = f.WriteString(s)
	if err != nil {
		fmt.Println(err)
		f.Close()
		return
	}

	err = f.Close()
	if err != nil {
		fmt.Println(err)
		return

	}
}

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

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func main() {
	ch := make(chan map[string]int, 5)
	//fmt.Println("Hello")
	dat, err := ioutil.ReadFile("input.txt")
	if err != nil {
		log.Fatal(err)
	}

	text := string(dat)
	text = strings.ToLower(text)
	text = strings.ReplaceAll(text, "\n", " ")
	words := strings.Split(text, " ")
	//fmt.Printf("%T", split)
	size := float32(len(words))
	for i := 0; i < 5; i++ {
		one := size * float32(i) / 5.0
		two := size * float32(i+1) / 5.0
		mySlice := words[int(one):int(two)]

		go Frequency(mySlice, ch)
	}

	reducer(ch)

}
