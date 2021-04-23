package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
)

func Frequency(words []string) (m map[string]int) {
	m = make(map[string]int)
	for _, word := range words {
		m[word] += 1
	}
	return
}

func main() {
	fmt.Println("Hello")
	dat, err := ioutil.ReadFile("input.txt")
	if err != nil {
		log.Fatal(err)
	}

	text := string(dat)
	text = strings.ToLower(text)
	words := strings.Split(text, " ")
	//fmt.Printf("%T", split)
	mp := Frequency(words[:5])
	fmt.Print(mp)

}
