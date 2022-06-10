package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"strings"
	"sync"
)

var wgRead sync.WaitGroup

type GainsightUser struct {
	persona, userId string
}

func readFile(filePath string, out chan<- string, wg *sync.WaitGroup) {
	log.Printf("Reading file (%s)", filePath)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file %s. Error %+v", filePath, err)
	}
	defer wg.Done()
	out <- string(data)
}

func parseData(data string) []GainsightUser {
	log.Printf("Parsing data...")
	lines := strings.Split(data, "\n")
	log.Printf("Got %d rows", len(lines))
	var users []GainsightUser
	for index, l := range lines {
		parts := strings.Split(l, ",")
		if len(parts) != 2 {
			log.Printf("Wrong format on line %d. Expected <persona>,<userId> Received: %s", index, l)
			continue
		}
		user := GainsightUser{
			userId:  strings.TrimSpace(parts[1]),
			persona: strings.TrimSpace(parts[0]),
		}
		isValid := len(user.userId) > 0 && len(user.persona) > 0 && user.persona != "Persona"
		if isValid {
			users = append(users, user)
		}
	}
	return users
}

func main() {
	dirPath := "../tool-gainsight-csv/csv"
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		log.Fatalf("Failed to read directory %s. Error %+v", dirPath, err)
	}
	var chans = []chan string{}
	for _, f := range files {
		if !f.IsDir() && strings.Contains(f.Name(), "csv") {
			wgRead.Add(1)
			ch := make(chan string, 1)
			chans = append(chans, ch)
			filePath := fmt.Sprintf("%s/%s", dirPath, f.Name())
			go readFile(filePath, ch, &wgRead)
		}
	}

	cases := make([]reflect.SelectCase, len(chans))
	for i, ch := range chans {
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
	}

	var users []GainsightUser
	remaining := len(cases)
	for remaining > 0 {
		i, value, ok := reflect.Select(cases)
		if !ok {
			cases[i].Chan = reflect.ValueOf(nil)
			remaining -= 1
			continue
		}
		fUsers := parseData(value.String())
		users = append(users, fUsers...)
		log.Printf("Read from channel %+v. Got %d valid GainsightUsers", chans[i], len(fUsers))
	}

	log.Printf("Total %d", len(users))
	wgRead.Wait()
}
