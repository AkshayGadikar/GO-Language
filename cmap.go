package main

import (
	"fmt"
	//"strings"
)

type ReduceStruct struct {
	accum_str string
	accum_int int
}

type MyStruct struct{
	adding  chan string
	asking chan string
	reducing chan ReduceStruct
	reduceTemp chan ReduceStruct
	stop chan bool
	wordCountMap map[string]int
	temp chan int
}


/*This function makes all the required channels */
func NewChannelMap() *MyStruct{
	var cMap MyStruct
	cMap.adding =make(chan string, 32)
	cMap.asking = make(chan string, 8)
	cMap.reducing = make(chan ReduceStruct)
	cMap.reduceTemp = make(chan ReduceStruct)
	cMap.stop = make(chan bool)
	cMap.wordCountMap = make(map[string]int)
	cMap.temp = make(chan int)
	return  &cMap
}

/*Infinite loop that listens all channels*/
func (cMap *MyStruct)Listen(){
	for {
		select {
		case  searchword := <-cMap.asking:
			count := cMap.wordCountMap[searchword]
			cMap.temp <- count

		case word:= <-cMap.adding:
			keyvalue := cMap.wordCountMap[word]
			if keyvalue == 0 {
				cMap.wordCountMap[word] = 1
			}else{
				cMap.wordCountMap[word] = keyvalue + 1
			}

		case reduceParams:= <- cMap.reducing:
			for k, v := range cMap.wordCountMap {
				reduceParams.accum_str = k
				reduceParams.accum_int = v
				break
			}

			/*Iterate over map to find word with lowest word count*/
			for k, v := range cMap.wordCountMap {
				if v <  reduceParams.accum_int {
					reduceParams.accum_str = k
					reduceParams.accum_int = v
				}

			}
			cMap.reduceTemp <- reduceParams
		case <- cMap.stop:
			return
		}
	}
}

/*This function is used to break infinite listen loop*/
func (cMap *MyStruct) Stop(){
	cMap.stop <- true
}

/*This function returns the word/count pair of word having least no of occurances*/
func  (cMap *MyStruct)Reduce(functor ReduceFunc, accum_str string, accum_int int) (string, int){
	res := new(ReduceStruct)
	res.accum_str = accum_str
	res.accum_int = accum_int
	cMap.reducing <- *res

	minPair := <- cMap.reduceTemp

	return  minPair.accum_str,minPair.accum_int
}


/*This function is used to add a word in shared map*/
func (cMap *MyStruct) AddWord(word string){
	cMap.adding <- word
}

/*This function is used get no of occurances of input string in map*/
func (cMap *MyStruct) GetCount(word string) int{
	cMap.asking <- word
	count := <- cMap.temp
	fmt.Println("Word Count for ",word, "is ",count)
	return count
}





