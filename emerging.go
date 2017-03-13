package main


import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ReduceFunc func(key1 string, val1 int, key2 string, val2 int) (string,int)

var (
	arg_chan = flag.Bool("chan", false, "use the channel map")
	arg_lock = flag.Bool("lock", false, "use the locking map")

	arg_askers   = flag.Int("askers", 1, "number of asker goroutines")
	arg_askdelay = flag.Int("askdelay", 1000, "the delay in milliseconds for askers")
	arg_reducedelay = flag.Int("reducedelay", 1000, "the delay in milliseconds for reducers")
	arg_askfile  = flag.String("askfile", "data/ask.txt", "the file the askers should query from")

	arg_readers = flag.Int("readers", 4, "number of reader goroutines")
	arg_infiles = flag.String("infiles", "", "comma separated list of files to fill map with")

	totalWords   int64
	totalQueries int64
)

const (
	ADD_BUFFER_SIZE = 32
	ASK_BUFFER_SIZE = 8
)

type EmergingMap interface {
	Listen()
	Stop()

        Reduce(functor ReduceFunc, accum_str string, accum_int int) (string, int)
	AddWord(word string)
	GetCount(word string) int
}

func main() {
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(8)
	flag.Parse()
	fmt.Println("Concurrent Maps")
	fmt.Println("--------------------\n")

	fmt.Println("Creating Map")
	var emap EmergingMap
	if *arg_chan {
		emap = NewChannelMap()
	} else if *arg_lock {
		//emap = NewLockingMap()
	} else {
		fmt.Println("No map type specified... exiting")
		return
	}

	go emap.Listen()

	fmt.Println("Reading query file")
	ask_list := readFile(*arg_askfile)

	var wg sync.WaitGroup

	fmt.Println("Starting Askers")
	ask_kill := make(chan int)
	for i := 0; i < *arg_askers; i++ {
		go asker(ask_list, emap, ask_kill)
	}

	fmt.Println("Starting Readers")
	fill_list := strings.Split(*arg_infiles, ",")
	for i := 0; i < *arg_readers; i++ {
		FN := fill_list[i%16]
		var fn = FN

		wg.Add(1)
		go func() {
			defer wg.Done()
			reader(fn, emap)
		}()
	}

        fmt.Println("Starting Reducer")
        reduce_kill := make(chan int)
        go reducer(max_word, emap, "INVALID", 0, reduce_kill)


	fmt.Println("\nRunning\n")
	wg.Wait()

	fmt.Println("Stopping Askers")
	for i := 0; i < *arg_askers; i++ {
		ask_kill <- 1
	}
        reduce_kill <- 1

	emap.Stop()
	fmt.Println("Map Stopped\n")

	fmt.Println("Total words added:  ", totalWords)
	fmt.Println("Total queries made: ", totalQueries)

}

func reader(filename string, emap EmergingMap) {
	contents, err := ioutil.ReadFile(filename)
	checkPanic(err)

	lines := bytes.Split(contents, []byte("\n"))
	fmt.Println("Reading", len(lines), "lines of", filename)
	for _, line := range lines {
		// if (i+1)%100 == 0 {
		// 	fmt.Println(i, "lines read from", filename)
		// }
		words := bytes.Fields(line)
		for _, word := range words {
			time.Sleep(time.Millisecond)
			emap.AddWord(string(word))
			cnt := atomic.AddInt64(&totalWords, 1)
			if cnt%1000 == 0 {
				fmt.Println(cnt, "words added")
			}
		}
	}
	fmt.Println("Done Reading", filename)
}

func max_word(w1 string, c1 int, w2 string, c2 int) (string, int) {
    if c1 > c2 {
        return w1, c1
    }
    return w2, c2
}

func reducer(functor ReduceFunc, emap EmergingMap, accum_str string, accum_int int, kill chan int) {
    for {
        select {
        case <-time.Tick(time.Millisecond * time.Duration(*arg_reducedelay)):
            word, count := emap.Reduce(functor, accum_str, accum_int)
            fmt.Println("Reduction result:", word, count);
        case <-kill:
            return
        }
    }
}




func asker(word_list []string, emap EmergingMap, kill chan int) {
	L := len(word_list)
	// loop forever, until we get the kill signal
	for {
		select {
		case <-time.Tick(time.Millisecond * time.Duration(*arg_askdelay)):
			pos := rand.Intn(L)
			word := word_list[pos]
			emap.GetCount(word)
			cnt := atomic.AddInt64(&totalQueries, 1)
			if cnt%1000 == 0 {
				fmt.Println(cnt, "queries made")
			}
		case <-kill:
			return
		}
	}
}

func readFile(filename string) []string {
	contents, err := ioutil.ReadFile(filename)
	checkPanic(err)

	word_list := make([]string, 0)

	lines := bytes.Split(contents, []byte("\n"))
	for _, line := range lines {
		words := bytes.Fields(line)
		for _, word := range words {
			word_list = append(word_list, string(word))
		}
	}
	return word_list
}

func checkPanic(err error) {
	if err != nil {
		panic(err)
	}
}
