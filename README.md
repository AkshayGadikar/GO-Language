# GO-Language
Program for parallel reading and querying of text files in GO language 

The emering.go driver takes as command line arguments: the number of readers, the number of askers, an “ask file” (for the askers to draw words from), an “askdelay,” for the time between asks, a directory containing the files for the readers to use, and a “reducedelay” for the time between reduce calls.  These flags are described in emerging.go.

The reader goroutines will read individual words out of a file and send each word to the shared map using AddWord after it is read.  The shared map must keep a count of how many times it has seen each word in a thread-safe manner. The asker goroutines will periodically ask the shared structure for the number of occurrences of a random word in the askfile using GetCount.  The structure cannot add words at the same time it is searching for a word's count. The reducer goroutines will infrequently request a functional reduce from the shared structure.  

INTERFACE DEFINED:

type EmergingMap interface {
	Listen() // start listening for requests
	Stop() // stop listening, exit goroutine

	AddWord(word string) // increase count for given word
	GetCount(word string) int // retrieve count for given word

// reduce over current words
	Reduce(functor ReduceFunc, accum_str string, accum_int int) (string, int)
}

ReduceFunc is defined in emerging.go as:
type ReduceFunc func(key1 string, val1 int, key2 string, val2 int) (string, int)
