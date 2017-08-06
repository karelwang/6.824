package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.

type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	filePrefix := "mrtmp." + jobName + "-"
	a := make([]int, nMap)
	kvArray := []KeyValue{}

	for i := range a {
		fileName := filePrefix + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskNumber)
		fd, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(fd)
		var kv KeyValue
		for dec.Decode(&kv) == nil {
			kvArray = append(kvArray, kv)
		}
		err = fd.Close()
		if err != nil {
			log.Fatal(err)
		}
	}

	sort.Sort(ByKey(kvArray))

	fmt.Printf("outFile is %s \n", outFile)
	outFd, err := os.Create(outFile)
	if err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(outFd)
	for i := 0; i < len(kvArray); {
		j := i + 1
		for ; j < len(kvArray); j++ {
			if kvArray[j].Key != kvArray[i].Key {
				break
			}
		}

		values := make([]string, j-i)

		for k, n := 0, i; k < len(values); k++ {
			values[k] = kvArray[n].Value
			n++
		}
		res := reduceF(kvArray[i].Key, values)
		enc.Encode(KeyValue{kvArray[i].Key, res})

		i = j
	}

	err = outFd.Close()
	if err != nil {
		log.Fatal(err)
	}
}
