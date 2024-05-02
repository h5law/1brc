package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/alphadose/haxmap"
	mmap "github.com/edsrzf/mmap-go"
)

const fileName = "weather.csv"
const mapSize = 1 << 16

// Weather represents the data for a specific station
type Weather struct {
	Min   float64
	Max   float64
	Avg   float64
	Sum   float64
	Count int
}

// Chunk represents the start and end of a chunk of memory in an MMap file
type Chunk struct {
	Start int
	End   int
}

// ProcessRows splits and concurrently processes one billion rows of weather
// data from the 'weather.csv' file in accordance with the 1brc challenge.
func ProcessRows() *haxmap.Map[string, *Weather] {
	// Open the file
	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()

	// Memory map the file
	mappedFile, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer mappedFile.Unmap()

	// Split the memory map into chunks corresponding to the max of the number
	// of cores or max possible goroutines available on the system.
	maxGoroutines := min(runtime.NumCPU(), runtime.GOMAXPROCS(0))
	chunks := splitChunks(&mappedFile, maxGoroutines)
	results := haxmap.New[string, *Weather](mapSize)

	// Process each chunk in a goroutine using haxmap for concurrency safe fast
	// mapping for storing the results, do this for max possible goroutines.
	wg := &sync.WaitGroup{}
	for _, chunk := range chunks {
		wg.Add(1)
		go processChunk(wg, &mappedFile, results, chunk.Start, chunk.End)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Return the results map
	return results
}

// splitChunks splits the memory map into n chunks with their start and end
// indexes aligned around the newline character at the appropriate chunk size
func splitChunks(mp *mmap.MMap, n int) []Chunk {
	l := len(*mp)
	chunkSize := l / n
	chunks := make([]Chunk, n)

	// Initialise the first chunk's start index
	chunks[0].Start = 0

	// Set the start and end index for each chunk
	for i := 1; i < n; i++ {
		// Search for a newline at the end of the chunk size with some delta
		j := chunkSize*i + 50
		for (*mp)[j] != '\n' {
			j--
		}
		// Set the end and start points of the previous and next chunk
		chunks[i-1].End = j
		chunks[i].Start = j + 1
	}

	// Set the end index for the last chunk
	chunks[n-1].End = l - 1

	return chunks
}

// processChunk processes the weather data in the given chunk
func processChunk(
	wg *sync.WaitGroup,
	mp *mmap.MMap,
	results *haxmap.Map[string, *Weather],
	chunkStart int,
	chunkEnd int,
) {
	// Decrement the wait group counter
	defer wg.Done()

	// Create a reusable station name string builder
	stationName := strings.Builder{}

	// Initialise some variables for reuse
	prev := chunkStart
	temp := 0.0

	// Iterate over each chunk byte by byte
	for i := chunkStart; i <= chunkEnd; i++ {
		// Ignore commented first lines
		if (*mp)[i] == '#' {
			for (*mp)[i] != '\n' {
				i++
			}
			prev = i + 1
		}

		// Get the station name
		if (*mp)[i] == ';' {
			stationName.WriteString(string((*mp)[prev:i]))
			i++
			tempStartIdx := i
			for (*mp)[i] != '\n' {
				i++
			}
			t, err := strconv.ParseFloat(string((*mp)[tempStartIdx:i]), 64)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			temp = t

			// Create a weather instance and check if one already exists
			w, ok := results.GetOrSet(
				stationName.String(),
				&Weather{
					Sum:   temp,
					Count: 1,
					Min:   temp,
					Max:   temp,
					Avg:   temp,
				},
			)

			// If found, calculate the new weather values
			if ok {
				w.Sum += temp
				w.Count++
				w.Avg = w.Sum / float64(w.Count)
				w.Min = min(w.Min, temp)
				w.Max = max(w.Max, temp)

				// Insert the new station weather values into the haxmap
				results.Set(stationName.String(), w)
			}

			// Reset variables for reuse
			stationName.Reset()
			prev = i + 1
			temp = 0.0
		}
	}
}
