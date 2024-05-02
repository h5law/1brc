package main

import (
	"fmt"
	"time"
)

// Execute the program and process the weather data timing the computation
// and then printing the results.
func main() {
	// Measure the time taken to process the rows
	timeStart := time.Now()
	results := ProcessRows()
	timeElapsed := time.Since(timeStart)
	fmt.Printf("Time elapsed: %s\n", timeElapsed)
	fmt.Println("Number of results: ", results.Len())

	// Print the results
	// res := strings.Builder{}
	// results.ForEach(func(k string, v *Weather) bool {
	// 	res.WriteString(fmt.Sprintf("%s: %+v\n", k, v))
	// 	return true
	// })
	// fmt.Println(res.String())
}
