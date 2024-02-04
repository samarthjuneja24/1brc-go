package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func readFile(filePath string, dataChan chan<- []string) {
	const blockSize = 1000000 // Number of lines per chunk, adjust as needed

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		close(dataChan)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var chunk []string

	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			chunk = append(chunk, line)
		}

		if len(chunk) >= blockSize {
			dataChan <- chunk
			chunk = nil // Reset the chunk
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	if len(chunk) > 0 {
		dataChan <- chunk // Send any remaining lines
	}

	close(dataChan)
}

func main() {
	begin := time.Now()
	filePath := "../measurements_100m.txt"
	dataChan := make(chan []string)

	go readFile(filePath, dataChan)

	cityMinTemp := make(map[string]float64)
	cityMaxTemp := make(map[string]float64)
	citySumTemp := make(map[string]float64)
	cityRowCount := make(map[string]float64)

	for chunk := range dataChan {
		for _, cityTemp := range chunk {
			cityTempString := strings.Split(cityTemp, ";")
			city := cityTempString[0]
			temperature, err := strconv.ParseFloat(cityTempString[1], 64)
			if err != nil {
				fmt.Printf("Some error while parsing string to float %v\n", err)
				continue
			}

			// Min, max, sum, and count calculations
			if temp, exists := cityMinTemp[city]; !exists || temperature < temp {
				cityMinTemp[city] = temperature
			}
			if temp, exists := cityMaxTemp[city]; !exists || temperature > temp {
				cityMaxTemp[city] = temperature
			}
			citySumTemp[city] += temperature
			cityRowCount[city] += 1
		}
	}

	fmt.Println("Total time:", time.Since(begin).Seconds())

	// Uncomment to print city statistics
	for city, count := range cityRowCount {
		fmt.Printf("min temp of %s: %v\n", city, cityMinTemp[city])
		fmt.Printf("max temp of %s: %v\n", city, cityMaxTemp[city])
		fmt.Printf("avg temp of %s: %v\n", city, citySumTemp[city]/count)
	}
}
