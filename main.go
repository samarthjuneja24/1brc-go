package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"time"
)

func readFile(filePath string, dataChan chan<- []byte) {
	begin := time.Now()
	const chunkSize = 1048576 // Define the size of each chunk to read. Adjust as necessary.
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		close(dataChan)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var buffer []byte

	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			// Check if adding this line exceeds the chunk size
			if len(buffer)+len(line) > chunkSize {
				// Send the current buffer as a chunk
				dataChan <- append([]byte(nil), buffer...)
				buffer = buffer[:0] // Reset buffer
			}
			buffer = append(buffer, line...)
		}

		if err == io.EOF {
			break // End of file reached
		} else if err != nil {
			fmt.Println("Error reading file:", err)
			break
		}

		// If we've reached the end of a chunk or file, send whatever is in the buffer
		if len(buffer) >= chunkSize {
			dataChan <- append([]byte(nil), buffer...)
			buffer = buffer[:0] // Reset buffer
		}
	}

	// Send any remaining data in the buffer as the last chunk
	if len(buffer) > 0 {
		dataChan <- append([]byte(nil), buffer...)
	}

	close(dataChan)
	fmt.Println("Time taken by readFile:", time.Since(begin).Seconds())
}

func main() {
	f, err := os.Create("trace.out")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	err = trace.Start(f)
	if err != nil {
		panic(err)
	}
	defer trace.Stop()
	begin := time.Now()
	filePath := "../1brc/measurements_100m.txt"
	dataChan := make(chan []byte)

	go readFile(filePath, dataChan)

	cityMinTemp := make(map[string]float64)
	cityMaxTemp := make(map[string]float64)
	citySumTemp := make(map[string]float64)
	cityRowCount := make(map[string]float64)

	for chunk := range dataChan {
		citiesAndTemp := strings.Split(string(chunk), "\n")
		for _, cityTemp := range citiesAndTemp {
			if cityTemp == "" {
				continue
			}
			cityTempString := strings.Split(cityTemp, ";")
			city := cityTempString[0]
			temperature, err := strconv.ParseFloat(cityTempString[1], 32)
			if err != nil {
				fmt.Printf("Some error while parsing string to float %v\n", err)
			}

			//Comparing min
			value, exists := cityMinTemp[city]
			if !exists {
				cityMinTemp[city] = temperature
			} else if temperature < value {
				cityMinTemp[city] = temperature
			}

			//Comparing max
			value, exists = cityMaxTemp[city]
			if !exists {
				cityMaxTemp[city] = temperature
			} else if temperature > value {
				cityMaxTemp[city] = temperature
			}

			//Computing sum per city
			value, exists = citySumTemp[city]
			if !exists {
				citySumTemp[city] = temperature
			} else {
				citySumTemp[city] = citySumTemp[city] + temperature
			}

			//Computing count per city
			countValue, exists := cityRowCount[city]
			if !exists {
				cityRowCount[city] = 1
			} else {
				cityRowCount[city] = countValue + 1
			}
		}
	}
	fmt.Println(time.Since(begin).Seconds())

	//for city, count := range cityRowCount {
	//	fmt.Printf("min temp of %s: %v\n", city, cityMinTemp[city])
	//	fmt.Printf("max temp of %s: %v\n", city, cityMaxTemp[city])
	//	fmt.Printf("avg temp of %s: %v\n", city, citySumTemp[city]/count)
	//}
}
