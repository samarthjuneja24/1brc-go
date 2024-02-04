<h2>Devices tried on: </h2>

| Device              | 100M rows | 1B rows |
|---------------------|-----------|---------|
| M1 Air 8GB 14 inch  | 35s       | 345s    |
| M1 Pro 16GB 16 inch | 21s       | 210s    |

<h2>Approach used</h2>

1. Fetch a chunk of file in a go-routine (tweaked around a bit in the values starting from 1KB to 50MB. Settled on 1MB at a time).
2. Fetch the chunk of data via a channel
3. For loop waiting for channel data, and processing the values

Special thanks to GPT-4 to help me write the code faster. 