### Language 
GO lang

### Wildcat Version
v2.3.6

### Explaination
A sample program that shows you how to write 1000 key-value pairs, creating 16 SSTables concurrently. Essentially, what we do here is know the size of 1000 key-value pairs beforehand and divide by 16 to flush 16 SSTables. We write the key-value pairs concurrently using a goroutine, so 100 key-value pairs are written in parallel to accumulate up to 1000. We also validate at the end of the program that the keys and their values are available.
