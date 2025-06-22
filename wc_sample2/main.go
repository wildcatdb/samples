// MIT License

// Copyright (c) 2025 WildcatDB, Alex Gaetano Padula

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
package main

import (
	"fmt"
	"github.com/wildcatdb/wildcat/v2"
	"os"
	"sync"
)

func main() {
	defer os.RemoveAll("testdb")
	opts := &wildcat.Options{
		Directory:       "testdb",
		STDOutLogging:   true,
		WriteBufferSize: 15780 / 16, // 16 sstables
		BloomFilter:     true,
	}

	db, err := wildcat.Open(opts)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	defer db.Close()

	keyvs := make(map[string]string)
	keyvsLock := &sync.Mutex{}

	n := 1000

	wg := &sync.WaitGroup{}

	routines := 10
	batches := n / routines

	for i := 0; i < routines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := start; j < start+batches; j++ {
				key := fmt.Sprintf("key-%d", j)
				value := fmt.Sprintf("value-%d", j)

				keyvsLock.Lock()
				keyvs[key] = value
				keyvsLock.Unlock()
			}
		}(i * batches)
	}

	wg.Wait()

	for key, value := range keyvs {
		err = db.Update(func(txn *wildcat.Txn) error {
			return txn.Put([]byte(key), []byte(value))
		})
	}
	if err != nil {
		fmt.Println("Error during update:", err.Error())
		os.Exit(1)
	}

	for key, value := range keyvs {
		err = db.View(func(txn *wildcat.Txn) error {
			v, err := txn.Get([]byte(key))
			if err != nil {
				return err
			}
			if string(v) != value {
				return fmt.Errorf("expected %s but got %s for key %s", value, v, key)
			}
			return nil
		})
		if err != nil {
			fmt.Println("Error during view:", err.Error())
			os.Exit(1)
		}
	}

}
