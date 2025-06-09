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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/wildcatdb/wildcat/v2"
)

// Order is an e-commerce order
type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	ProductID  string    `json:"product_id"`
	Quantity   int       `json:"quantity"`
	Price      float64   `json:"price"`
	Status     string    `json:"status"`
	CreatedAt  time.Time `json:"created_at"`
}

// Customer is a customer
type Customer struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	// Change to a suitable directory for your environment as this
	// sample is written for Linux environments mainly.
	dbDir := "/tmp/wildcat_ecommerce_example"

	_ = os.RemoveAll(dbDir)

	opts := &wildcat.Options{
		Directory:                dbDir,
		WriteBufferSize:          32 * 1024 * 1024,
		SyncOption:               wildcat.SyncPartial,
		SyncInterval:             100 * time.Nanosecond,
		BloomFilter:              true,
		BloomFilterFPR:           0.01,
		MaxCompactionConcurrency: 2,
		CompactionCooldownPeriod: 3 * time.Second,
		STDOutLogging:            true,
	}

	db, err := wildcat.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *wildcat.DB) {
		_ = db.Close()
	}(db)

	fmt.Println("Wildcat Basic E-Commerce Sample")

	fmt.Println("\nBasic CRUD Operations")
	demonstrateBasicOperations(db)

	fmt.Println("\nConcurrent MVCC Transactions")
	demonstrateConcurrentTransactions(db)

	fmt.Println("\nIterator Types (Full, Range, Prefix)")
	demonstrateIterators(db)

	fmt.Println("\nBatch Operations")
	demonstrateBatchOperations(db)

	fmt.Println("\nExample 5: Transaction Recovery")
	demonstrateTransactionRecovery(db, opts)

	fmt.Println("\nExample 6: Database Statistics")
	stats := db.Stats()
	fmt.Println(stats)

	fmt.Println("\nAll examples completed successfully!")
}

func demonstrateBasicOperations(db *wildcat.DB) {
	customers := []Customer{
		{ID: "cust_001", Name: "Alice Johnson", Email: "alice@example.com"},
		{ID: "cust_002", Name: "Bob Smith", Email: "bob@example.com"},
		{ID: "cust_003", Name: "Carol Davis", Email: "carol@example.com"},
	}

	// Store customers using Update (auto-managed transactions)
	for _, customer := range customers {
		err := db.Update(func(txn *wildcat.Txn) error {
			data, _ := json.Marshal(customer)
			key := fmt.Sprintf("customer:%s", customer.ID)
			return txn.Put([]byte(key), data)
		})
		if err != nil {
			log.Printf("Failed to store customer %s: %v", customer.ID, err)
		}
	}

	// Read customer back
	var retrievedCustomer Customer
	err := db.View(func(txn *wildcat.Txn) error {
		data, err := txn.Get([]byte("customer:cust_001"))
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &retrievedCustomer)
	})

	if err != nil {
		log.Printf("Failed to retrieve customer: %v", err)
	} else {
		fmt.Printf("Retrieved customer: %s (%s)\n", retrievedCustomer.Name, retrievedCustomer.Email)
	}

	// Update customer email (demonstrating MVCC versioning..)
	err = db.Update(func(txn *wildcat.Txn) error {
		data, err := txn.Get([]byte("customer:cust_001"))
		if err != nil {
			return err
		}

		var customer Customer
		if err := json.Unmarshal(data, &customer); err != nil {
			return err
		}

		// Update email
		customer.Email = "alice.johnson@newdomain.com"
		updatedData, _ := json.Marshal(customer)

		return txn.Put([]byte("customer:cust_001"), updatedData)
	})

	if err != nil {
		log.Printf("Failed to update customer: %v", err)
	} else {
		fmt.Println("Customer email updated successfully")
	}
}

func demonstrateConcurrentTransactions(db *wildcat.DB) {
	var wg sync.WaitGroup
	orderCount := 10

	fmt.Printf("Processing %d concurrent orders...\n", orderCount)

	for i := 0; i < orderCount; i++ {
		wg.Add(1)
		go func(orderNum int) {
			defer wg.Done()

			// Create order with manual transaction management
			txn, err := db.Begin()
			if err != nil {
				log.Printf("Failed to begin transaction for order %d: %v", orderNum, err)
				return
			}

			order := Order{
				ID:         fmt.Sprintf("order_%03d", orderNum),
				CustomerID: fmt.Sprintf("cust_%03d", (orderNum%3)+1),
				ProductID:  fmt.Sprintf("prod_%03d", (orderNum%5)+1),
				Quantity:   orderNum%10 + 1,
				Price:      float64(orderNum*10 + 50),
				Status:     "pending",
				CreatedAt:  time.Now(),
			}

			orderData, _ := json.Marshal(order)
			orderKey := fmt.Sprintf("order:%s", order.ID)

			// Put order
			if err := txn.Put([]byte(orderKey), orderData); err != nil {
				txn.Rollback()
				log.Printf("Failed to put order %d: %v", orderNum, err)
				return
			}

			// Create an index entry for customer orders
			customerOrderKey := fmt.Sprintf("customer_orders:%s:%s", order.CustomerID, order.ID)
			if err := txn.Put([]byte(customerOrderKey), []byte(order.ID)); err != nil {
				txn.Rollback()
				log.Printf("Failed to create customer index for order %d: %v", orderNum, err)
				return
			}

			// Commit transaction
			if err := txn.Commit(); err != nil {
				log.Printf("Failed to commit order %d: %v", orderNum, err)
				return
			}

			fmt.Printf("✓ Order %s processed by goroutine %d\n", order.ID, orderNum)
		}(i)
	}

	wg.Wait()
	fmt.Printf("All %d orders processed concurrently!\n", orderCount)
}

func demonstrateIterators(db *wildcat.DB) {
	fmt.Println("1. Full Iterator (all keys in ascending order):")

	err := db.View(func(txn *wildcat.Txn) error {
		iter, err := txn.NewIterator(true) // ascending
		if err != nil {
			return err
		}

		count := 0
		for {
			key, value, timestamp, ok := iter.Next()
			if !ok {
				break
			}

			if count < 5 {
				fmt.Printf("  %s -> %d bytes (TS: %d)\n", string(key), len(value), timestamp)
			}
			count++
		}
		fmt.Printf("  ... (total %d keys)\n", count)
		return nil
	})

	if err != nil {
		log.Printf("Full iterator error: %v", err)
	}

	fmt.Println("\n2. Range Iterator (customer keys only):")

	err = db.View(func(txn *wildcat.Txn) error {
		// Range from "customer:" to "customer;" (next ASCII char after ':')
		iter, err := txn.NewRangeIterator([]byte("customer:"), []byte("customer;"), true)
		if err != nil {
			return err
		}

		for {
			key, value, timestamp, ok := iter.Next()
			if !ok {
				break
			}

			var customer Customer
			if err := json.Unmarshal(value, &customer); err == nil {
				fmt.Printf("  %s -> %s (%s) TS: %d\n", string(key), customer.Name, customer.Email, timestamp)
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("Range iterator error: %v", err)
	}

	fmt.Println("\n3. Prefix Iterator (customer_orders for cust_001):")

	err = db.View(func(txn *wildcat.Txn) error {
		iter, err := txn.NewPrefixIterator([]byte("customer_orders:cust_001:"), true)
		if err != nil {
			return err
		}

		for {
			key, value, timestamp, ok := iter.Next()
			if !ok {
				break
			}

			fmt.Printf("  %s -> %s (TS: %d)\n", string(key), string(value), timestamp)
		}
		return nil
	})

	if err != nil {
		log.Printf("Prefix iterator error: %v", err)
	}

	fmt.Println("\n4. Bidirectional Iterator (reverse through orders):")

	err = db.View(func(txn *wildcat.Txn) error {
		iter, err := txn.NewPrefixIterator([]byte("order:"), false) // descending
		if err != nil {
			return err
		}

		count := 0
		for {
			key, _, timestamp, ok := iter.Next()
			if !ok {
				break
			}

			if count < 3 { // Show first 3 in reverse order
				fmt.Printf("  %s (TS: %d)\n", string(key), timestamp)
			}
			count++
		}

		if count > 3 {
			fmt.Printf("  ... (%d more orders)\n", count-3)
		}
		return nil
	})

	if err != nil {
		log.Printf("Bidirectional iterator error: %v", err)
	}
}

func demonstrateBatchOperations(db *wildcat.DB) {
	batchSize := 100
	fmt.Printf("Inserting %d products in batch...\n", batchSize)

	start := time.Now()

	// Using Update with multiple operations, this is not the most optimized way, it's better to
	// manage transactions manually for large batches, but this is a simple example.
	err := db.Update(func(txn *wildcat.Txn) error {
		for i := 0; i < batchSize; i++ {
			product := map[string]interface{}{
				"id":         fmt.Sprintf("prod_%03d", i+1),
				"name":       fmt.Sprintf("Product %d", i+1),
				"price":      float64((i+1)*10 + 99),
				"category":   []string{"electronics", "gadgets", "accessories"}[i%3],
				"in_stock":   true,
				"created_at": time.Now().Unix(),
			}

			productData, _ := json.Marshal(product)
			key := fmt.Sprintf("product:prod_%03d", i+1)

			if err := txn.Put([]byte(key), productData); err != nil {
				return err
			}
		}
		return nil
	})

	duration := time.Since(start)

	if err != nil {
		log.Printf("Batch operation failed: %v", err)
	} else {
		fmt.Printf("✓ Batch insert completed in %v (%.2f ops/sec)\n",
			duration, float64(batchSize)/duration.Seconds())
	}

	// Force flush to see memtable behavior
	fmt.Println("Forcing flush to convert memtable to SSTables...")
	if err := db.ForceFlush(); err != nil {
		log.Printf("Force flush failed: %v", err)
	} else {
		fmt.Println("✓ Flush completed")
	}
}

func demonstrateTransactionRecovery(db *wildcat.DB, opts *wildcat.Options) {
	fmt.Println("Creating an incomplete transaction for recovery demo...")

	// Begin a transaction but don't commit it
	txn, err := db.Begin()
	if err != nil {
		log.Printf("Failed to begin transaction: %v", err)
		return
	}

	// Add some data to the transaction
	testData := map[string]string{
		"recovery_test:key1": "value1",
		"recovery_test:key2": "value2",
		"recovery_test:key3": "value3",
	}

	for key, value := range testData {
		if err := txn.Put([]byte(key), []byte(value)); err != nil {
			log.Printf("Failed to put %s: %v", key, err)
			txn.Rollback()
			return
		}
	}

	// Get the transaction ID before we potentially lose the reference
	txnID := txn.Id
	fmt.Printf("Created transaction ID: %d\n", txnID)

	// We close and reopen the database to simulate a crash
	_ = db.Close()

	// Reopen the database to simulate a crash recovery
	db, err = wildcat.Open(opts)
	if err != nil {
		log.Printf("Failed to reopen database: %v\n", err)
		return
	}

	// Try to recover the transaction
	recoveredTxn, err := db.GetTxn(txnID)
	if err != nil {
		log.Printf("Failed to recover transaction %d: %v", txnID, err)
	} else {
		fmt.Printf("✓ Recovered transaction %d\n", recoveredTxn.Id)
		fmt.Printf("  Committed: %v\n", recoveredTxn.Committed)
		fmt.Printf("  Write set size: %d\n", len(recoveredTxn.WriteSet))
		fmt.Printf("  Delete set size: %d\n", len(recoveredTxn.DeleteSet))

		// We can now decide to commit or rollback the recovered transaction
		fmt.Println("Committing recovered transaction...")
		if err := recoveredTxn.Commit(); err != nil {
			log.Printf("Failed to commit recovered transaction: %v", err)
		} else {
			fmt.Println("✓ Recovered transaction committed successfully")
		}
	}

	// Verify the data is now available
	err = db.View(func(viewTxn *wildcat.Txn) error {
		for key := range testData {
			if value, err := viewTxn.Get([]byte(key)); err != nil {
				return fmt.Errorf("failed to get %s: %v", key, err)
			} else {
				fmt.Printf("  Verified: %s -> %s\n", key, string(value))
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("Verification failed: %v", err)
	}
}
