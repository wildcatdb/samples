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
	dbDir := "/tmp/wc_sample1"

	_ = os.RemoveAll(dbDir)

	opts := &wildcat.Options{
		Directory:              dbDir,
		STDOutLogging:          true,
		RecoverUncommittedTxns: true,
	}

	db, err := wildcat.Open(opts)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer func(db *wildcat.DB) {
		_ = db.Close()
	}(db)

	fmt.Println("Wildcat Basic E-Commerce Sample. Demonstrates Wildcat's API, concurrency, and durability capabilities.")

	fmt.Println("\nTransaction Recovery")
	err = demonstrateTransactionRecovery(&db, opts)
	if err != nil {
		fmt.Printf("Error during transaction recovery: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Println("Transaction recovery completed successfully")
		fmt.Println()

	}

	fmt.Println("\nBasic CRUD Operations")
	err = demonstrateBasicOperations(db)
	if err != nil {
		fmt.Printf("Error during basic operations: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Println("Basic CRUD operations completed successfully")
		fmt.Println()

	}

	fmt.Println("\nConcurrent MVCC Transactions")
	err = demonstrateConcurrentTransactions(db)
	if err != nil {
		fmt.Printf("Error during concurrent transactions: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Println("Concurrent transactions demonstration completed successfully")
		fmt.Println()
	}

	fmt.Println("\nIterator Types (Full, Range, Prefix)")
	err = demonstrateIterators(db)
	if err != nil {
		fmt.Printf("Error during iterator demonstration: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Println("Iterator demonstration completed successfully")
		fmt.Println()
	}

	fmt.Println("\nBatch Operations")
	err = demonstrateBatchOperations(db)
	if err != nil {
		fmt.Printf("Error during batch operations: %v\n", err)
		os.Exit(1)
	} else {
		fmt.Println("Batch operations completed successfully")
		fmt.Println()

	}

	fmt.Println("\nDatabase Statistics")
	stats := db.Stats()
	fmt.Println(stats)

	fmt.Println("\nAll examples/demonstrations completed successfully!")
}

func demonstrateBasicOperations(db *wildcat.DB) error {
	customers := []Customer{
		{ID: "cust_001", Name: "Alice Johnson", Email: "alice@example.com"},
		{ID: "cust_002", Name: "Bob Smith", Email: "bob@example.com"},
		{ID: "cust_003", Name: "Carol Davis", Email: "carol@example.com"},
	}

	// Store customers using Update (auto-managed transactions; no need to begin/commit manually)
	for _, customer := range customers {
		err := db.Update(func(txn *wildcat.Txn) error {
			data, _ := json.Marshal(customer)
			key := fmt.Sprintf("customer:%s", customer.ID)
			return txn.Put([]byte(key), data)
		})
		if err != nil {
			return fmt.Errorf("failed to store customer %s: %v", customer.ID, err)
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
		return fmt.Errorf("failed to retrieve customer: %v", err)
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
		return fmt.Errorf("failed to update customer email: %v", err)
	} else {
		fmt.Println("Customer email updated successfully")
	}

	// Delete a customer
	err = db.Update(func(txn *wildcat.Txn) error {
		if err := txn.Delete([]byte("customer:cust_003")); err != nil {
			return fmt.Errorf("failed to delete customer: %v", err)
		}
		return nil

	})
	if err != nil {
		return fmt.Errorf("failed to delete customer: %v", err)
	} else {
		fmt.Println("Customer cust_003 deleted successfully")
	}

	// Verify deletion
	err = db.View(func(txn *wildcat.Txn) error {
		if _, err := txn.Get([]byte("customer:cust_003")); err == nil {
			return fmt.Errorf("customer cust_003 still exists after deletion")
		}
		fmt.Println("Verified: customer cust_003 does not exist")
		return nil

	})
	if err != nil {
		return fmt.Errorf("verification failed: %v", err)
	} else {
		fmt.Println("Deletion verification successful")
	}

	return nil
}

func demonstrateConcurrentTransactions(db *wildcat.DB) error {
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
				fmt.Printf("Failed to begin transaction for order %d: %v\n", orderNum, err)
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
				_ = txn.Rollback()
				fmt.Printf("Failed to put order %d: %v\n", orderNum, err)
				return
			}

			// Create an index entry for customer orders
			customerOrderKey := fmt.Sprintf("customer_orders:%s:%s", order.CustomerID, order.ID)
			if err := txn.Put([]byte(customerOrderKey), []byte(order.ID)); err != nil {
				_ = txn.Rollback()
				fmt.Printf("Failed to create customer index for order %d: %v\n", orderNum, err)
				return
			}

			// Commit transaction
			if err := txn.Commit(); err != nil {
				fmt.Printf("Failed to commit order %d: %v\n", orderNum, err)
				return
			}

			fmt.Printf("✓ Order %s processed by goroutine %d\n", order.ID, orderNum)
		}(i)
	}

	wg.Wait()

	// Verify all orders were processed
	err := db.View(func(txn *wildcat.Txn) error {
		for i := 0; i < orderCount; i++ {
			orderKey := fmt.Sprintf("order:order_%03d", i)
			if _, err := txn.Get([]byte(orderKey)); err != nil {
				return fmt.Errorf("order %d not found: %v", i, err)
			}
		}
		return nil

	})
	if err != nil {
		fmt.Printf("Error verifying orders: %v\n", err)
		return err

	}

	fmt.Printf("All %d orders processed concurrently!\n", orderCount)

	return nil
}

func demonstrateIterators(db *wildcat.DB) error {
	fmt.Println("Full Iterator (all keys in ascending order):")

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
		return fmt.Errorf("full iterator error: %v", err)
	}

	fmt.Println("\nRange Iterator (customer keys only):")

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
		fmt.Printf("Range iterator error: %v\n", err)
	}

	fmt.Println("\nPrefix Iterator (customer_orders for cust_001):")

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
		fmt.Printf("Prefix iterator error: %v\n", err)
	}

	fmt.Println("\nBidirectional Iterator (reverse through orders):")

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
		return fmt.Errorf("bidirectional iterator error: %v", err)
	}

	return nil
}

func demonstrateBatchOperations(db *wildcat.DB) error {
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
		return fmt.Errorf("batch insert failed: %v", err)
	} else {
		fmt.Printf("✓ Batch insert completed in %v (%.2f ops/sec)\n",
			duration, float64(batchSize)/duration.Seconds())
	}

	// Force flush to see memtable behavior
	fmt.Println("Forcing flush to convert memtable to SSTables...")
	if err := db.ForceFlush(); err != nil {
		fmt.Printf("Force flush failed: %v\n", err)
	} else {
		fmt.Println("✓ Flush completed")
	}

	return nil
}

func demonstrateTransactionRecovery(db **wildcat.DB, opts *wildcat.Options) error {
	fmt.Println("Creating an incomplete transaction for recovery demo...")

	// Begin a transaction but don't commit it
	txn, err := (*db).Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	testData := map[string]string{
		"recovery_test:key1": "value1",
		"recovery_test:key2": "value2",
		"recovery_test:key3": "value3",
	}

	for key, value := range testData {
		if err = txn.Put([]byte(key), []byte(value)); err != nil {
			_ = txn.Rollback()
			return fmt.Errorf("failed to put %s: %v", key, err)
		}
	}

	// Get the transaction ID before we potentially lose the reference
	txnID := txn.Id
	fmt.Printf("Created transaction ID: %d\n", txnID)

	// We close and reopen the database to simulate a crash
	_ = (*db).Close()

	// Reopen the database to simulate a crash recovery
	*db, err = wildcat.Open(opts)
	if err != nil {
		return fmt.Errorf("failed to reopen database: %v", err)
	}

	// Try to recover the transaction
	recoveredTxn, err := (*db).GetTxn(txnID)
	if err != nil {
		return fmt.Errorf("failed to recover transaction %d: %v", txnID, err)
	} else {
		fmt.Printf("✓ Recovered transaction %d\n", recoveredTxn.Id)
		fmt.Printf("  Committed: %v\n", recoveredTxn.Committed)
		fmt.Printf("  Write set size: %d\n", len(recoveredTxn.WriteSet))
		fmt.Printf("  Delete set size: %d\n", len(recoveredTxn.DeleteSet))

		// We can now decide to commit or rollback the recovered transaction
		// In this case, we will commit it
		fmt.Println("Committing recovered transaction...")
		if err := recoveredTxn.Commit(); err != nil {
			return fmt.Errorf("failed to commit recovered transaction: %v", err)
		} else {
			fmt.Println("✓ Recovered transaction committed successfully")
		}
	}

	// Verify the data is now available
	err = (*db).View(func(viewTxn *wildcat.Txn) error {
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
		return fmt.Errorf("verification failed after recovery: %v", err)
	}

	fmt.Println("✓ All data verified after recovery")

	return nil
}
