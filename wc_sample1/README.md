### Language 
GO lang

### Wildcat Version
v2.1.2

### Explaination
This sample e‑commerce program walks you through how to use Wildcat as the backing store for customers, orders and products in a single, cohesive program. 

First, it shows how to create and persist customer records with JSON serialization, then read them back and update a customer’s email using Wildcat’s `db.Update()` and `db.View()` transaction APIs. 

Next, it spins up ten goroutines that each manually begin their own transaction (via `db.Begin()`), write new order entries alongside customer index records, and commit—all without blocking one another thanks to MVCC. 

Once your data is in place, the program demonstrates four traversal modes - walking every key with a full iterator, scanning only customer records by key range, finding all orders for a given customer via a prefix iterator, and even stepping backwards through orders with a bidirectional iterator. To highlight bulk‑write performance, it then batches 100 product inserts in one transaction, measures ops‑per‑second throughput, and forces an in‑memory flush to durable SSTables. 

The final section simulates a crash by creating an uncommitted transaction with test data, recovers it later by transaction ID and commits it, and finally verifies that recovery properly persisted the data. Throughout, it prints internal Wildcat metrics so you can see how multiple readers and writers coexist without contention.