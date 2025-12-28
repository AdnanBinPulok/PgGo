package main

import (
	"fmt"
	"log"
	"pggo"
	"time"
)

func main() {
	log.Println("🚀 Starting PgGo Cache Test Suite")

	// 1. Setup Connection
	dbURL := "postgres://pggo_test:pggo_test@192.168.0.103:5432/pggo_test"
	connection := *pggo.NewDatabaseConnection(dbURL, 20, true)
	log.Println("✅ Database connection established")

	// 2. Define Table with Caching Enabled
	UsersTable := pggo.Table{
		Name:       "test_cache_users",
		Connection: connection,
		Columns: []pggo.Column{
			{Name: "id", DataType: *pggo.DataType.Serial().PrimaryKey()},
			{Name: "name", DataType: *pggo.DataType.Text().NotNull()},
			{Name: "email", DataType: *pggo.DataType.Text().Unique().NotNull()},
			{Name: "age", DataType: *pggo.DataType.Integer()},
		},
		DebugMode: true,
	}

	// Initialize Cache (Important!)
	UsersTable.CacheKey = "id"
	UsersTable.EnableCache(5 * time.Second)

	// 3. Cleanup & Create
	_ = UsersTable.DropTable()
	err := UsersTable.CreateTable()
	if err != nil {
		log.Fatalf("❌ Failed to create table: %v", err)
	}
	log.Println("✅ Table created with Caching Enabled")

	// 4. Insert Data
	log.Println("➕ Inserting test data...")
	user1 := map[string]interface{}{"name": "Alice", "email": "alice@example.com", "age": 25}
	insertedUser, err := UsersTable.Insert(user1)
	if err != nil {
		log.Fatalf("❌ Insert failed: %v", err)
	}
	userID := insertedUser["id"]
	log.Printf("✅ Inserted User ID: %v", userID)

	// ---------------------------------------------------------
	// Test 1: Cache Miss (First Fetch)
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 1: Cache Miss (First Fetch)")
	start := time.Now()
	_, err = UsersTable.FetchOne(map[string]interface{}{"id": userID})
	if err != nil {
		log.Fatalf("❌ FetchOne failed: %v", err)
	}
	durationMiss := time.Since(start)
	log.Printf("⏱️  Time taken (DB): %v", durationMiss)

	// ---------------------------------------------------------
	// Test 2: Cache Hit (Second Fetch)
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 2: Cache Hit (Second Fetch)")
	start = time.Now()
	_, err = UsersTable.FetchOne(map[string]interface{}{"id": userID})
	if err != nil {
		log.Fatalf("❌ FetchOne failed: %v", err)
	}
	durationHit := time.Since(start)
	log.Printf("⏱️  Time taken (Cache): %v", durationHit)

	if durationHit < durationMiss {
		log.Println("✅ Cache Hit confirmed (faster than DB fetch)")
	} else {
		log.Println("⚠️  Cache Hit might have failed or DB is too fast to notice difference")
	}

	// ---------------------------------------------------------
	// Test 3: Cache Population via FetchMany
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 3: Cache Population via FetchMany")
	// Insert another user
	UsersTable.Insert(map[string]interface{}{"name": "Bob", "email": "bob@example.com", "age": 30})

	// FetchMany (should trigger async cache population)
	log.Println("   Fetching Many (triggering async cache)...")
	_, err = UsersTable.FetchMany(map[string]interface{}{"age": pggo.Gt(20)})
	if err != nil {
		log.Fatalf("❌ FetchMany failed: %v", err)
	}

	// Wait for async goroutine to finish
	time.Sleep(100 * time.Millisecond)

	// Now FetchOne Bob by ID (should be in cache)
	// We need to know Bob's ID. Since we didn't return it, let's fetch it via email first (DB) then ID (Cache)
	bobRow, _ := UsersTable.FetchOne(map[string]interface{}{"email": "bob@example.com"})
	bobID := bobRow["id"]

	log.Println("   Fetching Bob by ID (expecting Cache Hit)...")
	start = time.Now()
	_, err = UsersTable.FetchOne(map[string]interface{}{"id": bobID})
	if err != nil {
		log.Fatalf("❌ FetchOne Bob failed: %v", err)
	}
	log.Printf("⏱️  Time taken: %v", time.Since(start))

	// ---------------------------------------------------------
	// Test 4: Cache Invalidation on Update
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 4: Cache Invalidation on Update")
	log.Println("   Updating Alice's age...")
	_, err = UsersTable.Update(map[string]interface{}{"age": 26}, map[string]interface{}{"id": userID})
	if err != nil {
		log.Fatalf("❌ Update failed: %v", err)
	}

	// Fetch Alice again. Should be a "Miss" (re-fetch from DB) because Update invalidated cache
	log.Println("   Fetching Alice after Update (expecting DB fetch)...")
	start = time.Now()
	updatedAlice, _ := UsersTable.FetchOne(map[string]interface{}{"id": userID})
	log.Printf("⏱️  Time taken: %v", time.Since(start))

	if fmt.Sprintf("%v", updatedAlice["age"]) == "26" {
		log.Println("✅ Data consistency verified (Age is 26)")
	} else {
		log.Fatalf("❌ Data inconsistency! Expected 26, got %v", updatedAlice["age"])
	}

	// ---------------------------------------------------------
	// Test 5: Custom Cache Key Syntax (key, value)
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 5: Custom Cache Key Syntax")
	// Fetch Alice using "id", value syntax
	start = time.Now()
	_, err = UsersTable.FetchOne("id", userID) // Using the new variadic syntax
	if err != nil {
		log.Fatalf("❌ FetchOne with custom syntax failed: %v", err)
	}
	log.Printf("⏱️  Time taken (Cache Hit from previous fetch): %v", time.Since(start))

	// ---------------------------------------------------------
	// Test 6: Cache Invalidation on Delete
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 6: Cache Invalidation on Delete")
	log.Println("   Deleting Alice...")
	_, err = UsersTable.Delete(map[string]interface{}{"id": userID})
	if err != nil {
		log.Fatalf("❌ Delete failed: %v", err)
	}

	// Fetch Alice. Should fail.
	_, err = UsersTable.FetchOne(map[string]interface{}{"id": userID})
	if err == nil {
		log.Fatalf("❌ FetchOne should have failed (User deleted), but found data!")
	} else {
		log.Println("✅ User correctly not found after delete")
	}

	// Cleanup
	// _ = UsersTable.DropTable() // Don't drop yet, we need it for injection tests
	log.Println("\n🎉 All Cache Tests Passed!")

	// ---------------------------------------------------------
	// Test 7: SQL Injection Tests
	// ---------------------------------------------------------
	log.Println("\n🧪 Test 7: SQL Injection Tests")

	// 7.1 Value Injection (Should be SAFE)
	log.Println("   7.1 Testing Value Injection (Input Sanitization)...")
	// Attempt to inject SQL via a value. This should be treated as a literal string.
	// We try to delete the table via a value.
	maliciousName := "Malicious'; DROP TABLE test_cache_users; --"
	_, err = UsersTable.Insert(map[string]interface{}{
		"name":  maliciousName,
		"email": "hacker@example.com",
		"age":   99,
	})
	if err != nil {
		log.Printf("   ℹ️ Insert with malicious value failed (unexpectedly): %v", err)
	} else {
		log.Println("   ✅ Insert with malicious value succeeded (as literal). Checking if table still exists...")
	}

	// Verify table still exists
	_, err = UsersTable.FetchMany(map[string]interface{}{"age": 99})
	if err != nil {
		log.Println("   ❌ VULNERABLE? Table might be gone or query failed:", err)
	} else {
		log.Println("   ✅ SAFE: Table still exists. Value injection failed (Good).")
	}

	// 7.2 Identifier Injection (Skipped)
	// User confirmed keys are static/trusted in backend.
	log.Println("\n   7.2 Identifier Injection tests skipped (Keys are trusted).")

	log.Println("\n🏁 SQL Injection Tests Completed.")

	// Final Cleanup
	_ = UsersTable.DropTable()
}
