package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// User represents the structure of a user document to insert
type User struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Email string `json:"email"`
}

// generateRandomName generates a random 6-letter name
func generateRandomName() string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	name := make([]byte, 6)
	for i := range name {
		name[i] = letters[rand.Intn(len(letters))]
	}
	// Capitalize first letter
	name[0] = name[0] - 32
	return string(name)
}

// generateRandomAge generates a random age between 18 and 99
func generateRandomAge() int {
	return rand.Intn(82) + 18 // 82 possible values (99-18+1) starting from 18
}

// insertUser sends a POST request to insert a user
func insertUser(baseURL string, user User) error {
	userJSON, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user: %w", err)
	}

	resp, err := http.Post(baseURL+"/collections/users/insert", "application/json", bytes.NewBuffer(userJSON))
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func testInsertDocsLoad() {
	// Check command line arguments
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test_scripts/insert_docs_load_test.go <number_of_users> [server_url]")
		fmt.Println("Example: go run test_scripts/insert_docs_load_test.go 1000")
		fmt.Println("Example: go run test_scripts/insert_docs_load_test.go 1000 http://localhost:8080")
		os.Exit(1)
	}

	// Parse number of users to create
	numUsers, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Printf("Error: Invalid number of users '%s'. Please provide a valid integer.\n", os.Args[1])
		os.Exit(1)
	}

	if numUsers <= 0 {
		fmt.Println("Error: Number of users must be greater than 0")
		os.Exit(1)
	}

	// Set server URL (default to localhost:8080)
	serverURL := "http://localhost:8080"
	if len(os.Args) >= 3 {
		serverURL = os.Args[2]
	}

	// Seed random number generator
	rand.Seed(time.Now().UnixNano())

	fmt.Printf("Starting load test: inserting %d users to %s\n", numUsers, serverURL)
	fmt.Println("Press Ctrl+C to stop early")

	// Track timing and statistics
	startTime := time.Now()
	successCount := 0
	errorCount := 0

	// Progress reporting
	reportInterval := max(1, numUsers/10) // Report every 10% or at least every request

	for i := 0; i < numUsers; i++ {
		// Generate random user data
		name := generateRandomName()
		user := User{
			Name:  name,
			Age:   generateRandomAge(),
			Email: fmt.Sprintf("%s@example.com", name),
		}

		// Insert user
		err := insertUser(serverURL, user)
		if err != nil {
			errorCount++
			fmt.Printf("Error inserting user %d (%s): %v\n", i+1, user.Name, err)
		} else {
			successCount++
		}

		// Report progress
		if (i+1)%reportInterval == 0 || i == numUsers-1 {
			elapsed := time.Since(startTime)
			rate := float64(i+1) / elapsed.Seconds()
			fmt.Printf("Progress: %d/%d users (%.1f%%) - Rate: %.1f users/sec - Success: %d, Errors: %d\n",
				i+1, numUsers, float64(i+1)/float64(numUsers)*100, rate, successCount, errorCount)
		}

		// Optional: Add small delay to avoid overwhelming the server
		// Uncomment the next line if you want to add a 1ms delay between requests
		// time.Sleep(1 * time.Millisecond)
	}

	// Final statistics
	totalTime := time.Since(startTime)
	averageRate := float64(numUsers) / totalTime.Seconds()

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("LOAD TEST COMPLETE")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Total users attempted: %d\n", numUsers)
	fmt.Printf("Successful inserts:    %d\n", successCount)
	fmt.Printf("Failed inserts:        %d\n", errorCount)
	fmt.Printf("Success rate:          %.2f%%\n", float64(successCount)/float64(numUsers)*100)
	fmt.Printf("Total time:            %v\n", totalTime)
	fmt.Printf("Average rate:          %.2f users/sec\n", averageRate)
	fmt.Printf("Average time per user: %v\n", totalTime/time.Duration(numUsers))

	if errorCount > 0 {
		fmt.Printf("\nWarning: %d errors occurred during the load test\n", errorCount)
		os.Exit(1)
	}

	fmt.Println("\nLoad test completed successfully!")
}

// max returns the maximum of two integers (for Go versions < 1.21)
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
