#!/bin/bash
echo "=== wrk Update Performance Test ==="
echo "Testing document update performance"
echo ""

wrk -t12 -c400 -d30s -s update.lua http://localhost:8080/collections/wrk_test/documents/1
