#!/bin/bash
echo "=== wrk Find Performance Test ==="
echo "Testing document find performance"
echo ""

wrk -t12 -c400 -d30s -s find.lua http://localhost:8080/collections/wrk_test/find
