#!/bin/bash
echo "=== wrk Insert Performance Test ==="
echo "Testing single document insert performance"
echo ""

wrk -t12 -c400 -d30s -s insert.lua http://localhost:8080/collections/wrk_test
