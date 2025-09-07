#!/usr/bin/env node

// Simple script to analyze k6 performance comparison results
// Usage: k6 run index-performance-test.js 2>&1 | node analyze-results.js

const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  terminal: false,
});

let results = [];
let currentTest = null;
let dataMismatches = 0;
let documentMismatches = 0;

rl.on('line', (line) => {
  // Pass through all progress and status messages to stdout
  if (
    line.includes('🌱') ||
    line.includes('📊') ||
    line.includes('🏁') ||
    line.includes('✅') ||
    line.includes('❌') ||
    line.includes('Progress:')
  ) {
    console.log(line);
  }

  // Parse performance comparison lines (with validation checkmark)
  const match = line.match(
    /✅ Age (\d+): Indexed=([\d.]+)ms \((\d+) results\), Non-indexed=([\d.]+)ms \((\d+) results\), Speedup=([\d.]+)x/
  );

  if (match) {
    const [
      ,
      age,
      indexedMs,
      indexedResults,
      nonIndexedMs,
      nonIndexedResults,
      speedup,
    ] = match;

    results.push({
      age: parseInt(age),
      indexedMs: parseFloat(indexedMs),
      indexedResults: parseInt(indexedResults),
      nonIndexedMs: parseFloat(nonIndexedMs),
      nonIndexedResults: parseInt(nonIndexedResults),
      speedup: parseFloat(speedup),
    });
  }

  // Track data validation errors
  if (line.includes('❌ DATA MISMATCH:')) {
    dataMismatches++;
  }
  if (line.includes('❌ DOCUMENT MISMATCH:')) {
    documentMismatches++;
  }
});

rl.on('close', () => {
  if (results.length === 0) {
    console.log('No performance comparison data found in input.');
    return;
  }

  console.log('\n=== INDEX PERFORMANCE ANALYSIS ===\n');

  // Data validation summary
  if (dataMismatches > 0 || documentMismatches > 0) {
    console.log('⚠️  DATA VALIDATION ISSUES:');
    if (dataMismatches > 0) {
      console.log(
        `   ${dataMismatches} queries had different result counts between indexed and non-indexed`
      );
    }
    if (documentMismatches > 0) {
      console.log(
        `   ${documentMismatches} queries had document content mismatches`
      );
    }
    console.log('   These queries were excluded from performance analysis\n');
  } else {
    console.log(
      '✅ All queries passed data validation - indexed and non-indexed results match exactly\n'
    );
  }

  // Calculate statistics
  const indexedTimes = results.map((r) => r.indexedMs);
  const nonIndexedTimes = results.map((r) => r.nonIndexedMs);
  const speedups = results.map((r) => r.speedup);

  const avgIndexed =
    indexedTimes.reduce((a, b) => a + b, 0) / indexedTimes.length;
  const avgNonIndexed =
    nonIndexedTimes.reduce((a, b) => a + b, 0) / nonIndexedTimes.length;
  const avgSpeedup = speedups.reduce((a, b) => a + b, 0) / speedups.length;

  const minIndexed = Math.min(...indexedTimes);
  const maxIndexed = Math.max(...indexedTimes);
  const minNonIndexed = Math.min(...nonIndexedTimes);
  const maxNonIndexed = Math.max(...nonIndexedTimes);

  const minSpeedup = Math.min(...speedups);
  const maxSpeedup = Math.max(...speedups);

  // Count wins
  const indexedWins = results.filter((r) => r.speedup > 1).length;
  const nonIndexedWins = results.filter((r) => r.speedup < 1).length;
  const ties = results.filter((r) => r.speedup === 1).length;

  console.log(`📊 SUMMARY STATISTICS:`);
  console.log(`   Total comparisons: ${results.length}`);
  console.log(
    `   Indexed wins: ${indexedWins} (${(
      (indexedWins / results.length) *
      100
    ).toFixed(1)}%)`
  );
  console.log(
    `   Non-indexed wins: ${nonIndexedWins} (${(
      (nonIndexedWins / results.length) *
      100
    ).toFixed(1)}%)`
  );
  console.log(
    `   Ties: ${ties} (${((ties / results.length) * 100).toFixed(1)}%)`
  );
  console.log('');

  console.log(`⚡ PERFORMANCE METRICS:`);
  console.log(`   Average indexed query time: ${avgIndexed.toFixed(2)}ms`);
  console.log(
    `   Average non-indexed query time: ${avgNonIndexed.toFixed(2)}ms`
  );
  console.log(`   Average speedup: ${avgSpeedup.toFixed(2)}x`);
  console.log('');

  console.log(`📈 RANGE ANALYSIS:`);
  console.log(
    `   Indexed time range: ${minIndexed.toFixed(2)}ms - ${maxIndexed.toFixed(
      2
    )}ms`
  );
  console.log(
    `   Non-indexed time range: ${minNonIndexed.toFixed(
      2
    )}ms - ${maxNonIndexed.toFixed(2)}ms`
  );
  console.log(
    `   Speedup range: ${minSpeedup.toFixed(2)}x - ${maxSpeedup.toFixed(2)}x`
  );
  console.log('');

  // Show best and worst cases
  const bestSpeedup = results.reduce((best, current) =>
    current.speedup > best.speedup ? current : best
  );
  const worstSpeedup = results.reduce((worst, current) =>
    current.speedup < worst.speedup ? current : worst
  );

  console.log(`🏆 BEST INDEX PERFORMANCE:`);
  console.log(
    `   Age ${bestSpeedup.age}: ${bestSpeedup.speedup.toFixed(2)}x speedup`
  );
  console.log(
    `   Indexed: ${bestSpeedup.indexedMs.toFixed(2)}ms (${
      bestSpeedup.indexedResults
    } results)`
  );
  console.log(
    `   Non-indexed: ${bestSpeedup.nonIndexedMs.toFixed(2)}ms (${
      bestSpeedup.nonIndexedResults
    } results)`
  );
  console.log('');

  console.log(`🐌 WORST INDEX PERFORMANCE:`);
  console.log(
    `   Age ${worstSpeedup.age}: ${worstSpeedup.speedup.toFixed(2)}x speedup`
  );
  console.log(
    `   Indexed: ${worstSpeedup.indexedMs.toFixed(2)}ms (${
      worstSpeedup.indexedResults
    } results)`
  );
  console.log(
    `   Non-indexed: ${worstSpeedup.nonIndexedMs.toFixed(2)}ms (${
      worstSpeedup.nonIndexedResults
    } results)`
  );
  console.log('');

  // Performance by result count
  const byResultCount = {};
  results.forEach((r) => {
    const count = r.indexedResults;
    if (!byResultCount[count]) {
      byResultCount[count] = [];
    }
    byResultCount[count].push(r.speedup);
  });

  console.log(`📋 PERFORMANCE BY RESULT COUNT:`);
  Object.keys(byResultCount)
    .sort((a, b) => parseInt(a) - parseInt(b))
    .forEach((count) => {
      const speedups = byResultCount[count];
      const avgSpeedup = speedups.reduce((a, b) => a + b, 0) / speedups.length;
      console.log(
        `   ${count} results: ${
          speedups.length
        } queries, avg speedup ${avgSpeedup.toFixed(2)}x`
      );
    });

  console.log('\n=== ANALYSIS COMPLETE ===\n');
});
