#!/bin/bash

# Create a simple test to run the Indexer with fbin file
echo "Testing Indexer with base.1M.fbin file..."

# Run with test parameters
java -cp "target/classes:$(find ~/.m2/repository -name '*.jar' | grep -E '(solr-solrj|jackson|slf4j|commons)' | head -20 | tr '\n' ':')" \
  com.searchscale.benchmarks.Indexer \
  data_file=./base.1M.fbin \
  output_file=test_output \
  batch_size=1000 \
  docs_count=5000 \
  legacy=true

echo "Test complete. Check for test_output.* files"
ls -la test_output.*