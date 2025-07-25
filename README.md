# JavaBin Solr Integration Test

This integration test verifies the JavaBin generation and upload functionality with Solr 9.9 using Solr's test framework.

## Prerequisites

- Maven 3.6+ 
- Java 17+

## Running the Integration Test

To run the integration test:

```bash
mvn clean verify
```

This will:
1. Compile the project
2. Run unit tests
3. Run the integration test which:
   - Generates a small sample test data file (100 vectors, 768 dimensions)
   - Starts a MiniSolrCloudCluster (embedded Solr instance)
   - Generates JavaBin batch files from the test data
   - Uploads the configset from `src/test/resources/solr-configset/` directory
   - Creates a test collection
   - Uploads the JavaBin batches to Solr
   - Verifies that the correct number of documents were indexed

## Test Configuration

The test is configured to:
- Generate and process 100 test vectors
- Create batches of 25 documents each
- Use vector dimension of 768 (as defined in schema.xml)
- Run using Solr's MiniSolrCloudCluster with 1 node

## Test Framework

The test uses:
- `solr-test-framework`: Solr's official test framework
- `SolrCloudTestCase`: Base class for Solr Cloud tests
- `MiniSolrCloudCluster`: Embedded Solr cluster for testing

## Troubleshooting

If the test fails:
1. Check the test logs for detailed error messages
2. Ensure the configset files exist in `src/test/resources/solr-configset/` directory
3. Check that temporary directories can be created
4. Verify Java 17+ is being used

## Using Real Datasets

### Example: 1M Wikipedia Vectors from NVIDIA

Generate JavaBin files from the 1M Wikipedia vector dataset (768 dimensions):

```bash
mvn clean package
wget https://data.rapids.ai/raft/datasets/wiki_all_1M/wiki_all_1M.tar
tar -xf wiki_all_1M.tar
java -jar target/javabin-generator-1.0-SNAPSHOT-jar-with-dependencies.jar data_file=base.1M.fbin output_dir=wiki_batches batch_size=10000 docs_count=1000000 legacy=true threads=all
```

**Performance**: Using `threads=all` or `threads=4` can improve performance significantly (~80% faster) for large datasets.

### Parameters

- `data_file`: Path to the .fbin/.fvecs input file
- `output_dir`: Directory where JavaBin batch files will be created
- `batch_size`: Number of documents per batch file (default: 1000)
- `docs_count`: Total number of documents to process (default: 10000)
- `legacy`: Use legacy JavaBin format - set to `true` for Solr compatibility (default: false)
- `threads`: Number of parallel threads for processing (default: 1, use `all` for all available processors)
- `overwrite`: Delete existing files in output directory before processing (default: false)

## Customization

To test with different parameters, modify these constants in `JavaBinSolrIT.java`:
- `VECTOR_COUNT`: Number of vectors to generate and process
- `BATCH_SIZE`: Number of documents per batch file
- `VECTOR_DIMENSION`: Dimension of the generated vectors