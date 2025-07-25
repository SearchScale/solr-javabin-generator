# Solr JavaBin Generator

A high-performance tool for generating and uploading JavaBin batch files to Apache Solr for vector search workloads.

## Prerequisites

- Maven 3.6+ 
- Java 17+

## Build

To build the project:

```bash
mvn clean package
```

This creates an executable JAR with all dependencies: `target/javabin-generator-1.0-SNAPSHOT-jar-with-dependencies.jar`

## Usage

### Example: 1M Wikipedia Vectors from NVIDIA

Generate JavaBin files from the 1M Wikipedia vector dataset (768 dimensions):

```bash
mvn clean package
wget https://data.rapids.ai/raft/datasets/wiki_all_1M/wiki_all_1M.tar
tar -xf wiki_all_1M.tar
java -jar target/javabin-generator-1.0-SNAPSHOT-jar-with-dependencies.jar data_file=base.1M.fbin output_dir=wiki_batches batch_size=10000 docs_count=1000000 legacy=true threads=all
```

**Performance**: Using `threads=all` or `threads=4` can improve performance significantly (~80% faster) for large datasets.

## Customization

You can customize the JavaBin generation by modifying the command-line parameters or the source code:

### Command-line Parameters
- `data_file`: Path to the .fbin/.fvecs input file
- `output_dir`: Directory where JavaBin batch files will be created
- `batch_size`: Number of documents per batch file (default: 1000)
- `docs_count`: Total number of documents to process (default: 10000)
- `legacy`: Use legacy JavaBin format - set to `true` for Solr compatibility (default: false)
- `threads`: Number of parallel threads for processing (default: 1, use `all` for all available processors)
- `overwrite`: Delete existing files in output directory before processing (default: false)

## Performance Benchmark

To run a performance benchmark comparing single-threaded vs multi-threaded processing:

**Prerequisites**: Download the 1M Wikipedia dataset first:
```bash
wget https://data.rapids.ai/raft/datasets/wiki_all_1M/wiki_all_1M.tar
tar -xf wiki_all_1M.tar
```

**Run the benchmark**:
```bash
mvn clean package
java -cp target/javabin-generator-1.0-SNAPSHOT-jar-with-dependencies.jar com.searchscale.benchmarks.PerformanceBenchmark file=base.1M.fbin total_docs=50000 batch_size=2500
```
