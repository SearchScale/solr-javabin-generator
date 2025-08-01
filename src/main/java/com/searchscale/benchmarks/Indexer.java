package com.searchscale.benchmarks;

import static org.apache.solr.common.util.JavaBinCodec.END;
import static org.apache.solr.common.util.JavaBinCodec.ITERATOR;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;

import org.apache.solr.common.MapWriter;
import org.apache.solr.common.util.JavaBinCodec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Indexer {

  public static Map<String, String> parseStringToMap(String[] pairs) {
    Map<String, String> map = new HashMap<>();

    // Split the input string by commas to get individual key-value pairs

    for (String pair : pairs) {
      // Split each pair by '=' to separate key and value
      String[] keyValue = pair.split("=");

      // Check if the split resulted in exactly two parts
      if (keyValue.length == 2) {
        String key = keyValue[0].trim(); // Remove any potential whitespace
        String value = keyValue[1].trim();
        map.put(key, value);
      } else {
        // Log or handle malformed key-value pairs
        System.err.println("Malformed key-value pair: " + pair);
      }
    }

    return map;
  }

  public static class Params {

    public final int threads;
    public final int totalVectors;

    public final String solrUrl;
    public final String dataFile;
    public final String queryFile;
    public final String testColl;
    public final int batchSize;
    public final int queryCount;
    public final boolean runQuery;
    public final String outputDir;

    public final int docsCount;
    public final boolean overwrite;

    Map<String, String> p;

    public Params(String[] s) {
      p = parseStringToMap(s);
      
      // Handle threads parameter - support "all" for all available processors
      String threadsParam = p.getOrDefault("threads", "1");
      if ("all".equalsIgnoreCase(threadsParam)) {
        threads = Runtime.getRuntime().availableProcessors();
        System.out.println("Using all available processors: " + threads + " threads");
      } else {
        threads = Integer.parseInt(threadsParam);
      }
      
      totalVectors = Integer.parseInt(p.getOrDefault("total_vectors", "0"));
      solrUrl = p.getOrDefault("solr_url", "http://localhost:8983/solr");
      dataFile = p.get("data_file");
      testColl = p.getOrDefault("test_coll", "test");
      batchSize = Integer.parseInt(p.getOrDefault("batch_size", "1000"));
      docsCount = Integer.parseInt(p.getOrDefault("docs_count", "10000"));
      queryFile = p.get("query_file");
      outputDir = p.get("output_dir");

      queryCount = Integer.parseInt(p.getOrDefault("query_count", "1"));
      runQuery = Boolean.parseBoolean(p.getOrDefault("query", "false"));
      overwrite = Boolean.parseBoolean(p.getOrDefault("overwrite", "false"));
    }

  }

  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static final String EOL = "###";

  public static void main(String[] args) throws Exception {
    Params p = new Params(args);
    long docsCount = p.docsCount;
    long batchSz = p.batchSize;
    System.out.println(p.p.toString());
    if (batchSz > docsCount)
      batchSz = docsCount;

    // Handle output directory
    if (p.outputDir == null) {
      System.err.println("Error: output_dir parameter is required");
      System.exit(1);
    }
    
    Path outputPath = Paths.get(p.outputDir);
    if (Files.exists(outputPath)) {
      if (!p.overwrite) {
        System.err.println("Error: Output directory '" + p.outputDir + "' already exists.");
        System.err.println("Use overwrite=true to delete existing files and proceed.");
        System.exit(1);
      } else {
        System.out.println("Overwrite flag set. Cleaning existing directory: " + p.outputDir);
        deleteDirectoryContents(outputPath);
      }
    } else {
      try {
        Files.createDirectories(outputPath);
        System.out.println("Created output directory: " + p.outputDir);
      } catch (IOException e) {
        System.err.println("Error: Failed to create output directory '" + p.outputDir + "': " + e.getMessage());
        System.exit(1);
      }
    }

    // Check if the file is fvec/fbin format
    if (p.dataFile.endsWith(".fvecs") || p.dataFile.endsWith(".fbin") || p.dataFile.endsWith(".fvecs.gz")) {
      processFvecFile(p, docsCount, batchSz);
    } else {
      // Original CSV processing
      try (InputStream in = new GZIPInputStream(new FileInputStream(p.dataFile))) {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String header = br.readLine();
        int count = 0;
        for (int i = 0;; i++) {
          String name = Paths.get(p.outputDir, "batch." + i).toString();
          try (FileOutputStream os = new FileOutputStream(name)) {
            JavaBinCodec codec = new J(os);
            if (!writeBatch(batchSz, br, codec))
              break;
            System.out.println(name);
            count += batchSz;
            if (count > docsCount)
              break;
          }
        }
      }
    }
  }

  private static void deleteDirectoryContents(Path directory) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
      for (Path file : stream) {
        if (Files.isDirectory(file)) {
          deleteDirectoryContents(file);
          Files.delete(file);
        } else {
          Files.delete(file);
        }
      }
    }
  }

  private static void processFvecFile(Params p, long docsCount, long batchSz) throws Exception {
    if (p.threads == 1) {
      processFvecFileSingleThreaded(p, docsCount, batchSz);
    } else {
      processFvecFileMultiThreaded(p, docsCount, batchSz);
    }
  }
  
  private static void processFvecFileSingleThreaded(Params p, long docsCount, long batchSz) throws Exception {
    System.out.println("Using optimized single-threaded processing");
    
    int totalBatches = (int) Math.ceil((double) docsCount / batchSz);
    long totalBytesRead = 0;
    
    for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      final long startIndex = batchIndex * batchSz;
      final long endIndex = Math.min(startIndex + batchSz, docsCount);
      final int batchSize = (int) (endIndex - startIndex);
      
      long bytesRead = processBatch(p, batchIndex, startIndex, batchSize);
      totalBytesRead += bytesRead;
      
      System.out.println("Completed batch " + batchIndex + " (" + (batchIndex + 1) + "/" + totalBatches + ")");
    }
    
    System.out.println("All batches completed successfully!");
    System.out.println("Total documents processed: " + docsCount);
    System.out.println("TOTAL BYTES READ FROM FILE: " + totalBytesRead + " bytes");
  }

  private static void processFvecFileMultiThreaded(Params p, long docsCount, long batchSz) throws Exception {
    System.out.println("Using optimized multi-threaded processing with " + p.threads + " threads");
    
    int totalBatches = (int) Math.ceil((double) docsCount / batchSz);
    
    // Choose optimal approach based on dataset size
    boolean isLargeDataset = docsCount > 1_000_000;
    boolean isHighThreadCount = p.threads > 8;
    
    if (isLargeDataset || isHighThreadCount) {
      // Use conservative approach for large datasets
      processLargeDatasetMultiThreaded(p, docsCount, batchSz, totalBatches);
    } else {
      // Use aggressive approach for smaller datasets
      processSmallDatasetMultiThreaded(p, docsCount, batchSz, totalBatches);
    }
  }
  
  // Optimized processing for large datasets or high thread counts
  private static void processLargeDatasetMultiThreaded(Params p, long docsCount, long batchSz, int totalBatches) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(p.threads);
    AtomicInteger completedBatches = new AtomicInteger(0);
    AtomicLong totalBytesRead = new AtomicLong(0);
    List<Future<Long>> futures = new ArrayList<>();
    
    try {
      // Submit batch processing tasks directly
      for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
        final int finalBatchIndex = batchIndex;
        final long startIndex = batchIndex * batchSz;
        final int batchSize = (int) Math.min(batchSz, docsCount - startIndex);
        
        Future<Long> future = executor.submit(() -> {
          try {
            long bytesRead = processBatch(p, finalBatchIndex, startIndex, batchSize);
            totalBytesRead.addAndGet(bytesRead);
            
            int completed = completedBatches.incrementAndGet();
            System.out.println("Completed batch " + finalBatchIndex + " (" + completed + "/" + totalBatches + 
                             ") - Total processed: " + (completed * batchSz));
            return bytesRead;
          } catch (Exception e) {
            System.err.println("Error processing batch " + finalBatchIndex + ": " + e.getMessage());
            e.printStackTrace();
            return 0L;
          }
        });
        
        futures.add(future);
      }
      
      // Wait for completion
      for (Future<Long> future : futures) {
        future.get();
      }
      
      System.out.println("All batches completed successfully!");
      System.out.println("Total documents processed: " + docsCount);
      System.out.println("TOTAL BYTES READ FROM FILE: " + totalBytesRead.get() + " bytes");
      
    } finally {
      executor.shutdown();
    }
  }
  
  // Optimized processing for smaller datasets with aggressive optimizations
  private static void processSmallDatasetMultiThreaded(Params p, long docsCount, long batchSz, int totalBatches) throws Exception {
    // Use ForkJoinPool for better work distribution on smaller datasets
    ForkJoinPool pool = new ForkJoinPool(p.threads);
    
    ConcurrentLinkedQueue<Integer> workQueue = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < totalBatches; i++) {
      workQueue.offer(i);
    }
    
    AtomicInteger completedBatches = new AtomicInteger(0);
    AtomicLong totalBytesRead = new AtomicLong(0);
    
    try {
      List<Future<?>> futures = new ArrayList<>();
      
      // Submit worker tasks with work-stealing
      for (int t = 0; t < p.threads; t++) {
        futures.add(pool.submit(() -> {
          Integer batchIndex;
          while ((batchIndex = workQueue.poll()) != null) {
            try {
              long startIndex = batchIndex * batchSz;
              int batchSize = (int) Math.min(batchSz, docsCount - startIndex);
              
              long bytesRead = processBatch(p, batchIndex, startIndex, batchSize);
              totalBytesRead.addAndGet(bytesRead);
              
              int completed = completedBatches.incrementAndGet();
              System.out.println("Completed batch " + batchIndex + " (" + completed + "/" + totalBatches + 
                               ") - Total processed: " + (completed * batchSz));
            } catch (Exception e) {
              System.err.println("Error processing batch " + batchIndex + ": " + e.getMessage());
              e.printStackTrace();
            }
          }
        }));
      }
      
      // Wait for completion
      for (Future<?> future : futures) {
        future.get();
      }
      
      System.out.println("All batches completed successfully!");
      System.out.println("Total documents processed: " + docsCount);
      System.out.println("TOTAL BYTES READ FROM FILE: " + totalBytesRead.get() + " bytes");
      
    } finally {
      pool.shutdown();
    }
  }

  private static long processBatch(Params p, int batchIndex, long startIndex, int batchSize) throws Exception {
    String name = Paths.get(p.outputDir, "batch." + batchIndex).toString();
    
    // Use buffered output stream for better I/O performance
    try (FileOutputStream fos = new FileOutputStream(name);
         java.io.BufferedOutputStream bos = new java.io.BufferedOutputStream(fos, 64 * 1024)) {
      
      JavaBinCodec codec = new J(bos);
      codec.writeTag(ITERATOR);
      
      // Pre-allocate reusable list with estimated capacity
      List<Float> reusableFloatList = new ArrayList<>(1024); // Will grow as needed
      
      // Use optimized memory-mapped reading for best performance
      long bytesRead = OptimizedReader.streamFvecsRangeMapped(p.dataFile, (int) startIndex, batchSize, (vector, index) -> {
        try {
          final int docId = (int) (startIndex + index);
          final float[] vectorData = vector;
          
          // Clear and reuse the list for better memory efficiency
          reusableFloatList.clear();
          
          // Ensure capacity if vector is larger than current capacity
          if (reusableFloatList instanceof ArrayList && vectorData.length > ((ArrayList<Float>) reusableFloatList).size()) {
            ((ArrayList<Float>) reusableFloatList).ensureCapacity(vectorData.length);
          }
          
          // Batch add operation - more efficient than individual adds
          for (float f : vectorData) {
            reusableFloatList.add(f);
          }
          
          MapWriter d = ew -> {
            ew.put("id", String.valueOf(docId));
            ew.put("article_vector", reusableFloatList);
          };
          
          codec.writeMap(d);
        } catch (IOException e) {
          throw new RuntimeException("Error writing vector to JavaBin", e);
        }
      });
      
      codec.writeTag(END);
      codec.close();
      System.out.println(name);
      
      return bytesRead;
    }
  }


  private static boolean writeBatch(long docsCount, BufferedReader br, JavaBinCodec codec)
      throws IOException {
    codec.writeTag(ITERATOR);
    int count = 0;
    for (;;) {
      String line = br.readLine();
      if (line == null) {
        System.out.println(EOL);
        return false;
      }
      MapWriter d = null;
      try {
        d = parseRow(parseLine(line));
        codec.writeMap(d);

        count++;
        if (count >= docsCount)
          break;
      } catch (Exception e) {
        // invalid doc
        continue;
      }

    }
    codec.writeTag(END);
    codec.close();
    return true;
  }

  static class J extends JavaBinCodec {
    public J(OutputStream os) throws IOException {
      super(os, null);
    }

    @Override
    public void writeVal(Object o) throws IOException {
      if (o instanceof float[] f) {
        writeTag((byte) 21);
        writeTag(FLOAT);
        writeVInt(f.length, daos);
        for (float v : f) {
          daos.writeFloat(v);
        }
      } else {
        super.writeVal(o);
      }
    }
  }

  static MapWriter parseRow(String[] row) {
    String id;
    String title;
    String article;
    Object article_vector;
    if (row.length < 4) {
      throw new IllegalArgumentException("Invalid row");
    }

    id = row[0];
    title = row[1];
    article = row[2];
    try {
      String json = row[3];
      if (json.charAt(0) != '[') {
        throw new IllegalArgumentException("Invalid json");
      }

      List<Float> floatList = OBJECT_MAPPER.readValue(json, valueTypeRef);

      article_vector = floatList;

      return ew -> {
        ew.put("id", id);
        // ew.put("title", title);
        // ew.put("article", article);
        ew.put("article_vector", article_vector);
      };

    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON");

    }

  }

  private static String[] parseLine(String line) {
    List<String> values = new ArrayList<>();
    StringBuilder currentValue = new StringBuilder();
    boolean inQuotes = false;
    char[] chars = line.toCharArray();

    for (int i = 0; i < chars.length; i++) {
      char currentChar = chars[i];

      if (currentChar == '"') {
        // Toggle the inQuotes flag
        inQuotes = !inQuotes;
      } else if (currentChar == ',' && !inQuotes) {
        // If a comma is found and we're not inside quotes, end the current value
        values.add(currentValue.toString());
        currentValue = new StringBuilder();
      } else {
        // Add the current character to the current value
        currentValue.append(currentChar);
      }
    }

    // Add the last value
    values.add(currentValue.toString());
    return values.toArray(new String[0]);
  }

  static TypeReference<List<Float>> valueTypeRef = new TypeReference<>() {
  };

}