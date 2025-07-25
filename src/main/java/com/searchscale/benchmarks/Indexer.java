package com.searchscale.benchmarks;

import static org.apache.solr.common.util.JavaBinCodec.END;
import static org.apache.solr.common.util.JavaBinCodec.ITERATOR;

import java.io.BufferedReader;
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
import java.util.concurrent.atomic.AtomicInteger;
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
    public final boolean isLegacy;
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
      isLegacy = Boolean.parseBoolean(p.getOrDefault("legacy", "false"));
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
            if (!writeBatch(batchSz, br, codec, p.isLegacy))
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
    System.out.println("Using single-threaded processing");
    
    // Calculate total batches needed
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
    System.out.println("Using " + p.threads + " threads for parallel processing");
    
    // Calculate number of batches needed (same logic as single-threaded)
    int totalBatches = (int) Math.ceil((double) docsCount / batchSz);
    
    // Get file information for chunking approach - use batches, not threads
    long chunkCalcStart = System.currentTimeMillis();
    FileChunkInfo chunkInfo = calculateFileChunks(p.dataFile, docsCount, totalBatches);
    long chunkCalcEnd = System.currentTimeMillis();
    long chunkCalcTime = chunkCalcEnd - chunkCalcStart;
    
    System.out.println("calculateFileChunks() took: " + chunkCalcTime + "ms");
    System.out.println("File chunking info: total size=" + chunkInfo.fileSize + " bytes, " +
                       "estimated bytes per vector=" + chunkInfo.bytesPerVector + ", " +
                       "chunks=" + chunkInfo.chunks.size());
    
    // Use ExecutorService for thread management
    ExecutorService executor = Executors.newFixedThreadPool(p.threads);
    
    AtomicInteger completedBatches = new AtomicInteger(0);
    AtomicInteger totalDocsProcessed = new AtomicInteger(0);
    
    List<Future<ChunkProcessingResult>> futures = new ArrayList<>();
    
    long taskSubmitStart = System.currentTimeMillis();
    
    try {
      for (int i = 0; i < chunkInfo.chunks.size(); i++) {
        final int chunkIndex = i;
        final FileChunk chunk = chunkInfo.chunks.get(i);
        
        Future<ChunkProcessingResult> future = executor.submit(() -> {
          try {
            ChunkProcessingResult result = processBatchChunked(p, chunkIndex, chunk, (int) batchSz);
            int completed = completedBatches.incrementAndGet();
            totalDocsProcessed.addAndGet(result.documentsProcessed);
            System.out.println("Completed chunk " + chunkIndex + " with " + result.documentsProcessed + " documents (" + 
                               completed + "/" + chunkInfo.chunks.size() + ") - Total processed: " + totalDocsProcessed.get());
            return result;
          } catch (Exception e) {
            System.err.println("Error processing chunk " + chunkIndex + ": " + e.getMessage());
            e.printStackTrace();
            return new ChunkProcessingResult(0, 0);
          }
        });
        
        futures.add(future);
      }
      
      long taskSubmitTime = System.currentTimeMillis() - taskSubmitStart;
      
      // Wait for all tasks to complete and collect results
      long waitStart = System.currentTimeMillis();
      int totalProcessed = 0;
      long totalBytesRead = 0;
      for (int i = 0; i < futures.size(); i++) {
        ChunkProcessingResult result = futures.get(i).get();
        totalProcessed += result.documentsProcessed;
        totalBytesRead += result.bytesRead;
        System.out.println("Chunk " + i + " processed " + result.documentsProcessed + " documents, read " + result.bytesRead + " bytes");
      }
      
      long waitTime = System.currentTimeMillis() - waitStart;
      long totalMultiThreadTime = System.currentTimeMillis() - chunkCalcEnd;
      
      System.out.println("[TIMING] All tasks waited/completed in " + waitTime + "ms");
      System.out.println("[TIMING] Total multithreaded processing time: " + totalMultiThreadTime + "ms");
      System.out.println("All chunks completed successfully!");
      System.out.println("Total documents processed: " + totalProcessed);
      System.out.println("TOTAL BYTES READ FROM FILE: " + totalBytesRead + " bytes");
      
    } finally {
      executor.shutdown();
    }
  }

  private static long processBatch(Params p, int batchIndex, long startIndex, int batchSize) throws Exception {
    String name = Paths.get(p.outputDir, "batch." + batchIndex).toString();
    
    // Read only the vectors needed for this batch
    List<float[]> vectors = new ArrayList<>();
    FBIvecsReader.ReadResult readResult = FBIvecsReader.readFvecsRangeWithMetrics(p.dataFile, (int) startIndex, batchSize, vectors);
    
    try (FileOutputStream os = new FileOutputStream(name)) {
      JavaBinCodec codec = new J(os);
      codec.writeTag(ITERATOR);
      
      for (int i = 0; i < vectors.size(); i++) {
        float[] vector = vectors.get(i);
        final int docId = (int) (startIndex + i);
        final float[] vectorData = vector;
        
        MapWriter d = ew -> {
          ew.put("id", String.valueOf(docId));
          if (p.isLegacy) {
            // Convert float[] to List<Float> for legacy mode
            List<Float> floatList = new ArrayList<>();
            for (float f : vectorData) {
              floatList.add(f);
            }
            ew.put("article_vector", floatList);
          } else {
            ew.put("article_vector", vectorData);
          }
        };
        
        codec.writeMap(d);
      }
      
      codec.writeTag(END);
      codec.close();
      System.out.println(name);
    }
    
    return readResult.bytesRead;
  }

  // Class to hold processing results
  private static class ChunkProcessingResult {
    final int documentsProcessed;
    final long bytesRead;
    
    ChunkProcessingResult(int documentsProcessed, long bytesRead) {
      this.documentsProcessed = documentsProcessed;
      this.bytesRead = bytesRead;
    }
  }
  
  private static ChunkProcessingResult processBatchChunked(Params p, int chunkIndex, FileChunk chunk, int maxBatchSize) throws Exception {
    long chunkStart = System.currentTimeMillis();
    
    String name = Paths.get(p.outputDir, "batch." + chunkIndex).toString();
    
    // Read vectors from this chunk
    long readStart = System.currentTimeMillis();
    List<float[]> vectors = new ArrayList<>();
    long bytesRead = FBIvecsReader.readFvecsChunk(p.dataFile, chunk, vectors);
    long readTime = System.currentTimeMillis() - readStart;
    
    long writeStart = System.currentTimeMillis();
    try (FileOutputStream os = new FileOutputStream(name)) {
      JavaBinCodec codec = new J(os);
      codec.writeTag(ITERATOR);
      
      for (int i = 0; i < vectors.size(); i++) {
        float[] vector = vectors.get(i);
        final int docId = chunk.startVectorIndex + i;
        final float[] vectorData = vector;
        
        MapWriter d = ew -> {
          ew.put("id", String.valueOf(docId));
          if (p.isLegacy) {
            // Convert float[] to List<Float> for legacy mode
            List<Float> floatList = new ArrayList<>();
            for (float f : vectorData) {
              floatList.add(f);
            }
            ew.put("article_vector", floatList);
          } else {
            ew.put("article_vector", vectorData);
          }
        };
        
        codec.writeMap(d);
      }
      
      codec.writeTag(END);
      codec.close();
    }
    long writeTime = System.currentTimeMillis() - writeStart;
    long totalTime = System.currentTimeMillis() - chunkStart;
    
    System.out.println(name + " (" + vectors.size() + " documents, " + bytesRead + " bytes read)");
    
    return new ChunkProcessingResult(vectors.size(), bytesRead);
  }

  private static boolean writeBatch(long docsCount, BufferedReader br, JavaBinCodec codec, boolean legacy)
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
        d = parseRow(parseLine(line), legacy);
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

  static MapWriter parseRow(String[] row, boolean legacy) {
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

      if (legacy) {
        article_vector = floatList;

      } else {
        float[] floats = new float[floatList.size()];
        for (int i = 0; i < floatList.size(); i++) {
          floats[i] = floatList.get(i);
        }
        article_vector = floats;
      }

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

  // File chunking classes for efficient parallel processing
  private static class FileChunk {
    public final long startByteOffset;
    public final long endByteOffset;
    public final int startVectorIndex;
    public final int estimatedVectorCount;
    
    FileChunk(long startByteOffset, long endByteOffset, int startVectorIndex, int estimatedVectorCount) {
      this.startByteOffset = startByteOffset;
      this.endByteOffset = endByteOffset;
      this.startVectorIndex = startVectorIndex;
      this.estimatedVectorCount = estimatedVectorCount;
    }
  }
  
  private static class FileChunkInfo {
    final long fileSize;
    final long bytesPerVector;
    final List<FileChunk> chunks;
    
    FileChunkInfo(long fileSize, long bytesPerVector, List<FileChunk> chunks) {
      this.fileSize = fileSize;
      this.bytesPerVector = bytesPerVector;
      this.chunks = chunks;
    }
  }
  
  private static FileChunkInfo calculateFileChunks(String filePath, long totalVectors, int numChunks) throws IOException {
    File file = new File(filePath);
    long fileSize = file.length();
    
    // For .fbin files, read the actual vector count from header to calculate bytes per vector correctly
    long actualVectorsInFile = totalVectors;
    if (filePath.endsWith(".fbin")) {
      // Read the actual count from the .fbin file header
      try (FileInputStream fis = new FileInputStream(filePath)) {
        byte[] countBytes = new byte[4];
        fis.read(countBytes);
        ByteBuffer bb = ByteBuffer.wrap(countBytes);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        actualVectorsInFile = bb.getInt();
      } catch (Exception e) {
        // Fallback to provided totalVectors if header read fails
        System.err.println("Warning: Could not read vector count from .fbin header, using provided count: " + totalVectors);
      }
    }
    
    // Calculate bytes per vector based on actual file content
    long dataSize = fileSize;
    if (filePath.endsWith(".fbin")) {
      dataSize = fileSize - 8; // subtract header size
    }
    long bytesPerVector = dataSize / actualVectorsInFile;
    
    // Calculate the total data size we actually want to process (only for requested vectors)
    long requestedDataSize = totalVectors * bytesPerVector;
    
    // Calculate chunk size based on requested data, not entire file
    long chunkSize = requestedDataSize / numChunks;
    
    List<FileChunk> chunks = new ArrayList<>();
    
    for (int i = 0; i < numChunks; i++) {
      long startOffset = i * chunkSize;
      long endOffset = (i == numChunks - 1) ? requestedDataSize : (i + 1) * chunkSize;
      
      // For .fbin files, add header offset
      if (filePath.endsWith(".fbin")) {
        startOffset += 8;
        endOffset += 8;
      }
      
      int startVectorIndex = (int) (i * totalVectors / numChunks);
      int estimatedVectorCount = (int) ((endOffset - startOffset) / bytesPerVector);
      
      chunks.add(new FileChunk(startOffset, endOffset, startVectorIndex, estimatedVectorCount));
    }
    
    return new FileChunkInfo(fileSize, bytesPerVector, chunks);
  }

}