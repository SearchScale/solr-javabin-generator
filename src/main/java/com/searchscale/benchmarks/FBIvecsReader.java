package com.searchscale.benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: The three static methods have a lot of common logic, ideally should be combined as just one.
public class FBIvecsReader {

  private static final Logger log = LoggerFactory.getLogger(FBIvecsReader.class.getName());

  public static int getDimension(InputStream fc) throws IOException {
    byte[] b = fc.readNBytes(4);
    ByteBuffer bb = ByteBuffer.wrap(b);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    int dimension = bb.getInt();
    return dimension;
  }

  public static void readFvecs(String filePath, int numRows, List<float[]> vectors) {
    log.info("Reading {} from file: {}", numRows, filePath);

    try {
      InputStream is = null;

      if (filePath.endsWith("fvecs.gz")) {
        is = new GZIPInputStream(new FileInputStream(filePath));
      } else if (filePath.endsWith(".fvecs") || filePath.endsWith(".fbin")) {
        is = new FileInputStream(filePath);
      }

      int count = 0;
      int dimension = -1;
      
      // For .fbin files, read header differently
      if (filePath.endsWith(".fbin")) {
        // Read total count (first 4 bytes)
        int totalCount = getDimension(is);
        log.info("Total vectors in file: {}", totalCount);
        
        // Read dimension (second 4 bytes)
        dimension = getDimension(is);
        log.info("Vector dimension: {}", dimension);
        
        if (dimension <= 0 || dimension > 10000) {
          log.warn("Invalid dimension: {}", dimension);
          return;
        }
        
        // Read vectors
        while (count < numRows && count < totalCount) {
          float[] row = new float[dimension];
          
          for (int i = 0; i < dimension; i++) {
            ByteBuffer bbf = ByteBuffer.wrap(is.readNBytes(4));
            bbf.order(ByteOrder.LITTLE_ENDIAN);
            row[i] = bbf.getFloat();
          }
          
          vectors.add(row);
          count += 1;
          
          if (count % 1000 == 0) {
            System.out.print(".");
          }
        }
      } else {
        // Original .fvecs format - dimension per vector
        while (is.available() != 0) {
          // Read dimension for each vector
          dimension = getDimension(is);
          if (dimension <= 0 || dimension > 10000) {
            log.warn("Invalid dimension: {}", dimension);
            break;
          }
          
          float[] row = new float[dimension];
          
          // Read the vector data
          for (int i = 0; i < dimension; i++) {
            ByteBuffer bbf = ByteBuffer.wrap(is.readNBytes(4));
            bbf.order(ByteOrder.LITTLE_ENDIAN);
            row[i] = bbf.getFloat();
          }
          
          vectors.add(row);
          count += 1;

          if (count % 1000 == 0) {
            System.out.print(".");
          }

          if (numRows != -1 && count == numRows) {
            break;
          }
        }
      }
      
      System.out.println();
      is.close();
      log.info("Reading complete. Read {} vectors.", count);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static ArrayList<int[]> readIvecs(String filePath, int numRows) {
    log.info("Reading {} from file: {}", numRows, filePath);
    ArrayList<int[]> vectors = new ArrayList<int[]>();

    try {
      InputStream is = null;

      if (filePath.endsWith("ivecs.gz")) {
        is = new GZIPInputStream(new FileInputStream(filePath));
      } else if (filePath.endsWith(".ivecs")) {
        is = new FileInputStream(filePath);
      }

      int dimension = getDimension(is);
      int[] row = new int[dimension];
      int count = 0;
      int rc = 0;

      while (is.available() != 0) {

        ByteBuffer bbf = ByteBuffer.wrap(is.readNBytes(4));
        bbf.order(ByteOrder.LITTLE_ENDIAN);
        row[rc++] = bbf.getInt();

        if (rc == dimension) {
          vectors.add(row);
          count += 1;
          rc = 0;
          row = new int[dimension];

          // Skip last 4 bytes.
          is.readNBytes(4);

          if (count % 1000 == 0) {
            System.out.print(".");
          }

          if (numRows != -1 && count == numRows) {
            break;
          }
        }
      }
      System.out.println();
      is.close();
      log.info("Reading complete. Read {} vectors.", count);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return vectors;
  }

  public static void readBvecs(String filePath, int numRows, List<float[]> vectors) {
    log.info("Reading {} from file: {}", numRows, filePath);

    try {
      InputStream is = null;

      if (filePath.endsWith("bvecs.gz")) {
        is = new GZIPInputStream(new FileInputStream(filePath));
      } else if (filePath.endsWith(".bvecs")) {
        is = new FileInputStream(filePath);
      }

      int dimension = getDimension(is);

      float[] row = new float[dimension];
      int count = 0;
      int rc = 0;

      while (is.available() != 0) {

        ByteBuffer bbf = ByteBuffer.wrap(is.readNBytes(1));
        bbf.order(ByteOrder.LITTLE_ENDIAN);
        row[rc++] = bbf.get() & 0xff;

        if (rc == dimension) {
          vectors.add(row);
          count += 1;
          rc = 0;
          row = new float[dimension];

          // Skip last 4 bytes.
          is.readNBytes(4);

          if (count % 1000 == 0) {
            System.out.print(".");
          }

          if (numRows != -1 && count == numRows) {
            break;
          }
        }
      }
      System.out.println();
      is.close();
      log.info("Reading complete. Read {} vectors.", count);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // Result class for returning both vectors and bytes read
  public static class ReadResult {
    public final long bytesRead;
    
    public ReadResult(long bytesRead) {
      this.bytesRead = bytesRead;
    }
  }
  
  /**
   * Read a specific range of vectors from an fbin file for parallel processing
   */
  public static void readFvecsRange(String filePath, int startIndex, int count, List<float[]> vectors) {
    readFvecsRangeWithMetrics(filePath, startIndex, count, vectors);
  }
  
  /**
   * Read a specific range of vectors from an fbin file and return metrics
   */
  public static ReadResult readFvecsRangeWithMetrics(String filePath, int startIndex, int count, List<float[]> vectors) {
    long totalBytesRead = 0;
    try {
      if (filePath.endsWith(".fbin")) {
        // Use optimized NIO approach similar to chunked reading
        FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);
        
        // Read header efficiently
        ByteBuffer headerBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(0);
        channel.read(headerBuffer);
        headerBuffer.flip();
        int totalCount = headerBuffer.getInt();
        int dimension = headerBuffer.getInt();
        totalBytesRead += 8;
        
        if (dimension <= 0 || dimension > 10000) {
          log.warn("Invalid dimension: {}", dimension);
          channel.close();
          return new ReadResult(totalBytesRead);
        }
        
        // Calculate byte positions for the range we want to read
        long startByteOffset = 8 + ((long) startIndex * dimension * 4L); // Header + vector data offset
        long dataSize = (long) count * dimension * 4L; // Size of data to read
        
        // Bulk read the entire range in one operation
        ByteBuffer rangeBuffer = ByteBuffer.allocate((int)dataSize).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(startByteOffset);
        int bytesRead = channel.read(rangeBuffer);
        totalBytesRead += bytesRead;
        rangeBuffer.flip();
        
        // Process vectors efficiently from the bulk-read buffer
        int readCount = 0;
        while (rangeBuffer.remaining() >= dimension * 4 && readCount < count && (startIndex + readCount) < totalCount) {
          float[] row = new float[dimension];
          
          // Direct float reading from buffer - much faster than individual byte reads
          for (int i = 0; i < dimension; i++) {
            row[i] = rangeBuffer.getFloat();
          }
          
          vectors.add(row);
          readCount++;
        }
        
        channel.close();
      } else {
        // For .fvecs format, we need to read sequentially
        // This is less efficient but maintains compatibility
        log.warn("Range reading is less efficient for .fvecs format. Consider using .fbin format for better parallel performance.");
        
        InputStream is = new FileInputStream(filePath);
        int currentIndex = 0;
        while (is.available() != 0 && currentIndex < startIndex + count) {
          totalBytesRead += 4; // dimension bytes
          int dimension = getDimension(is);
          if (dimension <= 0 || dimension > 10000) {
            break;
          }
          
          float[] row = new float[dimension];
          for (int i = 0; i < dimension; i++) {
            byte[] bytes = is.readNBytes(4);
            totalBytesRead += bytes.length;
            ByteBuffer bbf = ByteBuffer.wrap(bytes);
            bbf.order(ByteOrder.LITTLE_ENDIAN);
            row[i] = bbf.getFloat();
          }
          
          // Only add vectors in our range
          if (currentIndex >= startIndex) {
            vectors.add(row);
          }
          
          currentIndex++;
        }
        is.close();
      }
    } catch (Exception e) {
      log.error("Error reading range [{}, {}] from file: {}", startIndex, startIndex + count - 1, filePath, e);
      e.printStackTrace();
    }
    
    return new ReadResult(totalBytesRead);
  }

  /**
   * Read vectors from a specific byte chunk of the file for efficient parallel processing.
   * This avoids the performance issue of seeking from the beginning for each thread.
   * Returns the exact number of bytes read from the file.
   */
  public static long readFvecsChunk(String filePath, Object chunkObj, List<float[]> vectors) {
    long methodStart = System.currentTimeMillis();
    long totalBytesRead = 0;
    
    try {
      // Get chunk properties using reflection
      long reflectionStart = System.currentTimeMillis();
      Class<?> chunkClass = chunkObj.getClass();
      long startByteOffset = (Long) chunkClass.getField("startByteOffset").get(chunkObj);
      long endByteOffset = (Long) chunkClass.getField("endByteOffset").get(chunkObj);
      int startVectorIndex = (Integer) chunkClass.getField("startVectorIndex").get(chunkObj);
      int estimatedVectorCount = (Integer) chunkClass.getField("estimatedVectorCount").get(chunkObj);
      long reflectionTime = System.currentTimeMillis() - reflectionStart;
      
      // Pre-allocate vector list with estimated capacity (if it's an ArrayList)
      if (vectors instanceof ArrayList) {
        ((ArrayList<float[]>) vectors).ensureCapacity(estimatedVectorCount);
      }
      
      // Open file channel for optimized reading
      long fileOpenStart = System.currentTimeMillis();
      FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ);
      long fileOpenTime = System.currentTimeMillis() - fileOpenStart;
      
      if (filePath.endsWith(".fbin")) {
        // Read header efficiently with NIO
        long headerStart = System.currentTimeMillis();
        ByteBuffer headerBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(0);
        channel.read(headerBuffer);
        headerBuffer.flip();
        int totalCount = headerBuffer.getInt();
        int dimension = headerBuffer.getInt();
        totalBytesRead += 8;
        long headerTime = System.currentTimeMillis() - headerStart;
        
        if (dimension <= 0 || dimension > 10000) {
          log.warn("Invalid dimension: {}", dimension);
          channel.close();
          return totalBytesRead;
        }
        
        // Bulk read entire chunk in one operation
        long chunkSize = endByteOffset - startByteOffset;
        long bulkReadStart = System.currentTimeMillis();
        
        ByteBuffer chunkBuffer = ByteBuffer.allocate((int)chunkSize).order(ByteOrder.LITTLE_ENDIAN);
        channel.position(startByteOffset);
        int bytesRead = channel.read(chunkBuffer);
        totalBytesRead += bytesRead;
        chunkBuffer.flip();
        
        long bulkReadTime = System.currentTimeMillis() - bulkReadStart;
        
        // Process vectors efficiently from the bulk-read buffer
        long vectorProcessStart = System.currentTimeMillis();
        int vectorsRead = 0;
        int vectorIndex = startVectorIndex;
        
        while (chunkBuffer.remaining() >= dimension * 4) {
          float[] row = new float[dimension];
          
          // Direct float reading from buffer - much faster than individual byte reads
          for (int i = 0; i < dimension; i++) {
            row[i] = chunkBuffer.getFloat();
          }
          
          vectors.add(row);
          vectorsRead++;
          vectorIndex++;
          
          if (vectorIndex % 1000 == 0) {
            System.out.print(".");
          }
        }
        
        long vectorProcessTime = System.currentTimeMillis() - vectorProcessStart;
        long totalTime = System.currentTimeMillis() - methodStart;
        
        log.debug("Chunk [{}->{}] OPTIMIZED timing: reflection={}ms, fileOpen={}ms, header={}ms, bulkRead={}ms, vectorProcess={}ms, total={}ms", 
                 startByteOffset, endByteOffset, reflectionTime, fileOpenTime, headerTime, bulkReadTime, vectorProcessTime, totalTime);
        log.debug("Chunk read complete. Read {} vectors ({} bytes) from offset {} to {}", 
                 vectorsRead, totalBytesRead, startByteOffset, endByteOffset);
        
      } else {
        log.warn("Chunk reading optimized for .fbin format. Using fallback for .fvecs format.");
        // For .fvecs, fall back to sequential reading - not optimal but functional
        readFvecs(filePath, -1, vectors);
        totalBytesRead = -1; // Cannot track bytes for fallback
      }
      
      channel.close();
      
    } catch (Exception e) {
      long totalTime = System.currentTimeMillis() - methodStart;
      log.error("Error reading chunk from file: {} (after {}ms)", filePath, totalTime, e);
      e.printStackTrace();
    }
    
    return totalBytesRead;
  }
}
