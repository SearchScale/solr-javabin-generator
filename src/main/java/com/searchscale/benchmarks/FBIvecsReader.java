package com.searchscale.benchmarks;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

  /**
   * Read a specific range of vectors from an fbin file for parallel processing
   */
  public static void readFvecsRange(String filePath, int startIndex, int count, List<float[]> vectors) {
    try {
      InputStream is = new FileInputStream(filePath);
      
      if (filePath.endsWith(".fbin")) {
        // Read header
        int totalCount = getDimension(is);
        int dimension = getDimension(is);
        
        if (dimension <= 0 || dimension > 10000) {
          log.warn("Invalid dimension: {}", dimension);
          return;
        }
        
        // Skip to the start index
        long bytesToSkip = (long) startIndex * dimension * 4L; // 4 bytes per float
        long skipped = is.skip(bytesToSkip);
        if (skipped != bytesToSkip) {
          // Handle case where skip didn't work as expected
          is.close();
          is = new FileInputStream(filePath);
          getDimension(is); // Skip total count
          getDimension(is); // Skip dimension
          
          // Skip vectors one by one if skip() doesn't work
          for (int i = 0; i < startIndex; i++) {
            for (int j = 0; j < dimension; j++) {
              is.readNBytes(4); // Skip each float
            }
          }
        }
        
        // Read the requested range
        int readCount = 0;
        while (readCount < count && (startIndex + readCount) < totalCount) {
          float[] row = new float[dimension];
          
          for (int i = 0; i < dimension; i++) {
            byte[] bytes = is.readNBytes(4);
            if (bytes.length < 4) {
              // End of file reached
              is.close();
              return;
            }
            ByteBuffer bbf = ByteBuffer.wrap(bytes);
            bbf.order(ByteOrder.LITTLE_ENDIAN);
            row[i] = bbf.getFloat();
          }
          
          vectors.add(row);
          readCount++;
        }
      } else {
        // For .fvecs format, we need to read sequentially
        // This is less efficient but maintains compatibility
        log.warn("Range reading is less efficient for .fvecs format. Consider using .fbin format for better parallel performance.");
        
        int currentIndex = 0;
        while (is.available() != 0 && currentIndex < startIndex + count) {
          int dimension = getDimension(is);
          if (dimension <= 0 || dimension > 10000) {
            break;
          }
          
          float[] row = new float[dimension];
          for (int i = 0; i < dimension; i++) {
            ByteBuffer bbf = ByteBuffer.wrap(is.readNBytes(4));
            bbf.order(ByteOrder.LITTLE_ENDIAN);
            row[i] = bbf.getFloat();
          }
          
          // Only add vectors in our range
          if (currentIndex >= startIndex) {
            vectors.add(row);
          }
          
          currentIndex++;
        }
      }
      
      is.close();
    } catch (Exception e) {
      log.error("Error reading range [{}, {}] from file: {}", startIndex, startIndex + count - 1, filePath, e);
      e.printStackTrace();
    }
  }
}
