package com.searchscale.benchmarks;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.BiConsumer;

/**
 * Optimized fbin reader using memory-mapped I/O and direct buffers
 */
public class OptimizedReader {

    /**
     * {@link MappedByteBuffer} capacity is limited to {@link Integer#MAX_VALUE} bytes, so each
     * {@link FileChannel#map} call must stay within this size. Large batches (e.g. 1M × 768-dim
     * floats) exceed that limit unless we map multiple contiguous windows.
     */
    private static final long MAX_MAP_BYTES = Integer.MAX_VALUE;

    // Use larger buffer size for bulk reading - optimized for modern hardware
    private static final int BUFFER_SIZE = 256 * 1024; // 256KB buffer for better throughput
    
    /**
     * Ultra-fast streaming using memory-mapped I/O
     * Supports both .fbin (with 8-byte header) and .fvecs (with per-vector dimension prefix)
     *
     * @param startIndex first vector index in the file (after header for .fbin)
     * @param count number of vectors to read from that index
     * @param consumer receives each vector and its index within this range (0 .. count-1)
     */
    public static long streamFvecsRangeMapped(String filePath, long startIndex, long count, BiConsumer<float[], Long> consumer) {
        long totalBytesRead = 0;
        
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             FileChannel channel = file.getChannel()) {
            
            // Check if it's .fvecs or .fbin format by file extension
            boolean isFvecs = filePath.endsWith(".fvecs") || filePath.endsWith(".fvecs.gz");
            
            if (isFvecs) {
                // .fvecs format: each vector has [int dim][float...] format, no global header
                // Read first vector's dimension to determine vector size
                ByteBuffer dimBuffer = ByteBuffer.allocate(4);
                dimBuffer.order(ByteOrder.LITTLE_ENDIAN);
                channel.read(dimBuffer);
                dimBuffer.flip();
                int dimension = dimBuffer.getInt();
                channel.position(0); // Reset to start
                totalBytesRead += 4;
                
                if (dimension <= 0 || dimension > 10000) {
                    return totalBytesRead;
                }
                
                // Each vector is: 4 bytes (dim) + dim * 4 bytes (floats)
                int vectorSize = 4 + dimension * 4;
                long vectorsPerMap = Math.max(1L, MAX_MAP_BYTES / vectorSize);

                long fileVectorIndex = startIndex;
                long vectorsRemaining = count;
                long outputIndex = 0;

                while (vectorsRemaining > 0) {
                    long startByteOffset = fileVectorIndex * vectorSize;
                    if (startByteOffset >= file.length()) {
                        break;
                    }
                    long nThis = Math.min(vectorsRemaining, vectorsPerMap);
                    long maxVectorsFromFile = (file.length() - startByteOffset + vectorSize - 1) / vectorSize;
                    nThis = Math.min(nThis, maxVectorsFromFile);
                    if (nThis <= 0) {
                        break;
                    }
                    long dataSize = nThis * vectorSize;
                    if (dataSize > MAX_MAP_BYTES) {
                        nThis = MAX_MAP_BYTES / vectorSize;
                        dataSize = nThis * vectorSize;
                    }

                    MappedByteBuffer mappedBuffer = channel.map(
                        FileChannel.MapMode.READ_ONLY, startByteOffset, dataSize);
                    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                    if (mappedBuffer.capacity() > 1024 * 1024) {
                        mappedBuffer.load();
                    }

                    long consumedThisMap = 0;
                    for (long i = 0; i < nThis; i++) {
                        if (mappedBuffer.remaining() < vectorSize) {
                            break;
                        }

                        int vecDim = mappedBuffer.getInt();
                        if (vecDim != dimension) {
                            System.err.println("Warning: Inconsistent dimension at vector " + (fileVectorIndex + i) + ": " + vecDim + " vs " + dimension);
                            vectorsRemaining = 0;
                            break;
                        }

                        float[] vector = new float[dimension];
                        for (int j = 0; j < dimension; j++) {
                            vector[j] = mappedBuffer.getFloat();
                        }

                        consumer.accept(vector, outputIndex);
                        outputIndex++;
                        consumedThisMap++;
                    }

                    totalBytesRead += consumedThisMap * vectorSize;
                    if (consumedThisMap == 0) {
                        break;
                    }
                    fileVectorIndex += consumedThisMap;
                    vectorsRemaining -= consumedThisMap;
                }
                
            } else {
                // .fbin format: has 8-byte header [int count][int dimension]
                ByteBuffer headerBuffer = ByteBuffer.allocate(8);
                headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
                channel.read(headerBuffer);
                headerBuffer.flip();
                
                int totalCount = headerBuffer.getInt();
                int dimension = headerBuffer.getInt();
                long totalCountL = totalCount;
                totalBytesRead += 8;
                
                if (dimension <= 0 || dimension > 10000) {
                    return totalBytesRead;
                }

                long bytesPerVector = (long) dimension * 4L;
                long vectorsPerMap = Math.max(1L, MAX_MAP_BYTES / bytesPerVector);

                long fileVectorIndex = startIndex;
                long vectorsRemaining = count;
                long outputIndex = 0;

                while (vectorsRemaining > 0) {
                    long availableInFile = Math.max(0L, totalCountL - fileVectorIndex);
                    long nThis = Math.min(vectorsRemaining, vectorsPerMap);
                    nThis = Math.min(nThis, availableInFile);
                    if (nThis <= 0) {
                        break;
                    }

                    long chunkBytes = nThis * bytesPerVector;
                    if (chunkBytes > MAX_MAP_BYTES) {
                        nThis = MAX_MAP_BYTES / bytesPerVector;
                        chunkBytes = nThis * bytesPerVector;
                    }

                    long startByteOffset = 8L + fileVectorIndex * bytesPerVector;
                    if (startByteOffset >= file.length()) {
                        break;
                    }
                    long maxBytes = file.length() - startByteOffset;
                    if (chunkBytes > maxBytes) {
                        nThis = maxBytes / bytesPerVector;
                        chunkBytes = nThis * bytesPerVector;
                    }
                    if (nThis <= 0) {
                        break;
                    }

                    MappedByteBuffer mappedBuffer = channel.map(
                        FileChannel.MapMode.READ_ONLY, startByteOffset, chunkBytes);
                    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                    if (mappedBuffer.capacity() > 1024 * 1024) {
                        mappedBuffer.load();
                    }

                    int rowBytes = dimension * 4;
                    long consumedThisMap = 0;
                    for (long i = 0; i < nThis; i++) {
                        if (mappedBuffer.remaining() < rowBytes) {
                            break;
                        }

                        float[] vector = new float[dimension];
                        for (int j = 0; j < dimension; j++) {
                            vector[j] = mappedBuffer.getFloat();
                        }

                        consumer.accept(vector, outputIndex);
                        outputIndex++;
                        consumedThisMap++;
                    }

                    totalBytesRead += consumedThisMap * rowBytes;
                    if (consumedThisMap == 0) {
                        break;
                    }
                    fileVectorIndex += consumedThisMap;
                    vectorsRemaining -= consumedThisMap;
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return totalBytesRead;
    }
    
    /**
     * Optimized streaming with bulk reads and direct buffers
     */
    public static long streamFvecsRangeOptimized(String filePath, int startIndex, int count, BiConsumer<float[], Integer> consumer) {
        long totalBytesRead = 0;
        
        try (FileChannel channel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            
            // Read header with direct buffer
            ByteBuffer headerBuffer = ByteBuffer.allocateDirect(8);
            headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
            channel.read(headerBuffer);
            headerBuffer.flip();
            
            int totalCount = headerBuffer.getInt();
            int dimension = headerBuffer.getInt();
            totalBytesRead += 8;
            
            if (dimension <= 0 || dimension > 10000) {
                return totalBytesRead;
            }
            
            // Calculate position
            long startByteOffset = 8 + ((long) startIndex * dimension * 4L);
            channel.position(startByteOffset);
            
            // Use direct buffer for better performance
            int vectorSize = dimension * 4;
            int bufferedVectors = BUFFER_SIZE / vectorSize;
            ByteBuffer bulkBuffer = ByteBuffer.allocateDirect(bufferedVectors * vectorSize);
            bulkBuffer.order(ByteOrder.LITTLE_ENDIAN);
            
            int vectorsProcessed = 0;
            
            while (vectorsProcessed < count && (startIndex + vectorsProcessed) < totalCount) {
                bulkBuffer.clear();
                int bytesRead = channel.read(bulkBuffer);
                if (bytesRead <= 0) break;
                
                totalBytesRead += bytesRead;
                bulkBuffer.flip();
                
                // Process vectors from bulk buffer
                while (bulkBuffer.remaining() >= vectorSize && vectorsProcessed < count) {
                    float[] vector = new float[dimension];
                    for (int j = 0; j < dimension; j++) {
                        vector[j] = bulkBuffer.getFloat();
                    }
                    
                    consumer.accept(vector, vectorsProcessed);
                    vectorsProcessed++;
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return totalBytesRead;
    }
    
    /**
     * Optimized chunk streaming with memory mapping
     */
    public static long streamFvecsChunkMapped(String filePath, Object chunkObj, BiConsumer<float[], Integer> consumer) {
        long totalBytesRead = 0;
        
        try {
            // Get chunk properties
            Class<?> chunkClass = chunkObj.getClass();
            long startByteOffset = (Long) chunkClass.getField("startByteOffset").get(chunkObj);
            long endByteOffset = (Long) chunkClass.getField("endByteOffset").get(chunkObj);
            
            try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
                 FileChannel channel = file.getChannel()) {
                
                // Read header for dimension
                ByteBuffer headerBuffer = ByteBuffer.allocate(8);
                headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
                channel.read(headerBuffer);
                headerBuffer.flip();
                
                int totalCount = headerBuffer.getInt();
                int dimension = headerBuffer.getInt();
                totalBytesRead += 8;
                
                if (dimension <= 0 || dimension > 10000) {
                    return totalBytesRead;
                }
                
                // Memory map the chunk with optimization hints
                long chunkSize = endByteOffset - startByteOffset;
                MappedByteBuffer mappedBuffer = channel.map(
                    FileChannel.MapMode.READ_ONLY, startByteOffset, chunkSize);
                mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                
                // Set load hint for better performance on large chunks
                if (mappedBuffer.capacity() > 1024 * 1024) {
                    mappedBuffer.load();
                }
                
                totalBytesRead += chunkSize;
                
                // Process vectors directly from mapped memory
                int vectorIndex = 0;
                while (mappedBuffer.remaining() >= dimension * 4) {
                    float[] vector = new float[dimension];
                    for (int j = 0; j < dimension; j++) {
                        vector[j] = mappedBuffer.getFloat();
                    }
                    
                    consumer.accept(vector, vectorIndex);
                    vectorIndex++;
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return totalBytesRead;
    }
}
