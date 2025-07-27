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
    
    // Use larger buffer size for bulk reading - optimized for modern hardware
    private static final int BUFFER_SIZE = 256 * 1024; // 256KB buffer for better throughput
    
    /**
     * Ultra-fast streaming using memory-mapped I/O
     */
    public static long streamFvecsRangeMapped(String filePath, int startIndex, int count, BiConsumer<float[], Integer> consumer) {
        long totalBytesRead = 0;
        
        try (RandomAccessFile file = new RandomAccessFile(filePath, "r");
             FileChannel channel = file.getChannel()) {
            
            // Read header
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
            
            // Calculate the data region to map
            long startByteOffset = 8 + ((long) startIndex * dimension * 4L);
            long dataSize = Math.min((long) count * dimension * 4L, 
                                     file.length() - startByteOffset);
            
            // Memory map the required portion of the file with read-ahead hints
            MappedByteBuffer mappedBuffer = channel.map(
                FileChannel.MapMode.READ_ONLY, startByteOffset, dataSize);
            mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
            
            // Set load hint for better performance on large sequential reads
            if (mappedBuffer.capacity() > 1024 * 1024) { // For buffers > 1MB
                mappedBuffer.load(); // Hint to OS to pre-load into memory
            }
            
            totalBytesRead += dataSize;
            
            // Process vectors directly from mapped memory
            for (int i = 0; i < count && (startIndex + i) < totalCount; i++) {
                if (mappedBuffer.remaining() < dimension * 4) {
                    break;
                }
                
                // Allocate vector for each iteration (consumer may keep reference)
                float[] vector = new float[dimension];
                for (int j = 0; j < dimension; j++) {
                    vector[j] = mappedBuffer.getFloat();
                }
                
                consumer.accept(vector, i);
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