package com.searchscale.benchmarks;

import java.io.FileOutputStream;

public class TestBytesRead {
    public static void main(String[] args) throws Exception {
        String fbinFile = "base.1M.fbin";
        String testOutput = "test_output.javabin";
        
        System.out.println("Testing OptimizedJavaBinWriter...");
        
        // Test writing with optimized writer
        try (FileOutputStream os = new FileOutputStream(testOutput)) {
            OptimizedJavaBinWriter writer = new OptimizedJavaBinWriter(os);
            writer.startBatch();
            
            // Read and write first 3 vectors
            OptimizedReader.streamFvecsRangeMapped(fbinFile, 0, 3, (vector, index) -> {
                try {
                    System.out.println("Writing vector " + index + " with dimension " + vector.length);
                    System.out.println("First few values: " + java.util.Arrays.toString(java.util.Arrays.copyOf(vector, 5)));
                    writer.writeVectorDocument(String.valueOf(index), vector);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            
            writer.endBatch();
            System.out.println("Successfully wrote JavaBin file");
        }
        
        System.out.println("File size: " + java.nio.file.Files.size(java.nio.file.Paths.get(testOutput)) + " bytes");
    }
}