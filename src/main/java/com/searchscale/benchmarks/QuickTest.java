package com.searchscale.benchmarks;

import java.util.ArrayList;
import java.util.List;

public class QuickTest {
    public static void main(String[] args) throws Exception {
        String fbinFile = "base.1M.fbin";
        
        System.out.println("Testing OptimizedReader vs FBIvecsReader...");
        
        // Test 1: Read with original reader
        System.out.println("\n1. Testing original FBIvecsReader:");
        List<float[]> originalVectors = new ArrayList<>();
        FBIvecsReader.readFvecs(fbinFile, 5, originalVectors);
        System.out.println("Original reader - Read " + originalVectors.size() + " vectors");
        if (!originalVectors.isEmpty()) {
            System.out.println("First vector dimension: " + originalVectors.get(0).length);
            System.out.println("First few values: " + java.util.Arrays.toString(java.util.Arrays.copyOf(originalVectors.get(0), 5)));
        }
        
        // Test 2: Read with optimized reader using streaming
        System.out.println("\n2. Testing OptimizedReader streaming:");
        final List<float[]> optimizedVectors = new ArrayList<>();
        long bytesRead = OptimizedReader.streamFvecsRangeMapped(fbinFile, 0, 5, (vector, index) -> {
            optimizedVectors.add(vector.clone());
            System.out.println("Optimized - Read vector " + index + " with dimension " + vector.length);
        });
        System.out.println("Optimized reader - Read " + optimizedVectors.size() + " vectors, " + bytesRead + " bytes");
        
        // Test 3: Compare results
        System.out.println("\n3. Comparing results:");
        if (originalVectors.size() == optimizedVectors.size()) {
            for (int i = 0; i < originalVectors.size(); i++) {
                float[] orig = originalVectors.get(i);
                float[] opt = optimizedVectors.get(i);
                
                if (orig.length != opt.length) {
                    System.out.println("ERROR: Vector " + i + " dimension mismatch: " + orig.length + " vs " + opt.length);
                } else {
                    boolean match = true;
                    for (int j = 0; j < orig.length; j++) {
                        if (Math.abs(orig[j] - opt[j]) > 0.0001f) {
                            System.out.println("ERROR: Vector " + i + " component " + j + " mismatch: " + orig[j] + " vs " + opt[j]);
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        System.out.println("✓ Vector " + i + " matches perfectly");
                    }
                }
            }
        } else {
            System.out.println("ERROR: Different number of vectors: " + originalVectors.size() + " vs " + optimizedVectors.size());
        }
    }
}