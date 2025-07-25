package com.searchscale.benchmarks;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class PerformanceBenchmark {
    
    private static final String FBIN_FILE = "base.1M.fbin";
    private static final int BATCH_SIZE = 2500;
    private static final int TOTAL_DOCS = 25000; // 10 batches of 2.5k each
    private static final int NUM_BATCHES = 10;
    
    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("JavaBin Batch Generation Performance Benchmark");
        System.out.println("=".repeat(80));
        System.out.println();
        
        // Check if base.1M.fbin exists
        if (!new File(FBIN_FILE).exists()) {
            System.err.println("Error: " + FBIN_FILE + " not found!");
            System.err.println("Please download the dataset first:");
            System.err.println("wget https://data.rapids.ai/raft/datasets/wiki_all_1M/wiki_all_1M.tar");
            System.err.println("tar -xf wiki_all_1M.tar");
            System.exit(1);
        }
        
        System.out.println("Configuration:");
        System.out.println("- Input file: " + FBIN_FILE);
        System.out.println("- Batch size: " + String.format("%,d", BATCH_SIZE) + " vectors");
        System.out.println("- Total batches: " + NUM_BATCHES);
        System.out.println("- Total vectors: " + String.format("%,d", TOTAL_DOCS));
        System.out.println();
        
        // Clean up any existing benchmark directories
        cleanupDirectory("benchmark_single");
        cleanupDirectory("benchmark_multi");
        
        // Run single-threaded benchmark
        System.out.println("Running single-threaded benchmark...");
        BenchmarkResult singleThreadResult = runBenchmark(1, "benchmark_single");
        
        // Run multi-threaded benchmark using all available processors
        System.out.println("Running multi-threaded benchmark (all processors)...");
        BenchmarkResult multiThreadResult = runBenchmark("all", "benchmark_multi");
        
        // Compare generated files
        System.out.println("Comparing generated files...");
        boolean filesIdentical = compareFiles("benchmark_single", "benchmark_multi");
        
        // Print summary
        printSummary(singleThreadResult, multiThreadResult, filesIdentical);
        
        // Clean up
        cleanupDirectory("benchmark_single");
        cleanupDirectory("benchmark_multi");
    }
    
    private static class BenchmarkResult {
        final long executionTime;
        final int actualThreads;
        
        BenchmarkResult(long executionTime, int actualThreads) {
            this.executionTime = executionTime;
            this.actualThreads = actualThreads;
        }
    }
    
    private static BenchmarkResult runBenchmark(Object threads, String outputDir) throws Exception {
        String[] args = {
            "data_file=" + FBIN_FILE,
            "output_dir=" + outputDir,
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + TOTAL_DOCS,
            "legacy=true",
            "overwrite=true",
            "threads=" + threads.toString()
        };
        
        // Determine actual number of threads that will be used
        int actualThreads;
        if ("all".equalsIgnoreCase(threads.toString())) {
            actualThreads = Runtime.getRuntime().availableProcessors();
        } else {
            actualThreads = Integer.parseInt(threads.toString());
        }
        
        long startTime = System.currentTimeMillis();
        Indexer.main(args);
        long endTime = System.currentTimeMillis();
        
        return new BenchmarkResult(endTime - startTime, actualThreads);
    }
    
    private static boolean compareFiles(String dir1, String dir2) {
        System.out.println("Verifying file integrity...");
        
        for (int i = 0; i < NUM_BATCHES; i++) {
            String file1 = Paths.get(dir1, "batch." + i).toString();
            String file2 = Paths.get(dir2, "batch." + i).toString();
            
            File f1 = new File(file1);
            File f2 = new File(file2);
            
            if (!f1.exists() || !f2.exists()) {
                System.err.println("Missing files: " + file1 + " or " + file2);
                return false;
            }
            
            if (f1.length() != f2.length()) {
                System.err.println("Size mismatch for batch " + i + ": " + 
                    f1.length() + " vs " + f2.length());
                return false;
            }
            
            if (!compareFileContents(file1, file2)) {
                System.err.println("Content mismatch for batch " + i);
                return false;
            }
        }
        
        System.out.println("✓ All " + NUM_BATCHES + " batch files are identical");
        return true;
    }
    
    private static boolean compareFileContents(String file1, String file2) {
        try {
            String hash1 = calculateFileHash(file1);
            String hash2 = calculateFileHash(file2);
            return hash1.equals(hash2);
        } catch (Exception e) {
            System.err.println("Error comparing files: " + e.getMessage());
            return false;
        }
    }
    
    private static String calculateFileHash(String filename) throws IOException, NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (FileInputStream fis = new FileInputStream(filename)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                md.update(buffer, 0, bytesRead);
            }
        }
        
        byte[] hashBytes = md.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    private static void cleanupDirectory(String dirName) {
        try {
            Path dir = Paths.get(dirName);
            if (Files.exists(dir)) {
                Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
            }
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }
    
    private static void printSummary(BenchmarkResult singleThreadResult, BenchmarkResult multiThreadResult, boolean filesIdentical) {
        System.out.println();
        System.out.println("=".repeat(80));
        System.out.println("PERFORMANCE BENCHMARK RESULTS");
        System.out.println("=".repeat(80));
        System.out.println();
        
        double singleThreadSec = singleThreadResult.executionTime / 1000.0;
        double multiThreadSec = multiThreadResult.executionTime / 1000.0;
        double speedup = singleThreadSec / multiThreadSec;
        double improvement = ((singleThreadSec - multiThreadSec) / singleThreadSec) * 100;
        
        String multiThreadHeader = String.format("%d Threads", multiThreadResult.actualThreads);
        System.out.printf("%-25s | %-15s | %-15s | %-15s%n", 
            "Metric", "Single Thread", multiThreadHeader, "Improvement");
        System.out.println("-".repeat(80));
        
        System.out.printf("%-25s | %-15s | %-15s | %-15s%n", 
            "Execution Time", 
            String.format("%.2f sec", singleThreadSec),
            String.format("%.2f sec", multiThreadSec),
            String.format("%.1f%% faster", improvement));
            
        System.out.printf("%-25s | %-15s | %-15s | %-15s%n", 
            "Throughput", 
            String.format("%,.0f docs/sec", TOTAL_DOCS / singleThreadSec),
            String.format("%,.0f docs/sec", TOTAL_DOCS / multiThreadSec),
            String.format("%.2fx speedup", speedup));
            
        System.out.printf("%-25s | %-15s | %-15s | %-15s%n", 
            "Batch Processing", 
            String.format("%.2f sec/batch", singleThreadSec / NUM_BATCHES),
            String.format("%.2f sec/batch", multiThreadSec / NUM_BATCHES),
            String.format("%.2fx faster", (singleThreadSec / NUM_BATCHES) / (multiThreadSec / NUM_BATCHES)));
        
        System.out.println("-".repeat(80));
        System.out.printf("%-25s | %-47s%n", 
            "File Integrity", 
            filesIdentical ? "✓ PASSED - All files identical" : "✗ FAILED - Files differ");
        
        System.out.println();
        System.out.println("Summary:");
        System.out.printf("• Multi-threading with %d threads achieved %.1f%% performance improvement%n", 
            multiThreadResult.actualThreads, improvement);
        System.out.printf("• Processing %.1fM vectors took %.2f seconds (vs %.2f seconds single-threaded)%n", 
            TOTAL_DOCS / 1000000.0, multiThreadSec, singleThreadSec);
        System.out.printf("• Throughput increased from %,.0f to %,.0f documents per second%n", 
            TOTAL_DOCS / singleThreadSec, TOTAL_DOCS / multiThreadSec);
        
        if (filesIdentical) {
            System.out.println("• ✓ Data integrity verified - multi-threaded output is identical to single-threaded");
        } else {
            System.out.println("• ✗ Data integrity issue - outputs differ between single and multi-threaded");
        }
        
        System.out.println();
        System.out.println("=".repeat(80));
    }
}