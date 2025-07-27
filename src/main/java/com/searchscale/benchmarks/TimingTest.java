package com.searchscale.benchmarks;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

public class TimingTest {
    public static void main(String[] args) throws Exception {
        String fbinFile = "base.1M.fbin";
        int totalDocs = 100000;
        int batchSize = 10000;
        
        System.out.println("Performance Timing Test");
        System.out.println("=======================");
        System.out.println("Processing " + totalDocs + " vectors in batches of " + batchSize);
        System.out.println();
        
        // Warmup
        System.out.println("Warming up...");
        for (int i = 0; i < 3; i++) {
            runBenchmark(fbinFile, 10000, 1000, false);
        }
        
        // Test different scenarios
        int[] docCounts = {50000, 100000, 200000};
        int[] batchSizes = {5000, 10000, 20000};
        
        System.out.println("\nBenchmark Results:");
        System.out.println("==================");
        System.out.printf("%-15s %-15s %-20s %-20s %-15s%n", 
            "Total Docs", "Batch Size", "Single Thread (ms)", "Multi Thread (ms)", "Speedup");
        System.out.println("-".repeat(85));
        
        for (int docs : docCounts) {
            for (int batch : batchSizes) {
                if (batch > docs) continue;
                
                // Single-threaded
                long singleTime = runBenchmark(fbinFile, docs, batch, false);
                
                // Multi-threaded
                long multiTime = runBenchmark(fbinFile, docs, batch, true);
                
                double speedup = (double) singleTime / multiTime;
                
                System.out.printf("%-15d %-15d %-20d %-20d %-15.2fx%n", 
                    docs, batch, singleTime, multiTime, speedup);
                
                // Clean up
                cleanupDirs();
            }
        }
    }
    
    private static long runBenchmark(String fbinFile, int totalDocs, int batchSize, boolean multiThreaded) throws Exception {
        String outputDir = multiThreaded ? "timing_test/multi" : "timing_test/single";
        String threads = multiThreaded ? "all" : "1";
        
        // Clean up before run
        cleanupDir(outputDir);
        
        String[] cmdArgs = {
            "data_file=" + fbinFile,
            "output_dir=" + outputDir,
            "batch_size=" + batchSize,
            "docs_count=" + totalDocs,
            "overwrite=true",
            "threads=" + threads
        };
        
        // Capture output
        PrintStream originalOut = System.out;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
        
        long startTime = System.nanoTime();
        Indexer.main(cmdArgs);
        long endTime = System.nanoTime();
        
        // Restore output
        System.setOut(originalOut);
        
        return TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
    }
    
    private static void cleanupDirs() throws IOException {
        cleanupDir("timing_test/single");
        cleanupDir("timing_test/multi");
    }
    
    private static void cleanupDir(String dirName) throws IOException {
        Path dir = Paths.get(dirName);
        if (Files.exists(dir)) {
            Files.walk(dir)
                .sorted((a, b) -> b.compareTo(a))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        // Ignore
                    }
                });
        }
    }
}