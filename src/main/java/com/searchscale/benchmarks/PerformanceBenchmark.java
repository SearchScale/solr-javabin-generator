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
    
    // Default values
    private static String FBIN_FILE = "base.1M.fbin";
    private static int BATCH_SIZE = 6250;
    private static int TOTAL_DOCS = 50000;
    private static int NUM_BATCHES = 8;
    private static int THREADS = Runtime.getRuntime().availableProcessors(); // Default to all available cores
    
    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        parseArguments(args);
        
        System.out.println("=".repeat(80));
        System.out.println("JavaBin Batch Generation Performance Benchmark");
        System.out.println("=".repeat(80));
        System.out.println();
        
        System.out.println("Configuration:");
        System.out.println("- Input file: " + FBIN_FILE);
        System.out.println("- Batch size: " + String.format("%,d", BATCH_SIZE) + " vectors");
        System.out.println("- Total batches: " + NUM_BATCHES);
        System.out.println("- Total vectors: " + String.format("%,d", TOTAL_DOCS));
        System.out.println("- Threads: " + THREADS);
        System.out.println();
        
        // Clean up any existing benchmark directories
        System.out.println("Cleaning up previous benchmark directories...");
        cleanupDirectory("benchmark_single");
        cleanupDirectory("benchmark_multi");
        
        // Run multi-threaded benchmark using specified number of threads
        System.out.println("Running multi-threaded benchmark (" + THREADS + " threads)...");
        BenchmarkResult multiThreadResult = runBenchmark(THREADS, "benchmark_multi");
        
        // Run single-threaded benchmark
        System.out.println("Running single-threaded benchmark...");
        BenchmarkResult singleThreadResult = runBenchmark(1, "benchmark_single");
        
        // Compare generated files
        System.out.println("Comparing generated files...");
        boolean filesIdentical = compareFiles("benchmark_single", "benchmark_multi");
        
        // Print summary
        printSummary(singleThreadResult, multiThreadResult, filesIdentical);
    }
    
    private static class BenchmarkResult {
        final long executionTime;
        final int actualThreads;
        final long bytesRead;
        
        BenchmarkResult(long executionTime, int actualThreads, long bytesRead) {
            this.executionTime = executionTime;
            this.actualThreads = actualThreads;
            this.bytesRead = bytesRead;
        }
    }
    
    private static BenchmarkResult runBenchmark(Object threads, String outputDir) throws Exception {
        String[] args = {
            "data_file=" + FBIN_FILE,
            "output_dir=" + outputDir,
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + TOTAL_DOCS,
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
        
        // Capture output to extract bytes read
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.PrintStream originalOut = System.out;
        java.io.PrintStream ps = new java.io.PrintStream(baos);
        System.setOut(ps);
        
        long startTime = System.currentTimeMillis();
        Indexer.main(args);
        long endTime = System.currentTimeMillis();
        
        // Restore original output
        System.setOut(originalOut);
        String output = baos.toString();
        
        // Print the output
        System.out.print(output);
        
        // Extract bytes read from output
        long bytesRead = 0;
        String[] lines = output.split("\n");
        for (String line : lines) {
            if (line.contains("TOTAL BYTES READ FROM FILE:")) {
                String bytesStr = line.replaceAll(".*: ([0-9]+) bytes.*", "$1");
                try {
                    bytesRead = Long.parseLong(bytesStr);
                } catch (NumberFormatException e) {
                    // Ignore parsing errors
                }
                break;
            }
        }
        
        return new BenchmarkResult(endTime - startTime, actualThreads, bytesRead);
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
                System.out.println("  Removing existing directory: " + dirName);
                Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            System.err.println("  Warning: Could not delete " + path + ": " + e.getMessage());
                        }
                    });
            }
        } catch (IOException e) {
            System.err.println("  Warning: Error cleaning up " + dirName + ": " + e.getMessage());
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
            
        System.out.printf("%-25s | %-15s | %-15s | %-15s%n", 
            "Total Bytes Read", 
            String.format("%,d bytes", singleThreadResult.bytesRead),
            String.format("%,d bytes", multiThreadResult.bytesRead),
            String.format("%.1f%% diff", ((double)(multiThreadResult.bytesRead - singleThreadResult.bytesRead) / singleThreadResult.bytesRead) * 100));
        
        System.out.println("-".repeat(80));
        System.out.printf("%-25s | %-47s%n", 
            "File Integrity", 
            filesIdentical ? "✓ PASSED - All files identical" : "✗ FAILED - Files differ");
        
        System.out.println();
        System.out.println("Summary:");
        System.out.printf("• Multi-threading with %d threads achieved %.1f%% performance improvement%n", 
            multiThreadResult.actualThreads, improvement);
        System.out.printf("• Processing %,d vectors took %.2f seconds (vs %.2f seconds single-threaded)%n", 
            TOTAL_DOCS, multiThreadSec, singleThreadSec);
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
    
    private static void parseArguments(String[] args) {
        if (args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
            printUsage();
            System.exit(0);
        }
        
        for (String arg : args) {
            if (arg.startsWith("file=")) {
                FBIN_FILE = arg.substring(5);
            } else if (arg.startsWith("total_docs=")) {
                TOTAL_DOCS = Integer.parseInt(arg.substring(11));
            } else if (arg.startsWith("batch_size=")) {
                BATCH_SIZE = Integer.parseInt(arg.substring(11));
            } else if (arg.startsWith("threads=")) {
                String threadsValue = arg.substring(8);
                if ("all".equalsIgnoreCase(threadsValue)) {
                    THREADS = Runtime.getRuntime().availableProcessors();
                } else {
                    THREADS = Integer.parseInt(threadsValue);
                }
            } else if (!arg.isEmpty()) {
                System.err.println("Unknown argument: " + arg);
                printUsage();
                System.exit(1);
            }
        }
        
        // Calculate number of batches based on total docs and batch size
        NUM_BATCHES = (int) Math.ceil((double) TOTAL_DOCS / BATCH_SIZE);
        
        // Validate arguments
        if (TOTAL_DOCS <= 0) {
            System.err.println("Error: total_docs must be positive");
            System.exit(1);
        }
        if (BATCH_SIZE <= 0) {
            System.err.println("Error: batch_size must be positive");
            System.exit(1);
        }
        if (THREADS <= 0) {
            System.err.println("Error: threads must be positive");
            System.exit(1);
        }
        if (!new File(FBIN_FILE).exists()) {
            System.err.println("Error: File does not exist: " + FBIN_FILE);
            System.exit(1);
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: java PerformanceBenchmark [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  file=<filename>        Input .fbin file (default: base.1M.fbin)");
        System.out.println("  total_docs=<number>    Total number of documents to process (default: 50000)");
        System.out.println("  batch_size=<number>    Documents per batch (default: 6250)");
        System.out.println("  threads=<number|all>   Number of threads for multi-threaded benchmark (default: all available cores)");
        System.out.println("  -h, --help            Show this help message");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  java PerformanceBenchmark file=vectors.fbin total_docs=10000 batch_size=1000");
        System.out.println("  java PerformanceBenchmark total_docs=100000 threads=4");
        System.out.println("  java PerformanceBenchmark threads=all");
        System.out.println();
    }
}