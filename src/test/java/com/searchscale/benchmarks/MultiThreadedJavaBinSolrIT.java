package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiThreadedJavaBinSolrIT extends SolrCloudTestCase {
    
    private static final String SINGLE_COLLECTION_NAME = "single-threaded-test-collection";
    private static final String MULTI_COLLECTION_NAME = "multi-threaded-test-collection";
    private static final String CONFIG_NAME = "multithreaded-test-config";
    
    // Configuration for fast integrity testing: 5k vectors with 500 per batch
    private static final int VECTOR_COUNT = 5000; // 5k vectors for fast testing
    private static final int BATCH_SIZE = 500; // 500 documents per batch
    private static final int NUM_BATCHES = 10;
    private static final int VECTOR_DIMENSION = 768;
    private static final int NUM_THREADS = 6; // 6 threads as requested
    private static final String TEST_FBIN_FILE = "multithreaded-test-vectors.fbin";
    
    private static Path tempDir;
    private static MiniSolrCloudCluster cluster;
    
    @BeforeClass
    public static void setupCluster() throws Exception {
        // Force SSL off to avoid SSL issues in tests
        System.setProperty("tests.ssl", "false");
        System.setProperty("tests.clientauth", "false");
        
        // Create temp directory for JavaBin files
        tempDir = Files.createTempDirectory("multithreaded-javabin-test");
        
        // Generate sample test data file
        generateTestDataFile();
        
        // Generate JavaBin files using both single and multiple threads
        generateJavaBinFilesSingleThreaded();
        generateJavaBinFilesMultiThreaded();
        
        // Start MiniSolrCloudCluster with 1 node
        // Load configset from test resources
        Path configsetPath = Paths.get(MultiThreadedJavaBinSolrIT.class.getResource("/solr-configset").toURI());
        cluster = configureCluster(1)
                .addConfig(CONFIG_NAME, configsetPath)
                .configure();
        
        // Create both collections
        CollectionAdminRequest.createCollection(SINGLE_COLLECTION_NAME, CONFIG_NAME, 1, 1)
                .process(cluster.getSolrClient());
        CollectionAdminRequest.createCollection(MULTI_COLLECTION_NAME, CONFIG_NAME, 1, 1)
                .process(cluster.getSolrClient());
        
        // Wait for collections to be ready
        cluster.waitForActiveCollection(SINGLE_COLLECTION_NAME, 1, 1);
        cluster.waitForActiveCollection(MULTI_COLLECTION_NAME, 1, 1);
    }
    
    @AfterClass
    public static void tearDownCluster() throws Exception {
        if (cluster != null) {
            cluster.shutdown();
        }
        
        // Clean up temp directory
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception e) {
                            // Ignore
                        }
                    });
        }
    }
    
    private static void generateTestDataFile() throws Exception {
        // Generate test fbin file with same configuration as performance benchmark
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        TestDataGenerator.generateSampleFbinFile(testFilePath, VECTOR_COUNT, VECTOR_DIMENSION);
        System.out.println("Generated multi-threaded test data file with " + VECTOR_COUNT + " vectors");
    }
    
    private static void generateJavaBinFilesSingleThreaded() throws Exception {
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        
        // Generate JavaBin files using single thread for Solr compatibility
        String[] args = {
            "data_file=" + testFilePath,
            "output_dir=" + tempDir.resolve("single_batches").toString(),
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + VECTOR_COUNT,
            "overwrite=true",
            "threads=1"
        };
        
        System.out.println("Generating JavaBin files with 1 thread...");
        Indexer.main(args);
        System.out.println("Generated single-threaded batch files");
    }
    
    private static void generateJavaBinFilesMultiThreaded() throws Exception {
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        
        // Generate JavaBin files using the Indexer with multiple threads for Solr compatibility
        String[] args = {
            "data_file=" + testFilePath,
            "output_dir=" + tempDir.resolve("mt_batches").toString(),
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + VECTOR_COUNT,
            "overwrite=true",
            "threads=" + NUM_THREADS
        };
        
        System.out.println("Generating JavaBin files with " + NUM_THREADS + " threads...");
        Indexer.main(args);
        System.out.println("Generated " + NUM_BATCHES + " batch files using " + NUM_THREADS + " threads");
    }
    
    @Test
    public void testSingleVsMultiThreadedDataIntegrity() throws Exception {
        SolrClient solrClient = cluster.getSolrClient();
        
        System.out.println("Testing single vs multi-threaded JavaBin generation and data integrity...");
        
        // Upload single-threaded data
        uploadDataToSolr(solrClient, SINGLE_COLLECTION_NAME, "single-threaded");
        
        // Upload multi-threaded data  
        uploadDataToSolr(solrClient, MULTI_COLLECTION_NAME, "multi-threaded");
        
        // Compare the two indexes document by document
        compareIndexes(solrClient);
        
        System.out.println("✓ Data integrity test completed successfully!");
    }
    
    private void uploadDataToSolr(SolrClient solrClient, String collectionName, String mode) throws Exception {
        System.out.println("Uploading " + mode + " data to collection: " + collectionName);
        
        // Use the JavaBin files generated by the Indexer
        String batchDir = mode.equals("single-threaded") ? "single_batches" : "mt_batches";
        Path batchPath = tempDir.resolve(batchDir);
        
        System.out.println("Using JavaBin batch files from: " + batchPath);
        
        // Upload each JavaBin batch file using Solr's JavaBin update endpoint
        int uploadedDocs = 0;
        for (int i = 0; i < NUM_BATCHES; i++) {
            Path batchFile = batchPath.resolve("batch." + i);
            
            if (Files.exists(batchFile)) {
                // Upload JavaBin file directly to Solr using the update endpoint
                int docsInBatch = uploadJavaBinFile(solrClient, collectionName, batchFile, i);
                uploadedDocs += docsInBatch;
                
                System.out.println(mode + " uploaded batch " + i + " with " + docsInBatch + " documents from JavaBin file");
            } else {
                System.out.println("Warning: Batch file " + i + " not found at " + batchFile);
            }
        }
        
        // Commit all changes
        solrClient.commit(collectionName);
        
        // Verify document count
        SolrQuery query = new SolrQuery("*:*");
        query.setRows(0);
        
        QueryResponse response = solrClient.query(collectionName, query);
        long numFound = response.getResults().getNumFound();
        
        assertEquals("Expected " + VECTOR_COUNT + " documents in " + mode + " collection but found " + numFound,
            VECTOR_COUNT, numFound);
        
        System.out.println("✓ Successfully uploaded and verified " + numFound + " documents in " + mode + " collection from JavaBin files");
    }
    
    private int uploadJavaBinFile(SolrClient solrClient, String collectionName, Path batchFile, int batchIndex) throws Exception {
        // Use GenericSolrRequest to upload JavaBin file directly to Solr
        GenericSolrRequest uploadRequest = new GenericSolrRequest(SolrRequest.METHOD.POST, 
            "/update", 
            new MapSolrParams(Map.of("commit", "false", "overwrite", "true", "collection", collectionName)))
            .setContentWriter(new RequestWriter.ContentWriter() {
                @Override
                public void write(OutputStream os) throws IOException {
                    // Stream the JavaBin file directly to the output stream
                    Files.copy(batchFile, os);
                }

                @Override
                public String getContentType() {
                    return "application/javabin";
                }
            });
        
        // Execute the request
        solrClient.request(uploadRequest);
        
        // Estimate document count from file size (rough estimate for logging)
        long fileSize = Files.size(batchFile);
        int estimatedDocs = (int) (fileSize / 3000); // Rough estimate: ~3KB per document
        
        System.out.println("Successfully uploaded JavaBin file " + batchFile.getFileName() + 
            " (" + fileSize + " bytes, ~" + estimatedDocs + " docs estimated)");
        
        return estimatedDocs;
    }
    
    private void compareIndexes(SolrClient solrClient) throws Exception {
        System.out.println("Comparing single-threaded vs multi-threaded indexes...");
        
        // Get total counts
        SolrQuery countQuery = new SolrQuery("*:*").setRows(0);
        
        QueryResponse singleResponse = solrClient.query(SINGLE_COLLECTION_NAME, countQuery);
        QueryResponse multiResponse = solrClient.query(MULTI_COLLECTION_NAME, countQuery);
        
        long singleCount = singleResponse.getResults().getNumFound();
        long multiCount = multiResponse.getResults().getNumFound();
        
        assertEquals("Document counts should match between collections", singleCount, multiCount);
        System.out.println("✓ Document counts match: " + singleCount + " documents in both collections");
        
        // Compare documents by ID
        compareDocumentsByIds(solrClient);
        
        // Compare vector content for sample documents
        compareVectorContent(solrClient);
        
        System.out.println("✓ Index comparison completed successfully!");
    }
    
    private void compareDocumentsByIds(SolrClient solrClient) throws Exception {
        System.out.println("Comparing documents in batches of 100 between collections...");
        
        final int BATCH_SIZE_COMPARE = 100; // Smaller batches for 5k dataset
        int totalDocsCompared = 0;
        int start = 0;
        
        while (start < VECTOR_COUNT) {
            int rows = Math.min(BATCH_SIZE_COMPARE, VECTOR_COUNT - start);
            
            // Get batch from both collections
            SolrQuery batchQuery = new SolrQuery("*:*");
            batchQuery.setFields("id", "article_vector");
            batchQuery.setRows(rows);
            batchQuery.setStart(start);
            batchQuery.setSort("id", SolrQuery.ORDER.asc);
            
            QueryResponse singleResponse = solrClient.query(SINGLE_COLLECTION_NAME, batchQuery);
            QueryResponse multiResponse = solrClient.query(MULTI_COLLECTION_NAME, batchQuery);
            
            assertEquals("Batch starting at " + start + " should have same number of documents", 
                         singleResponse.getResults().size(), 
                         multiResponse.getResults().size());
            
            // Compare each document in this batch
            for (int i = 0; i < singleResponse.getResults().size(); i++) {
                String singleId = (String) singleResponse.getResults().get(i).get("id");
                String multiId = (String) multiResponse.getResults().get(i).get("id");
                
                assertEquals("Document IDs should match at batch position " + (start + i), singleId, multiId);
                
                // Compare vector content for each document in this batch
                @SuppressWarnings("unchecked")
                List<Float> singleVector = (List<Float>) singleResponse.getResults().get(i).get("article_vector");
                @SuppressWarnings("unchecked")
                List<Float> multiVector = (List<Float>) multiResponse.getResults().get(i).get("article_vector");
                
                assertNotNull("Single collection should have vector for doc " + singleId, singleVector);
                assertNotNull("Multi collection should have vector for doc " + multiId, multiVector);
                
                assertEquals("Vector dimensions should match for doc " + singleId, 
                            singleVector.size(), multiVector.size());
                
                // Compare each vector component
                for (int j = 0; j < singleVector.size(); j++) {
                    assertEquals("Vector component " + j + " should match for doc " + singleId, 
                               singleVector.get(j), multiVector.get(j), 0.0001f);
                }
                
                totalDocsCompared++;
            }
            
            System.out.println("✓ Compared batch " + (start/BATCH_SIZE_COMPARE + 1) + 
                              " (" + singleResponse.getResults().size() + " documents)" +
                              " - Total compared: " + totalDocsCompared);
            
            start += rows;
        }
        
        System.out.println("✓ Document-by-document comparison completed: " + 
                          totalDocsCompared + " documents verified");
    }
    
    private void compareVectorContent(SolrClient solrClient) throws Exception {
        System.out.println("Comparing Solr vectors against original source file...");
        
        // Read original vectors from the source .fbin file
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        List<float[]> originalVectors = new ArrayList<>();
        FBIvecsReader.readFvecs(testFilePath, VECTOR_COUNT, originalVectors);
        
        System.out.println("Read " + originalVectors.size() + " original vectors from source file");
        
        // Test sample documents from both collections against original source
        int[] sampleIds = {0, 100, 500, 1000, 2500, 4999}; // Various points in the dataset
        
        for (int docId : sampleIds) {
            if (docId >= originalVectors.size()) continue;
            
            float[] originalVector = originalVectors.get(docId);
            
            // Verify against single-threaded collection
            verifySolrVectorAgainstOriginal(solrClient, SINGLE_COLLECTION_NAME, docId, originalVector, "single-threaded");
            
            // Verify against multi-threaded collection  
            verifySolrVectorAgainstOriginal(solrClient, MULTI_COLLECTION_NAME, docId, originalVector, "multi-threaded");
        }
        
        System.out.println("✓ All sampled vectors match original source file data");
        
        // Additional comprehensive verification for first 50 documents
        performComprehensiveVectorVerification(solrClient, originalVectors);
    }
    
    private void verifySolrVectorAgainstOriginal(SolrClient solrClient, String collectionName, 
                                                int docId, float[] originalVector, String mode) throws Exception {
        
        SolrQuery query = new SolrQuery("id:" + docId);
        query.setFields("id", "article_vector");
        QueryResponse response = solrClient.query(collectionName, query);
        
        assertEquals("Should find exactly one document with id " + docId + " in " + mode, 
                    1, response.getResults().getNumFound());
        
        @SuppressWarnings("unchecked")
        List<Float> solrVector = (List<Float>) response.getResults().get(0).get("article_vector");
        
        assertTrue("Vector should not be null for doc " + docId + " in " + mode, solrVector != null);
        assertEquals("Vector dimension mismatch for doc " + docId + " in " + mode, 
                    originalVector.length, solrVector.size());
        
        // Compare each float value with tolerance for floating-point precision
        for (int i = 0; i < originalVector.length; i++) {
            float originalValue = originalVector[i];
            float solrValue = solrVector.get(i);
            
            assertEquals("Vector component " + i + " mismatch for doc " + docId + " in " + mode + 
                        " (original: " + originalValue + ", solr: " + solrValue + ")", 
                        originalValue, solrValue, 0.0001f);
        }
        
        System.out.println("✓ Doc " + docId + " vector verified against source (" + mode + ")");
    }
    
    private void performComprehensiveVectorVerification(SolrClient solrClient, List<float[]> originalVectors) throws Exception {
        System.out.println("Performing comprehensive vector verification for first 50 documents...");
        
        // Verify first 50 documents comprehensively
        int verifyCount = Math.min(50, originalVectors.size());
        
        SolrQuery batchQuery = new SolrQuery("*:*");
        batchQuery.setFields("id", "article_vector");
        batchQuery.setRows(verifyCount);
        batchQuery.setSort("id", SolrQuery.ORDER.asc);
        
        // Test both collections
        QueryResponse singleResponse = solrClient.query(SINGLE_COLLECTION_NAME, batchQuery);
        QueryResponse multiResponse = solrClient.query(MULTI_COLLECTION_NAME, batchQuery);
        
        for (int i = 0; i < Math.min(verifyCount, singleResponse.getResults().size()); i++) {
            int docId = Integer.parseInt((String) singleResponse.getResults().get(i).get("id"));
            if (docId >= originalVectors.size()) continue;
            
            float[] originalVector = originalVectors.get(docId);
            
            // Verify single-threaded
            @SuppressWarnings("unchecked")
            List<Float> singleVector = (List<Float>) singleResponse.getResults().get(i).get("article_vector");
            verifyVectorMatch(originalVector, singleVector, docId, "single-threaded");
            
            // Verify multi-threaded
            if (i < multiResponse.getResults().size()) {
                @SuppressWarnings("unchecked")
                List<Float> multiVector = (List<Float>) multiResponse.getResults().get(i).get("article_vector");
                verifyVectorMatch(originalVector, multiVector, docId, "multi-threaded");
            }
        }
        
        System.out.println("✓ Comprehensive verification completed for " + verifyCount + " documents");
    }
    
    private void verifyVectorMatch(float[] original, List<Float> solr, int docId, String mode) {
        assertEquals("Vector dimension mismatch for doc " + docId + " (" + mode + ")", 
                    original.length, solr.size());
        
        for (int i = 0; i < original.length; i++) {
            assertEquals("Component " + i + " mismatch for doc " + docId + " (" + mode + ")", 
                        original[i], solr.get(i), 0.0001f);
        }
    }
    
    private void testVectorFieldIntegrity(SolrClient solrClient, String collectionName) throws Exception {
        System.out.println("Testing vector field integrity for " + collectionName + "...");
        
        // Get a sample document and verify vector dimensions
        SolrQuery sampleQuery = new SolrQuery("id:0");
        QueryResponse sampleResponse = solrClient.query(collectionName, sampleQuery);
        
        assertEquals("Should find exactly one document", 1, sampleResponse.getResults().size());
        
        Object vectorField = sampleResponse.getResults().get(0).get("article_vector");
        assertTrue("Vector field should be a List", vectorField instanceof List);
        
        @SuppressWarnings("unchecked")
        List<Float> vector = (List<Float>) vectorField;
        assertEquals("Vector should have " + VECTOR_DIMENSION + " dimensions", 
            VECTOR_DIMENSION, vector.size());
        
        // Verify all values are valid floats
        for (Float value : vector) {
            assertTrue("Vector value should be a valid float", 
                value != null && !value.isNaN() && !value.isInfinite());
        }
        
        System.out.println("✓ Vector field integrity test passed for " + collectionName);
    }
    
    @Test
    public void testMultiThreadedJavaBinUploadAndQuery() throws Exception {
        // This test maintains the original functionality for backward compatibility
        SolrClient solrClient = cluster.getSolrClient();
        
        System.out.println("Testing multi-threaded collection functionality...");
        
        // Upload data to both collections first
        uploadDataToSolr(solrClient, SINGLE_COLLECTION_NAME, "single-threaded");
        uploadDataToSolr(solrClient, MULTI_COLLECTION_NAME, "multi-threaded");
        
        // Test vector field integrity for both collections
        testVectorFieldIntegrity(solrClient, SINGLE_COLLECTION_NAME);
        testVectorFieldIntegrity(solrClient, MULTI_COLLECTION_NAME);
        
        // Test document retrieval for both collections
        testDocumentRetrieval(solrClient, SINGLE_COLLECTION_NAME);
        testDocumentRetrieval(solrClient, MULTI_COLLECTION_NAME);
        
        System.out.println("✓ Multi-threaded JavaBin upload and query test passed");
    }
    
    private void testDocumentRetrieval(SolrClient solrClient, String collectionName) throws Exception {
        System.out.println("Testing document retrieval for " + collectionName + "...");
        
        // Test documents from first, middle, and last batches
        int[] testIds = {0, BATCH_SIZE/2, BATCH_SIZE, VECTOR_COUNT/2, VECTOR_COUNT-1};
        
        for (int testId : testIds) {
            SolrQuery idQuery = new SolrQuery("id:" + testId);
            QueryResponse idResponse = solrClient.query(collectionName, idQuery);
            assertEquals("Should find document with id:" + testId + " in " + collectionName, 
                1, idResponse.getResults().getNumFound());
            
            // Verify the document has vector field
            assertTrue("Document " + testId + " should contain article_vector field in " + collectionName,
                idResponse.getResults().get(0).containsKey("article_vector"));
        }
        
        System.out.println("✓ Document retrieval test passed for " + collectionName);
    }
    
    @Test
    public void testMultiThreadedPerformanceMetrics() throws Exception {
        System.out.println("Verifying multi-threaded processing configuration...");
        
        // With the updated chunking approach, we create files based on batch size, not thread count
        // So we expect NUM_BATCHES files (VECTOR_COUNT / BATCH_SIZE)
        int expectedFiles = NUM_BATCHES;
        int actualBatchFiles = 0;
        int totalDocsInFiles = 0;
        
        for (int i = 0; i < expectedFiles; i++) {
            Path batchFile = tempDir.resolve("mt_batches").resolve("batch." + i);
            if (Files.exists(batchFile)) {
                actualBatchFiles++;
                
                // Verify file is not empty
                long fileSize = Files.size(batchFile);
                assertTrue("Batch file " + i + " should not be empty", fileSize > 0);
                
                // Rough estimate of documents in file (each document ~3KB in JavaBin format)
                int estimatedDocs = (int) (fileSize / 3000);
                totalDocsInFiles += estimatedDocs;
                
                System.out.println("Batch file " + i + ": " + fileSize + " bytes (~" + estimatedDocs + " docs)");
            }
        }
        
        assertEquals("Should have created " + expectedFiles + " chunk files (one per thread)", 
            expectedFiles, actualBatchFiles);
        
        // Verify that we have approximately the expected number of documents across all files
        // Note: File size estimation is rough, so allow larger tolerance
        assertTrue("Total estimated documents (" + totalDocsInFiles + ") should be close to expected (" + VECTOR_COUNT + ")",
            Math.abs(totalDocsInFiles - VECTOR_COUNT) < VECTOR_COUNT * 0.4); // Within 40% tolerance for rough estimation
        
        System.out.println("✓ Multi-threaded processing created " + actualBatchFiles + " chunk files");
        System.out.println("✓ Configuration: " + NUM_THREADS + " threads, " + 
            BATCH_SIZE + " docs/batch, " + NUM_BATCHES + " batches, " + VECTOR_COUNT + " total docs");
        System.out.println("✓ Estimated total documents in files: " + totalDocsInFiles);
    }
}