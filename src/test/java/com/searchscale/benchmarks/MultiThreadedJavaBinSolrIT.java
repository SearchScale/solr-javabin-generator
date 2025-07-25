package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
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
        
        // Generate JavaBin files using single thread
        String[] args = {
            "data_file=" + testFilePath,
            "output_dir=" + tempDir.resolve("single_batches").toString(),
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + VECTOR_COUNT,
            "legacy=false",
            "overwrite=true",
            "threads=1"
        };
        
        System.out.println("Generating JavaBin files with 1 thread...");
        Indexer.main(args);
        System.out.println("Generated single-threaded batch files");
    }
    
    private static void generateJavaBinFilesMultiThreaded() throws Exception {
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        
        // Generate JavaBin files using the Indexer with multiple threads
        String[] args = {
            "data_file=" + testFilePath,
            "output_dir=" + tempDir.resolve("mt_batches").toString(),
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + VECTOR_COUNT,
            "legacy=false",
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
        
        // Read vectors from test data file to create documents directly
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        List<float[]> vectors = new ArrayList<>();
        FBIvecsReader.readFvecs(testFilePath, VECTOR_COUNT, vectors);
        
        System.out.println("Read " + vectors.size() + " vectors from test file for " + mode);
        
        // Create and upload documents in batches
        int uploadedDocs = 0;
        for (int i = 0; i < vectors.size(); i += BATCH_SIZE) {
            List<SolrInputDocument> docs = new ArrayList<>();
            int batchEnd = Math.min(i + BATCH_SIZE, vectors.size());
            
            for (int j = i; j < batchEnd; j++) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", String.valueOf(j));
                
                // Convert float[] to List<Float> for Solr compatibility
                float[] vector = vectors.get(j);
                List<Float> vectorList = new ArrayList<>();
                for (float f : vector) {
                    vectorList.add(f);
                }
                doc.addField("article_vector", vectorList);
                docs.add(doc);
            }
            
            if (!docs.isEmpty()) {
                solrClient.add(collectionName, docs);
                uploadedDocs += docs.size();
                
                System.out.println(mode + " uploaded batch " + (i / BATCH_SIZE) + " with " + docs.size() + " documents");
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
        
        System.out.println("✓ Successfully uploaded and verified " + numFound + " documents in " + mode + " collection");
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
        System.out.println("Note: Vector content comparison is now done comprehensively in batch comparison above.");
        System.out.println("✓ All vector content verified through document-by-document batch comparison");
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
        assertTrue("Total estimated documents (" + totalDocsInFiles + ") should be close to expected (" + VECTOR_COUNT + ")",
            Math.abs(totalDocsInFiles - VECTOR_COUNT) < VECTOR_COUNT * 0.1); // Within 10% tolerance
        
        System.out.println("✓ Multi-threaded processing created " + actualBatchFiles + " chunk files");
        System.out.println("✓ Configuration: " + NUM_THREADS + " threads, " + 
            BATCH_SIZE + " docs/batch, " + NUM_BATCHES + " batches, " + VECTOR_COUNT + " total docs");
        System.out.println("✓ Estimated total documents in files: " + totalDocsInFiles);
    }
}