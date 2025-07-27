package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JavaBinSolrIT extends SolrCloudTestCase {
    
    private static final String COLLECTION_NAME = "test-collection";
    private static final String CONFIG_NAME = "test-config";
    private static final int VECTOR_COUNT = 100; // Number of vectors to test with
    private static final int BATCH_SIZE = 25;
    private static final int VECTOR_DIMENSION = 768;
    private static final String TEST_FBIN_FILE = "test-vectors.fbin";
    private static Path tempDir;
    private static MiniSolrCloudCluster cluster;
    
    @BeforeClass
    public static void setupCluster() throws Exception {
        // Force SSL off to avoid SSL issues in tests
        System.setProperty("tests.ssl", "false");
        System.setProperty("tests.clientauth", "false");
        
        // Create temp directory for JavaBin files
        tempDir = Files.createTempDirectory("javabin-test");
        
        // Generate sample test data file
        generateTestDataFile();
        
        // Generate JavaBin files first
        generateJavaBinFiles();
        
        // Start MiniSolrCloudCluster with 1 node
        // Load configset from test resources
        Path configsetPath = Paths.get(JavaBinSolrIT.class.getResource("/solr-configset").toURI());
        cluster = configureCluster(1)
                .addConfig(CONFIG_NAME, configsetPath)
                .configure();
        
        // Create collection
        CollectionAdminRequest.createCollection(COLLECTION_NAME, CONFIG_NAME, 1, 1)
                .process(cluster.getSolrClient());
        
        // Wait for collection to be ready
        cluster.waitForActiveCollection(COLLECTION_NAME, 1, 1);
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
        // Generate small test fbin file
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        TestDataGenerator.generateSampleFbinFile(testFilePath, VECTOR_COUNT, VECTOR_DIMENSION);
    }
    
    private static void generateJavaBinFiles() throws Exception {
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        
        // Generate JavaBin files using the Indexer
        String[] args = {
            "data_file=" + testFilePath,
            "output_dir=" + tempDir.resolve("batches").toString(),
            "batch_size=" + BATCH_SIZE,
            "docs_count=" + VECTOR_COUNT,
            "overwrite=true"
        };
        
        Indexer.main(args);
    }
    
    @Test
    public void testJavaBinUploadAndQuery() throws Exception {
        SolrClient solrClient = cluster.getSolrClient();
        
        // Upload the generated JavaBin files directly to Solr
        Path batchDir = tempDir.resolve("batches");
        int uploadedDocs = 0;
        
        // Calculate number of batches based on documents and batch size
        int numBatches = (VECTOR_COUNT + BATCH_SIZE - 1) / BATCH_SIZE;
        
        for (int i = 0; i < numBatches; i++) {
            Path batchFile = batchDir.resolve("batch." + i);
            
            if (Files.exists(batchFile)) {
                // Upload JavaBin file directly using GenericSolrRequest
                GenericSolrRequest uploadRequest = new GenericSolrRequest(SolrRequest.METHOD.POST, 
                    "/update", 
                    new MapSolrParams(Map.of("commit", "false", "overwrite", "true", "collection", COLLECTION_NAME)))
                    .setContentWriter(new RequestWriter.ContentWriter() {
                        @Override
                        public void write(OutputStream os) throws IOException {
                            // Stream the entire JavaBin file directly to Solr
                            Files.copy(batchFile, os);
                        }

                        @Override
                        public String getContentType() {
                            return "application/javabin";
                        }
                    });
                
                // Execute the request to post the entire JavaBin file
                solrClient.request(uploadRequest);
                
                // Calculate actual documents in this batch
                int docsInBatch = (i == numBatches - 1 && VECTOR_COUNT % BATCH_SIZE != 0) 
                    ? VECTOR_COUNT % BATCH_SIZE 
                    : BATCH_SIZE;
                uploadedDocs += docsInBatch;
                
                System.out.println("Uploaded batch " + i + " with " + docsInBatch + " documents");
            }
        }
        
        // Commit all changes
        solrClient.commit(COLLECTION_NAME);
        
        // Verify document count
        SolrQuery query = new SolrQuery("*:*");
        query.setRows(0);
        
        QueryResponse response = solrClient.query(COLLECTION_NAME, query);
        long numFound = response.getResults().getNumFound();
        
        assertEquals("Expected " + VECTOR_COUNT + " documents but found " + numFound,
            VECTOR_COUNT, numFound);
        
        System.out.println("Successfully uploaded and verified " + numFound + " documents in Solr");
        
        // Test querying a specific document
        SolrQuery idQuery = new SolrQuery("id:0");
        QueryResponse idResponse = solrClient.query(COLLECTION_NAME, idQuery);
        assertEquals("Should find document with id:0", 
            1, idResponse.getResults().getNumFound());
        
        // Verify the document has vector field
        assertTrue("Document should contain article_vector field",
            idResponse.getResults().get(0).containsKey("article_vector"));
    }
}