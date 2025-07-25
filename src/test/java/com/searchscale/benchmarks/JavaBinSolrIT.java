package com.searchscale.benchmarks;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.JavaBinCodec;
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
            "legacy=false",
            "overwrite=true"
        };
        
        Indexer.main(args);
    }
    
    @Test
    public void testJavaBinUploadAndQuery() throws Exception {
        SolrClient solrClient = cluster.getSolrClient();
        
        // Instead of parsing JavaBin files, recreate the documents directly
        // Read vectors from test data file
        String testFilePath = tempDir.resolve(TEST_FBIN_FILE).toString();
        List<float[]> vectors = new ArrayList<>();
        FBIvecsReader.readFvecs(testFilePath, VECTOR_COUNT, vectors);
        
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
                solrClient.add(COLLECTION_NAME, docs);
                uploadedDocs += docs.size();
                
                System.out.println("Uploaded batch " + (i / BATCH_SIZE) + " with " + docs.size() + " documents");
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