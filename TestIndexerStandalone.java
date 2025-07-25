import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class TestIndexerStandalone {
    
    public static void main(String[] args) throws Exception {
        String inputFile = "./base.1M.fbin";
        String outputPrefix = "test_output";
        int batchSize = 1000;
        int totalDocs = 5000;
        
        System.out.println("Testing fvec file reading and batch creation...");
        System.out.println("Input file: " + inputFile);
        System.out.println("Batch size: " + batchSize);
        System.out.println("Total docs to process: " + totalDocs);
        
        List<float[]> vectors = readFvecs(inputFile, totalDocs);
        System.out.println("Vectors read: " + vectors.size());
        
        if (vectors.size() > 0) {
            System.out.println("First vector dimension: " + vectors.get(0).length);
            System.out.println("First 5 values of first vector:");
            for (int i = 0; i < Math.min(5, vectors.get(0).length); i++) {
                System.out.printf("%.6f ", vectors.get(0)[i]);
            }
            System.out.println("\n");
            
            // Create batches
            int batchCount = (vectors.size() + batchSize - 1) / batchSize;
            System.out.println("Will create " + batchCount + " batch files");
        }
    }
    
    public static List<float[]> readFvecs(String filePath, int numRows) throws IOException {
        List<float[]> vectors = new ArrayList<>();
        
        try (InputStream is = new FileInputStream(filePath)) {
            int count = 0;
            
            while (is.available() > 0 && count < numRows) {
                // Read dimension for each vector
                byte[] dimBytes = is.readNBytes(4);
                if (dimBytes.length < 4) break;
                
                ByteBuffer bb = ByteBuffer.wrap(dimBytes);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                int dimension = bb.getInt();
                
                if (dimension <= 0 || dimension > 10000) {
                    System.err.println("Invalid dimension: " + dimension);
                    break;
                }
                
                // Read the vector data
                float[] row = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    byte[] floatBytes = is.readNBytes(4);
                    if (floatBytes.length < 4) {
                        System.err.println("Incomplete vector data");
                        return vectors;
                    }
                    ByteBuffer bbf = ByteBuffer.wrap(floatBytes);
                    bbf.order(ByteOrder.LITTLE_ENDIAN);
                    row[i] = bbf.getFloat();
                }
                
                vectors.add(row);
                count++;
                
                if (count % 1000 == 0) {
                    System.out.print(".");
                }
            }
            System.out.println();
        }
        
        return vectors;
    }
}