package com.searchscale.benchmarks;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class TestDataGenerator {
    
    public static void generateSampleFbinFile(String filePath, int vectorCount, int dimension) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            // Write header: total count (4 bytes, little-endian)
            ByteBuffer countBuffer = ByteBuffer.allocate(4);
            countBuffer.order(ByteOrder.LITTLE_ENDIAN);
            countBuffer.putInt(vectorCount);
            fos.write(countBuffer.array());
            
            // Write dimension (4 bytes, little-endian)
            ByteBuffer dimBuffer = ByteBuffer.allocate(4);
            dimBuffer.order(ByteOrder.LITTLE_ENDIAN);
            dimBuffer.putInt(dimension);
            fos.write(dimBuffer.array());
            
            // Generate random vectors
            Random random = new Random(42); // Fixed seed for reproducible tests
            for (int i = 0; i < vectorCount; i++) {
                for (int j = 0; j < dimension; j++) {
                    // Generate random float between -1.0 and 1.0
                    float value = (random.nextFloat() - 0.5f) * 2.0f;
                    
                    ByteBuffer floatBuffer = ByteBuffer.allocate(4);
                    floatBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    floatBuffer.putFloat(value);
                    fos.write(floatBuffer.array());
                }
            }
        }
    }
}