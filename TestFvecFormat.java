import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TestFvecFormat {
    public static void main(String[] args) throws IOException {
        String filePath = "./base.1M.fbin";
        
        try (InputStream is = new FileInputStream(filePath)) {
            // Read first int - might be total count
            byte[] b = is.readNBytes(4);
            ByteBuffer bb = ByteBuffer.wrap(b);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            int firstInt = bb.getInt();
            System.out.println("First int: " + firstInt);
            
            // Read second int - might be dimension
            b = is.readNBytes(4);
            bb = ByteBuffer.wrap(b);
            bb.order(ByteOrder.LITTLE_ENDIAN);
            int secondInt = bb.getInt();
            System.out.println("Second int: " + secondInt);
            
            // Calculate expected vector size
            long fileSize = new File(filePath).length();
            System.out.println("File size: " + fileSize + " bytes");
            
            // If first int is count, calculate dimension
            if (firstInt == 1000000) {
                long dataSize = fileSize - 8; // minus two ints
                long bytesPerVector = dataSize / firstInt;
                long dimension = bytesPerVector / 4; // 4 bytes per float
                System.out.println("Calculated dimension (if first int is count): " + dimension);
            }
            
            // Read next few floats to check data
            System.out.println("\nNext 5 floats:");
            for (int i = 0; i < 5; i++) {
                b = is.readNBytes(4);
                bb = ByteBuffer.wrap(b);
                bb.order(ByteOrder.LITTLE_ENDIAN);
                float f = bb.getFloat();
                System.out.printf("%.6f ", f);
            }
            System.out.println();
        }
    }
}