import com.searchscale.benchmarks.FBIvecsReader;
import java.util.ArrayList;
import java.util.List;

public class TestFBinReader {
    public static void main(String[] args) {
        String filePath = "./base.1M.fbin";
        List<float[]> vectors = new ArrayList<>();
        
        // Try to read just 10 vectors to test
        FBIvecsReader.readFvecs(filePath, 10, vectors);
        
        System.out.println("Vectors read: " + vectors.size());
        if (vectors.size() > 0) {
            System.out.println("First vector dimension: " + vectors.get(0).length);
            System.out.println("First 5 values of first vector: ");
            for (int i = 0; i < Math.min(5, vectors.get(0).length); i++) {
                System.out.print(vectors.get(0)[i] + " ");
            }
            System.out.println();
        }
    }
}