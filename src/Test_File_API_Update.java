import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Test_File_API_Update {
    static BufferedReader file_data = null;
    static BufferedReader file_test = null;
    static BufferedWriter file_result = null;
    static Map<String, String> dp_map = new HashMap<>();

    public static void CreateConnection() {
        try {
            file_data = new BufferedReader(new FileReader("source/data.csv"));
            file_test = new BufferedReader(new FileReader("source/test_1w.csv"));
            file_result = new BufferedWriter(new FileWriter("source/test_insert_load.csv"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        CreateConnection();
        String line_data = null;
        String line_test = null;
        String test = "orange-4d912,orange,223651735,Austin,2019-08-01,车信元,男,4439-1677974796,28,2019-10-06,拉萨,孟琰,女,3629-1507236065,32.0,Atlanta,130826381.06448428,2019-08-21,香港,187884852.45721823,2019-09-21,cb73117e,Dry Container,清和号,京东,2019-10-06 11:06:33";
        long start = System.currentTimeMillis();
        while (true) {
            if ((line_test = file_test.readLine()) == null) break;
            String[] p = line_test.split(",", 2);
            dp_map.put(p[0], line_test);
        }
        while (true) {
            if ((line_data = file_data.readLine()) == null) break;
            String[] parts=line_data.split(",", 2);
            if (!dp_map.containsKey(parts[0])) {
                file_result.write(line_data);
                file_result.newLine();
            } else {
                file_result.write(dp_map.get(parts[0]));
                file_result.newLine();
            }
        }
        System.out.println("Update Time = " + (System.currentTimeMillis() - start) + " ms");
        file_result.close();
        file_data.close();
        file_test.close();
    }
}

