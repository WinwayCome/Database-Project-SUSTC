import java.io.*;

public class Test_File_API_Insert {
    static BufferedReader file_data = null;
    static BufferedReader file_test = null;
    static BufferedWriter file_result = null;

    public static void CreateConnection() {
        try {
            file_test = new BufferedReader(new FileReader("source/test_1w.csv"));
            file_result = new BufferedWriter(new FileWriter("source/test_insert_load.csv", true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        CreateConnection();
        String line_data = null;
        String line_test = null;
        long start = System.currentTimeMillis();
        while ((line_test = file_test.readLine()) != null) {
            String[] p = line_test.split(",", Integer.MAX_VALUE);
            for (int i = 0; i <= 25; ++i) {
                if (p[i].equals("")) p[i] = "null";
                else p[i] = "'" + p[i] + "'";
            }
            String s = String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19], p[20], p[21], p[22], p[23], p[24], p[25]);
            //System.out.println(s);
            file_result.write(s);
            //file_result.flush();
        }
        System.out.println("Select Time = " + (System.currentTimeMillis() - start) + " ms");
        file_result.close();
        file_test.close();
    }
}
