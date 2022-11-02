import java.io.*;
import java.lang.String;
public class Test_File_API_SELECT {
    static BufferedReader file_data= null;
    static BufferedReader file_test= null;
    static BufferedWriter file_result= null;

    public static void CreateConnection()
    {
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
        long start = System.currentTimeMillis();
        file_data.mark(2000000000);
        while(true)
        {
            if((line_test = file_test.readLine()) == null) break;
            file_data.reset();
            while(true)
            {
                if((line_data = file_data.readLine()) == null) break;
                if(line_test.equals(line_data)) {
                    file_result.write(line_data);
                    file_result.newLine();
                    break;
                }
            }
            //file_result.flush();
        }
        System.out.println("Select Time = " + (System.currentTimeMillis() - start) + " ms");
        file_result.close();
        file_data.close();
        file_test.close();
    }
}
