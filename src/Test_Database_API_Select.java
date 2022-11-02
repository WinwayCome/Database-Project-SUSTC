import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class Test_Database_API_Select {
    private static Connection conn = null;
    private static Statement stmt = null;

    static BufferedReader file;

    private static void Create_Conn() {
        String url = "jdbc:postgresql://localhost:5432/sustc_3";
        String user = "postgres";
        String password = "148891";
        try {
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            System.out.println("Postgres connection successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) throws SQLException {
        Create_Conn();
        String line = null;
        try {
            file = new BufferedReader(new FileReader("source/test_1w.csv"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            line = file.readLine();
            System.out.println("File is read successfully.");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        long start =System.currentTimeMillis();
        while (true) {
            try {
                if ((line = file.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            String[] parts = line.split(",", Integer.MAX_VALUE);
            ResultSet rs = stmt.executeQuery("select * from item where item_name = '" + parts[0] + "'");
            ResultSet rs2;
            if (!parts[13].equals(""))
                rs2 = stmt.executeQuery("select courier_ph_num from courier where courier_ph_num = '" + parts[13] + "'");
        }
        System.out.println("Time = " +(System.currentTimeMillis() - start)*5 + "ms");
    }
}
