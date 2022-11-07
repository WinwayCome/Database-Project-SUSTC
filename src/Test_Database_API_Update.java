import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Test_Database_API_Update {
    private static Connection conn = null;
    private static Statement stmt = null;

    static BufferedReader file;
    Map<String, String> dp_set = new HashMap<>();
    private static void Create_Conn() {
        String url = "jdbc:postgresql://localhost:5432/sustc_test_2";
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
        try {
            file = new BufferedReader(new FileReader("source/test_1w.csv"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String line = null;
        long start = System.currentTimeMillis();
        while (true) {
            try {
                if ((line = file.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            String[] p = line.split(",", Integer.MAX_VALUE);
            for(int i = 0; i <= 25; ++i)
            {
                if(p[i].equals("")) p[i] = "null";
                else p[i] = "'" + p[i] + "'";
            }
            String s = String.format("insert into sustc_test_2.public.item(item_name, item_type, item_price, retrieval_city, retrieval_starttime, retrieval_courier, retrieval_courier_gender, retrieval_courier_phonenumber, retrieval_courier_age, delivery_finishedtime, delivery_city, delivery_courier, delivery_courier_gender, delivery_courier_phonenumber, delivery_courier_age, item_export_city, item_export_tax, item_export_time, item_import_city, item_import_tax, item_import_time, container_code, container_type, ship_name, company_name, log_time) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict(item_name) do update" +
                            " set item_name = %s, item_type = %s, item_price = %s, retrieval_city = %s, retrieval_starttime = %s, retrieval_courier = %s, retrieval_courier_gender = %s, retrieval_courier_phonenumber = %s, retrieval_courier_age = %s, delivery_finishedtime = %s, delivery_city = %s, delivery_courier = %s, delivery_courier_gender = %s, delivery_courier_phonenumber = %s, delivery_courier_age = %s, item_export_city = %s, item_export_tax = %s, item_export_time = %s, item_import_city = %s, item_import_tax = %s, item_import_time = %s, container_code = %s, container_type = %s, ship_name = %s, company_name = %s, log_time = %s",
                    p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19], p[20], p[21], p[22], p[23], p[24], p[25],p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11], p[12], p[13], p[14], p[15], p[16], p[17], p[18], p[19], p[20], p[21], p[22], p[23], p[24], p[25]);
            //System.out.println(s);
            stmt.execute(s);
        }
        System.out.println("Update Time = " + (System.currentTimeMillis()-start) + " ms");
    }
}
