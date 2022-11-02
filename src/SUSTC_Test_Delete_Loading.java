import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SUSTC_Test_Delete_Loading {
    private static Connection conn = null;
    private static Statement stmt = null;

    static BufferedReader file;

    public static Map<String, Integer> company = new HashMap<>();
    public static Map<String, Integer> courier = new HashMap<>();
    public static Map<String, Integer> inland = new HashMap<>();
    public static Map<String, Integer> port = new HashMap<>();
    public static Map<String, Integer> ship = new HashMap<>();
    public static Set<String> container = new HashSet<>();

    private static void Create_Conn() {
        String url = "jdbc:postgresql://localhost:5432/sustc_test";
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

    private static void Delete() {
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
            String[] parts = line.split(",", Integer.MAX_VALUE);

            try {
                ResultSet rs = stmt.executeQuery("select import_id from sustc_test.public.item where item_name = '" + parts[0] + "'");
                if(rs.next())
                {int ans = rs.getInt("import_id");
                stmt.execute("delete from sustc_test.public.item  where item_name = '" + parts[0] + "'");
                stmt.execute("delete from sustc_test.public.item where import_id = '" + ans + "'");
                stmt.execute("delete from sustc_test.public.item where export_id = '" + ans + "'");
                stmt.execute("delete from sustc_test.public.item where retri_id = '" + ans + "'");
                stmt.execute("delete from sustc_test.public.item where deli_id = '" + ans + "'");}
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("Time = " + (System.currentTimeMillis() - start));
    }

    private static void Replace() {
        try {

            stmt.execute("delete from sustc_test.public.item");

            stmt.execute("delete from sustc_test.public.import_list");
            stmt.execute("alter sequence sustc_test.public.import_list_import_id_seq restart with 1");
            stmt.execute("delete from sustc_test.public.export_list");
            stmt.execute("alter sequence sustc_test.public.export_list_export_id_seq restart with 1");

            stmt.execute("delete from sustc_test.public.workpalce");

            stmt.execute("delete from sustc_test.public.retrieval");
            stmt.execute("alter sequence sustc_test.public.retrieval_retri_id_seq restart with 1");

            stmt.execute("delete from sustc_test.public.delivery");
            stmt.execute("alter sequence sustc_test.public.delivery_deli_id_seq restart with 1");

            stmt.execute("delete from sustc_test.public.ship");
            stmt.execute("alter sequence sustc_test.public.ship_ship_id_seq restart with 1");

            stmt.execute("delete from sustc_test.public.courier");
            stmt.execute("alter sequence sustc_test.public.courier_courier_id_seq restart with 1");

            stmt.execute("delete from sustc_test.public.company");
            stmt.execute("alter sequence sustc_test.public.company_company_id_seq restart with 1");

            stmt.execute("delete from sustc_test.public.inland_city");
            stmt.execute("delete from sustc_test.public.port_city");
            stmt.execute("alter sequence  sustc_test.public.inland_city_inland_city_id_seq restart with 1");
            stmt.execute("alter sequence  sustc_test.public.port_city_port_city_id_seq restart with 1");
            stmt.execute("delete from sustc_test.public.container");
            System.out.println("Replace complete");
        } catch (SQLException e) {
            System.out.println("Replace break.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) throws SQLException {
        Create_Conn();
        //Replace();
        Delete();
    }
}
