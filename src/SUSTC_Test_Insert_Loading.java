import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class SUSTC_Test_Insert_Loading {
    static Random r = new Random();
    private static Connection conn = null;
    private static Statement stmt = null;

    private static BufferedReader file;

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

    static String[] test_cases = new String[10005];
    static int cnt_test = 0;
    private static void Replace() {
        try {

            stmt.execute("delete from sustc_test" +
                    ".public.item");

            stmt.execute("delete from sustc_test" +
                    ".public.import_list");
            stmt.execute("alter sequence sustc_test" +
                    ".public.import_list_import_id_seq restart with 1");
            stmt.execute("delete from sustc_test" +
                    ".public.export_list");
            stmt.execute("alter sequence sustc_test" +
                    ".public.export_list_export_id_seq restart with 1");

            stmt.execute("delete from sustc_test" +
                    ".public.workpalce");

            stmt.execute("delete from sustc_test" +
                    ".public.retrieval");
            stmt.execute("alter sequence sustc_test" +
                    ".public.retrieval_retri_id_seq restart with 1");

            stmt.execute("delete from sustc_test" +
                    ".public.delivery");
            stmt.execute("alter sequence sustc_test" +
                    ".public.delivery_deli_id_seq restart with 1");

            stmt.execute("delete from sustc_test" +
                    ".public.ship");
            stmt.execute("alter sequence sustc_test" +
                    ".public.ship_ship_id_seq restart with 1");

            stmt.execute("delete from sustc_test" +
                    ".public.courier");
            stmt.execute("alter sequence sustc_test" +
                    ".public.courier_courier_id_seq restart with 1");

            stmt.execute("delete from sustc_test" +
                    ".public.company");
            stmt.execute("alter sequence sustc_test" +
                    ".public.company_company_id_seq restart with 1");

            stmt.execute("delete from sustc_test" +
                    ".public.inland_city");
            stmt.execute("delete from sustc_test" +
                    ".public.port_city");
            stmt.execute("alter sequence  sustc_test" +
                    ".public.inland_city_inland_city_id_seq restart with 1");
            stmt.execute("alter sequence  sustc_test" +
                    ".public.port_city_port_city_id_seq restart with 1");
            stmt.execute("delete from sustc_test" +
                    ".public.container");
            System.out.println("Replace complete");
        } catch (SQLException e) {
            System.out.println("Replace break.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void Add() throws SQLException {
        int cnt = 0;
        long start, end;
        int cnt_company = 0;
        int cnt_courier = 0;
        int cnt_inland = 0;
        int cnt_port = 0;
        int cnt_ship = 0;
        PreparedStatement Add_company;
        PreparedStatement Add_inlandCity;
        PreparedStatement Add_portCity;
        PreparedStatement Add_container;
        PreparedStatement Add_ship;
        PreparedStatement Add_export;
        PreparedStatement Add_import;
        PreparedStatement Add_courier;
        PreparedStatement Add_delivery;
        PreparedStatement Add_retrieval;
        PreparedStatement Add_item;
        PreparedStatement Add_workplace;
        try {
            Add_company = conn.prepareStatement("insert into sustc_test" +
                    ".public.company(company_name) values(?)");
            Add_inlandCity = conn.prepareStatement("insert into sustc_test" +
                    ".public.inland_city(inland_city_name) values (?)");
            Add_portCity = conn.prepareStatement("insert into sustc_test" +
                    ".public.port_city(port_city_name) values(?)");
            Add_container = conn.prepareStatement("insert into sustc_test" +
                    ".public.container(con_code, con_type) values (?, ?)");
            Add_ship = conn.prepareStatement("insert into sustc_test" +
                    ".public.ship(ship_name, company_id) values(?, ?)");
            Add_export = conn.prepareStatement("insert into sustc_test" +
                    ".public.export_list(export_time, export_tax, export_city_id, ship_id, used_con_code) values(?, ?, ?, ?, ?)");
            Add_import = conn.prepareStatement("insert into sustc_test" +
                    ".public.import_list(import_time, import_tax, import_city_id) values (?, ?, ?)");
            Add_courier = conn.prepareStatement("insert into sustc_test" +
                    ".public.courier(courier_name, courier_gender, courier_ph_num, courier_birthyear, company_id) values (?, ?, ?, ?, ?)");
            Add_delivery = conn.prepareStatement("insert into sustc_test" +
                    ".public.delivery(deli_time, courier_id, deli_city_id) values (?, ?, ?)");
            Add_retrieval = conn.prepareStatement("insert into sustc_test" +
                    ".public.retrieval(retri_time, courier_id, retri_city_id) values (?, ?, ?)");
            Add_workplace = conn.prepareStatement("insert into sustc_test" +
                    ".public.workpalce(courier_id, workcity_id) values (?,?)");
            Add_item = conn.prepareStatement("insert into sustc_test" +
                    ".public.item(item_name, item_type, item_price, retri_id, deli_id, export_id, import_id, log_time) values (?, ?, ?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            file = new BufferedReader(new FileReader("source/data.csv"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String line = null;
        try {
            line = file.readLine();
            System.out.println("File is read successfully.");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        start = System.currentTimeMillis();
        final int batch_size = 5000;
        int company_cnt = 0;
        int inland_cnt = 0;
        int port_cnt = 0;
        int contain_cnt = 0;
        int ship_cnt = 0;
        int export_cnt = 0;
        int import_cnt = 0;
        int courier_cnt = 0;
        int deli_cnt = 0;
        int retri_cnt = 0;
        int item_cnt = 0;
        int work_cnt = 0;
        while (true) {
            try {
                if ((line = file.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            if(cnt_test < 10000 && r.nextInt(2) == 1)
            {
                test_cases[cnt_test++] = line;
                if(cnt_test == 10000)
                {
                    BufferedWriter writer = null;
                    try {
                        writer = new BufferedWriter(new FileWriter("source/test_1w.csv"));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    for(int i = 0; i < 10000; ++i)
                    {
                        try {
                            writer.write(test_cases[i]);
                            if(i!=9999) writer.newLine();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                    }
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                    continue;
            }
            ++cnt;
            String[] parts = line.split(",", Integer.MAX_VALUE);
            /*String Item_Name = parts[0];
            String Item_Type = parts[1];
            String Item_Price = parts[2];
            String Retrieval_City = parts[3];
            String Retrieval_StartTime = parts[4];
            String Retrieval_Courier = parts[5];
            String Retrieval_Courier_Gender = parts[6];
            String Retrieval_Courier_PhoneNumber = parts[7];
            String Retrieval_Courier_Age = parts[8];
            String Delivery_FinishedTime = parts[9];
            String Delivery_City = parts[10];
            String Delivery_Courier = parts[11];
            String Delivery_Courier_Gender = parts[12];
            String Delivery_Courier_PhoneNumber = parts[13];
            String Delivery_Courier_Age = parts[14];
            String Item_Export_City = parts[15];
            String Item_Export_Tax = parts[16];
            String Item_Export_Time = parts[17];
            String Item_Import_City = parts[18];
            String Item_Import_Tax = parts[19];
            String Item_Import_Time = parts[20];
            String Container_Code = parts[21];
            String Container_Type = parts[22];
            String Ship_Name = parts[23];
            String Company_Name = parts[24];
            String Log_Time = parts[25];*/
            if (!company.containsKey(parts[24])) {
                ++cnt_company;
                Add_company.setString(1, parts[24]);
                Add_company.addBatch();
                ++company_cnt;
                if (company_cnt % batch_size == 0) Add_company.executeBatch();
                company.put(parts[24], cnt_company);
            }
            if (!inland.containsKey(parts[3])) {
                ++cnt_inland;
                Add_inlandCity.setString(1, parts[3]);
                Add_inlandCity.addBatch();
                ++inland_cnt;
                if (inland_cnt % batch_size == 0) Add_inlandCity.executeBatch();
                inland.put(parts[3], cnt_inland);
            }
            if (!inland.containsKey(parts[10])) {
                ++cnt_inland;
                Add_inlandCity.setString(1, parts[10]);
                Add_inlandCity.addBatch();
                ++inland_cnt;
                if (inland_cnt % batch_size == 0) Add_inlandCity.executeBatch();
                inland.put(parts[10], cnt_inland);
            }
            if (!port.containsKey(parts[15])) {
                ++cnt_port;
                Add_portCity.setString(1, parts[15]);
                Add_portCity.addBatch();
                ++port_cnt;
                if (port_cnt % batch_size == 0) Add_portCity.executeBatch();
                port.put(parts[15], cnt_port);
            }
            if (!port.containsKey(parts[18])) {
                ++cnt_port;
                Add_portCity.setString(1, parts[18]);
                Add_portCity.addBatch();
                ++port_cnt;
                if (port_cnt % batch_size == 0) Add_portCity.executeBatch();
                port.put(parts[18], cnt_port);
            }
            if (!parts[21].matches("")) {
                if (!container.contains(parts[21])) {
                    Add_container.setString(1, parts[21]);
                    Add_container.setString(2, parts[22]);
                    Add_container.addBatch();
                    ++contain_cnt;
                    if (contain_cnt % batch_size == 0) Add_container.executeBatch();
                    container.add(parts[21]);
                }
            }

            if (!parts[4].matches("")) {
                int birth = Integer.parseInt(parts[4].substring(0, 4)) - Integer.parseInt(parts[8]);
                if (!courier.containsKey(parts[7].substring(5))) {
                    ++cnt_courier;
                    Add_courier.setString(1, parts[5]);
                    Add_courier.setString(2, parts[6]);
                    Add_courier.setString(3, parts[7].substring(5));
                    Add_courier.setBigDecimal(4, BigDecimal.valueOf(birth));
                    Add_courier.setInt(5, company.get(parts[24]));
                    Add_courier.addBatch();
                    ++courier_cnt;
                    if (courier_cnt % batch_size == 0) Add_courier.executeBatch();
                    courier.put(parts[7].substring(5), cnt_courier);
                    {
                        Add_workplace.setInt(1, courier.get(parts[7].substring(5)));
                        Add_workplace.setInt(2, inland.get(parts[3]));
                        Add_workplace.addBatch();
                        ++work_cnt;
                        if (work_cnt % batch_size == 0) Add_workplace.executeBatch();
                    }
                }
            }
            if (!parts[9].matches("")) {
                int birth = Integer.parseInt(parts[9].substring(0, 4)) - Integer.parseInt(parts[14].substring(0, 2));
                if (!courier.containsKey(parts[13].substring(5))) {
                    ++cnt_courier;
                    Add_courier.setString(1, parts[11]);
                    Add_courier.setString(2, parts[12]);
                    Add_courier.setString(3, parts[13].substring(5));
                    Add_courier.setBigDecimal(4, BigDecimal.valueOf(birth));
                    Add_courier.setInt(5, company.get(parts[24]));
                    Add_courier.addBatch();
                    ++courier_cnt;
                    if (courier_cnt % batch_size == 0) Add_courier.executeBatch();
                    courier.put(parts[13].substring(5), cnt_courier);
                    {
                        Add_workplace.setInt(1, courier.get(parts[13].substring(5)));
                        Add_workplace.setInt(2, inland.get(parts[10]));
                        Add_workplace.addBatch();
                        ++work_cnt;
                        if (work_cnt % batch_size == 0) Add_workplace.executeBatch();
                    }
                }
            }
            if (!parts[23].equals("")) {
                if (!ship.containsKey(parts[23])) {
                    ++cnt_ship;
                    Add_ship.setString(1, parts[23]);
                    Add_ship.setInt(2, company.get(parts[24]));
                    Add_ship.addBatch();
                    ++ship_cnt;
                    if (ship_cnt % batch_size == 0) Add_ship.executeBatch();
                    ship.put(parts[23], cnt_ship);
                }
            }
            if (!parts[17].equals("")) {
                Add_export.setDate(1, Date.valueOf(parts[17]));
                Add_export.setBigDecimal(2, new BigDecimal(parts[16]));
                Add_export.setInt(3, port.get(parts[15]));
                Add_export.setInt(4, ship.get(parts[23]));
                Add_export.setString(5, parts[21]);
                Add_export.addBatch();
                ++export_cnt;
                if (export_cnt % batch_size == 0) Add_export.executeBatch();
            } else {
                Add_export.setDate(1, null);
                Add_export.setBigDecimal(2, new BigDecimal(parts[16]));
                Add_export.setInt(3, port.get(parts[15]));
                Add_export.setNull(4, Types.INTEGER);
                Add_export.setNull(5, Types.VARCHAR);
                Add_export.addBatch();
                ++export_cnt;
                if (export_cnt % batch_size == 0) Add_export.executeBatch();
            }
            if (!parts[20].equals("")) {
                Add_import.setDate(1, Date.valueOf(parts[20]));
                Add_import.setBigDecimal(2, new BigDecimal(parts[19]));
                Add_import.setInt(3, port.get(parts[18]));
                Add_import.addBatch();
                ++import_cnt;
                if (import_cnt % batch_size == 0) Add_import.executeBatch();
            } else {
                Add_import.setNull(1, Types.DATE);
                Add_import.setBigDecimal(2, new BigDecimal(parts[19]));
                Add_import.setInt(3, port.get(parts[18]));
                Add_import.addBatch();
                ++import_cnt;
                if (import_cnt % batch_size == 0) Add_import.executeBatch();
            }
            if (!parts[9].equals("")) {
                Add_delivery.setInt(3, inland.get(parts[10]));
                Add_delivery.setInt(2, courier.get(parts[13].substring(5)));
                Add_delivery.setDate(1, Date.valueOf(parts[9]));
                Add_delivery.addBatch();
                ++deli_cnt;
                if (deli_cnt % batch_size == 0) Add_delivery.executeBatch();
            } else {
                Add_delivery.setInt(3, inland.get(parts[10]));
                Add_delivery.setNull(2, Types.INTEGER);
                Add_delivery.setNull(1, Types.DATE);
                Add_delivery.addBatch();
                ++deli_cnt;
                if (deli_cnt % batch_size == 0) Add_delivery.executeBatch();
            }
            {
                Add_retrieval.setInt(3, inland.get(parts[3]));
                Add_retrieval.setInt(2, courier.get(parts[7].substring(5)));
                Add_retrieval.setDate(1, Date.valueOf(parts[4]));
                Add_retrieval.addBatch();
                ++retri_cnt;
                if (retri_cnt % batch_size == 0) Add_retrieval.executeBatch();
            }
            {
                Add_item.setString(1, parts[0]);
                Add_item.setString(2, parts[1]);
                Add_item.setBigDecimal(3, new BigDecimal(parts[2]));
                Add_item.setInt(4, cnt);
                Add_item.setInt(5, cnt);
                Add_item.setInt(6, cnt);
                Add_item.setInt(7, cnt);
                Add_item.setTimestamp(8, Timestamp.valueOf(parts[25]));
                Add_item.addBatch();
                ++item_cnt;
                if (item_cnt % batch_size == 0) Add_item.executeBatch();
            }
        }
        Add_inlandCity.executeBatch();
        Add_portCity.executeBatch();
        Add_container.executeBatch();
        Add_company.executeBatch();
        Add_courier.executeBatch();
        Add_delivery.executeBatch();
        Add_export.executeBatch();
        Add_item.executeBatch();
        Add_retrieval.executeBatch();
        Add_workplace.executeBatch();
        Add_ship.executeBatch();
        Add_import.executeBatch();
        end = System.currentTimeMillis();
        System.out.println("All tables are created completely.");
        System.out.println("Loading speed " + (cnt * 1000L) / (end - start) + " records/s");
    }

    public static void main(String[] args) throws SQLException {
        Create_Conn();
        //Replace();
        Add();
    }
}
