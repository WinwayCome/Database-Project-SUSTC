import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SUSTC_Create_v2_0 {
    private static Connection conn = null;
    private static Statement stmt = null;

    static BufferedReader file;

    private static void Create_Conn() {
        String url = "jdbc:postgresql://localhost:5432/sustc_2";
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

    private static void Add_Company(String company_name) {
        try {
            stmt.execute("insert into sustc_2.public.company(company_name) values ('" + company_name + "')");
        } catch (SQLException e) {
            try {
                System.out.println("Add_Company break.");
                e.printStackTrace();
                conn.rollback();
                System.exit(1);
            } catch (SQLException ex) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void Add_Courier(String courier_name, String courier_gender, String courier_call, String courier_birth_year, String company_id) {
        try {
            stmt.execute("insert into sustc_2.public.courier(courier_name, courier_gender, courier_ph_num, courier_birthyear, company_id) values ('"
                    + courier_name + "','" + courier_gender + "','" + courier_call + "','" + courier_birth_year + "','" + company_id + "')");
        } catch (SQLException e) {
            try {
                System.out.println("Add_Courier break.");
                e.printStackTrace();
                conn.rollback();
                System.exit(1);
            } catch (SQLException ex) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void Add_InlandCity(String city) {
        try {
            stmt.execute("insert into sustc_2.public.inland_city(inland_city_name) values ('" + city + "')");
        } catch (SQLException e) {
            try {
                System.out.println("Add_InlandCity break.");
                e.printStackTrace();
                conn.rollback();
                System.exit(1);
            } catch (SQLException ex) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void Add_PortCity(String city) {
        try {
            stmt.execute("insert into sustc_2.public.port_city(port_city_name) values ('" + city + "')");
        } catch (SQLException e) {
            try {
                System.out.println("Add_PortCity break.");
                e.printStackTrace();
                conn.rollback();
                System.exit(1);
            } catch (SQLException ex) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void Add_Container(String con_code, String con_type) {
        try {
            stmt.execute("insert into sustc_2.public.container(con_code, con_type) values ('" + con_code + "','" + con_type + "')");
        } catch (SQLException e) {
            try {
                System.out.println("Add_Container break.");
                e.printStackTrace();
                conn.rollback();
                System.exit(1);
            } catch (SQLException ex) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private static void Add_Ship(String ship_name, String company_id) {
        try {
            stmt.execute("insert into sustc_2.public.ship(ship_name, company_id) values ('" + ship_name + "','" + company_id + "')");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Export(String export_time, String export_tax, String export_city_id, String ship_id, String used_con_code) {
        try {
            stmt.execute("insert into sustc_2.public.export_list(export_time, export_tax, export_city_id, ship_id, used_con_code) VALUES ("
                    + export_time + "," + export_tax + "," + export_city_id + "," + ship_id + "," + export_city_id + ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Import(String import_time, String import_tax, String import_city_id) {
        try {
            stmt.execute("insert into sustc_2.public.import_list(import_time, import_tax, import_city_id) VALUES ("
                    + import_time + "," + import_tax + "," + import_city_id + ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Delivery(String deli_city_id, String courier_id, String deli_time) {
        try {
            stmt.execute("insert into sustc_2.public.delivery(deli_time, courier_id, deli_city_id) VALUES ("
                    + deli_time + "," + courier_id + "," + deli_city_id + ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Retrieval(String retri_city_id, String courier_id, String retri_time) {
        try {
            stmt.execute("insert into sustc_2.public.retrieval(retri_time, courier_id, retri_city_id) VALUES ("
                    + retri_time + "," + courier_id + "," + retri_city_id + ")");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Item(String item_name, String item_type, String item_price, String cnt, String log_time) {
        try {
            stmt.execute("insert into sustc_2.public.item(item_name, item_type, item_price, retri_id, deli_id, export_id, import_id, log_time) " +
                    "values('" + item_name + "','" + item_type + "','" + item_price + "','" + cnt + "','" + cnt + "','" + cnt + "','" + cnt + "','" + log_time + "')");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Add_WorkPlace(String courier_id, String city_id)
    {
        try {
            stmt.execute("insert into sustc_2.public.workpalce(courier_id, workcity_id) VALUES ('" + courier_id + "','" + city_id + "')");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Replace() {
        try {

            stmt.execute("delete from sustc_2.public.item");

            stmt.execute("delete from sustc_2.public.import_list");
            stmt.execute("alter sequence sustc_2.public.import_list_import_id_seq restart with 1");
            stmt.execute("delete from sustc_2.public.export_list");
            stmt.execute("alter sequence sustc_2.public.export_list_export_id_seq restart with 1");

            stmt.execute("delete from sustc_2.public.workpalce");

            stmt.execute("delete from sustc_2.public.retrieval");
            stmt.execute("alter sequence sustc_2.public.retrieval_retri_id_seq restart with 1");

            stmt.execute("delete from sustc_2.public.delivery");
            stmt.execute("alter sequence sustc_2.public.delivery_deli_id_seq restart with 1");

            stmt.execute("delete from sustc_2.public.ship");
            stmt.execute("alter sequence sustc_2.public.ship_ship_id_seq restart with 1");

            stmt.execute("delete from sustc_2.public.courier");
            stmt.execute("alter sequence sustc_2.public.courier_courier_id_seq restart with 1");

            stmt.execute("delete from sustc_2.public.company");
            stmt.execute("alter sequence sustc_2.public.company_company_id_seq restart with 1");

            stmt.execute("delete from sustc_2.public.inland_city");
            stmt.execute("delete from sustc_2.public.port_city");
            stmt.execute("alter sequence  sustc_2.public.inland_city_inland_city_id_seq restart with 1");
            stmt.execute("alter sequence  sustc_2.public.port_city_port_city_id_seq restart with 1");
            stmt.execute("delete from sustc_2.public.container");
            System.out.println("Replace complete");
        } catch (SQLException e) {
            System.out.println("Replace break.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void Add() {
        int cnt = 0;
        long start, end;
        int cnt_company = 0;
        int cnt_courier = 0;
        int cnt_inland = 0;
        int cnt_port = 0;
        int cnt_ship = 0;
        Map<String, Integer> company = new HashMap<>();
        Map<String, Integer> courier = new HashMap<>();
        Map<String, Integer> inland = new HashMap<>();
        Map<String, Integer> port = new HashMap<>();
        Map<String, Integer> ship = new HashMap<>();
        Set<String> container = new HashSet<>();
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
        while (true) {
            try {
                if ((line = file.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
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
                Add_Company(parts[24]);
                company.put(parts[24], cnt_company);
            }
            if (!inland.containsKey(parts[3])) {
                ++cnt_inland;
                Add_InlandCity(parts[3]);
                inland.put(parts[3], cnt_inland);
            }
            if (!inland.containsKey(parts[10])) {
                ++cnt_inland;
                Add_InlandCity(parts[10]);
                inland.put(parts[10], cnt_inland);
            }
            if (!port.containsKey(parts[15])) {
                ++cnt_port;
                Add_PortCity(parts[15]);
                port.put(parts[15], cnt_port);
            }
            if (!port.containsKey(parts[18])) {
                ++cnt_port;
                Add_PortCity(parts[18]);
                port.put(parts[18], cnt_port);
            }
            if (!parts[21].matches("")) {
                if (!container.contains(parts[21])) {
                    Add_Container(parts[21], parts[22]);
                    container.add(parts[21]);
                }
            }

            if (!parts[4].matches("")) {
                int birth = Integer.parseInt(parts[4].substring(0, 4)) - Integer.parseInt(parts[8]);
                String courier_birth = String.valueOf(birth);
                if (!courier.containsKey(parts[7].substring(5))) {
                    ++cnt_courier;
                    Add_Courier(parts[5], parts[6], parts[7].substring(5), courier_birth, company.get(parts[24]).toString());
                    courier.put(parts[7].substring(5), cnt_courier);
                    Add_WorkPlace(courier.get(parts[7].substring(5)).toString(), inland.get(parts[3]).toString());
                }
            }
            if (!parts[9].matches("")) {
                int birth = Integer.parseInt(parts[9].substring(0, 4)) - Integer.parseInt(parts[14].substring(0, 2));
                String courier_birth = String.valueOf(birth);
                if (!courier.containsKey(parts[13].substring(5))) {
                    ++cnt_courier;
                    Add_Courier(parts[11], parts[12], parts[13].substring(5), courier_birth, company.get(parts[24]).toString());
                    courier.put(parts[13].substring(5), cnt_courier);
                    Add_WorkPlace(courier.get(parts[13].substring(5)).toString(), inland.get(parts[10]).toString());
                }
            }
            if (!parts[23].equals("")) {
                if (!ship.containsKey(parts[23])) {
                    ++cnt_ship;
                    Add_Ship(parts[23], company.get(parts[24]).toString());
                    ship.put(parts[23], cnt_ship);
                }
            }
            if (!parts[17].equals(""))
                Add_Export("'" + parts[17] + "'", "'" + parts[16] + "'", "'" + port.get(parts[15]).toString() + "'", "'" + ship.get(parts[23]).toString() + "'", "'" + parts[21] + "'");
            else
                Add_Export("null", "'" + parts[16] + "'", "'" + port.get(parts[15]).toString() + "'", "null", "null");
            if (!parts[20].equals(""))
                Add_Import("'" + parts[20] + "'", "'" + parts[19] + "'", "'" + port.get(parts[18]).toString() + "'");
            else
                Add_Import("null", "'" + parts[19] + "'", "'" + port.get(parts[18]).toString() + "'");
            if (!parts[9].equals("")) {
                Add_Delivery("'" + inland.get(parts[10]).toString() + "'", "'" + courier.get(parts[13].substring(5)).toString() + "'", "'" + parts[9] + "'");
            } else
                Add_Delivery("'" + inland.get(parts[10]).toString() + "'", "null", "null");
            Add_Retrieval("'" + inland.get(parts[3]).toString() + "'", "'" + courier.get(parts[7].substring(5)).toString() + "'", "'" + parts[4] + "'");
            Add_Item(parts[0], parts[1], parts[2], Integer.toString(cnt), parts[25]);
        }
        end = System.currentTimeMillis();
        System.out.println("All tables are created completely.");
        System.out.println("Loading speed " + (cnt * 1000L) / (end - start) + " records/s");
    }

    public static void main(String[] args) {
        Create_Conn();
        Replace();
        Add();
        //Create_Index();
    }
}
