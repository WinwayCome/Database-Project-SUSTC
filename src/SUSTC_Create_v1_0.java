import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;

public class SUSTC_Create_v1_0 {
    private static Connection conn = null;
    private static Statement stmt = null;

    static BufferedReader file;

    private static void Create_Conn() {
        String url = "jdbc:postgresql://localhost:5432/sustc";
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
            stmt.execute("insert into sustc.public.company(company_name) values ('" + company_name + "') on conflict(company_name) do nothing");
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

    private static void Add_Courier(String courier_name, String courier_gender, String courier_call, String courier_birth_year, String company_name) {
        String company_id = null;
        ResultSet rs;
        try {
            rs = stmt.executeQuery("select company_id from sustc.public.company where company_name = '" + company_name + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            if (rs.next()) company_id = rs.getString("company_id");
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.courier(company_id, courier_name, courier_gender, courier_ph_num, courier_birthyear) values ('"
                    + company_id + "','" + courier_name + "','" + courier_gender + "','" + courier_call + "','" + courier_birth_year + "')on conflict(courier_ph_num) do nothing ");
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
            stmt.execute("insert into sustc.public.inland_city(inland_city_name) values ('" + city + "') on conflict(inland_city_name) do nothing");
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
            stmt.execute("insert into sustc.public.port_city(port_city_name) values ('" + city + "') on conflict(port_city_name) do nothing");
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
            stmt.execute("insert into sustc.public.container(con_code, con_type) values ('" + con_code + "','" + con_type + "')on conflict (con_code) do nothing ");
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

    private static void Add_Ship(String ship_name, String company_name) {
        String company_id = null;
        ResultSet rs;
        try {
            rs = stmt.executeQuery("select company_id from sustc.public.company where company_name = '" + company_name + "'");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            if (rs.next()) company_id = rs.getString("company_id");
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.ship(ship_name, company_id) values ('" + ship_name + "','" + company_id + "') on conflict(ship_name) do nothing");
        } catch (SQLException e) {
            System.out.println("Add_Ship break.");
        }
    }

    private static void Add_Delivery_N(String city_name) {
        String city_id = "null";
        ResultSet rs2;
        try {
            rs2 = stmt.executeQuery("select inland_city_id from sustc.public.inland_city where inland_city_name = '" + city_name + "'");
            if (rs2.next()) city_id = rs2.getString("inland_city_id");
            rs2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.delivery(deli_time, courier_id, deli_city_id) values (" + "null" + "," + "null" + ",'" + city_id + "') ");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Delivery_Y(String deli_time, String courier_phone, String city_name) {
        String courier_id = "null", city_id = "null";
        ResultSet rs1, rs2;
        try {
            rs1 = stmt.executeQuery("select courier_id from sustc.public.courier where courier_ph_num = '" + courier_phone + "'");
            if (rs1.next()) courier_id = rs1.getString("courier_id");
            rs1.close();
            rs2 = stmt.executeQuery("select inland_city_id from sustc.public.inland_city where inland_city_name = '" + city_name + "'");
            if (rs2.next()) city_id = rs2.getString("inland_city_id");
            rs2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.delivery(deli_time, courier_id, deli_city_id) values (" + "'" + deli_time + "'" + "," + "'" + courier_id + "'" + ",'" + city_id + "') ");
            stmt.execute("insert into sustc.public.workpalce(courier_id, workcity_id) values ('" +courier_id + "','"+ city_id + "')on conflict (courier_id, workcity_id)do nothing ");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Add_Retrieval(String retri_time, String courier_phone, String city_name)
    {
        String courier_id = "null", city_id = "null";
        ResultSet rs1, rs2;
        try {
            rs1 = stmt.executeQuery("select courier_id from sustc.public.courier where courier_ph_num = '" + courier_phone + "'");
            if (rs1.next()) courier_id = rs1.getString("courier_id");
            rs1.close();
            rs2 = stmt.executeQuery("select inland_city_id from sustc.public.inland_city where inland_city_name = '" + city_name + "'");
            if (rs2.next()) city_id = rs2.getString("inland_city_id");
            rs2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.retrieval(retri_time, courier_id, retri_city_id) values (" + "'" + retri_time + "'" + "," + "'" + courier_id + "'" + ",'" + city_id + "') ");
            stmt.execute("insert into sustc.public.workpalce(courier_id, workcity_id) values ('" +courier_id + "','"+ city_id + "')on conflict (courier_id, workcity_id)do nothing ");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Add_Import(String import_time, String import_tax, String city_name)
    {
        String city_id = "null";
        ResultSet rs2;
        try {
            rs2 = stmt.executeQuery("select port_city_id from sustc.public.port_city where port_city_name = '" + city_name + "'");
            if (rs2.next()) city_id = rs2.getString("port_city_id");
            rs2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.import_list(import_time, import_tax, import_city_id) values ("
            + (import_time.equals("")?"null":("'" + import_time + "'")) + ",'" + import_tax + "','" + city_id + "')");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Add_Export_Y(String export_time, String export_tax, String city_name, String con_code, String ship_name)
    {
        String city_id = "null";
        String ship_id = "null";
        ResultSet rs1, rs2;
        try {
            rs1 = stmt.executeQuery("select ship_id from sustc.public.ship where ship_name = '" + ship_name + "'");
            if (rs1.next()) ship_id = rs1.getString("ship_id");
            rs1.close();
            rs2 = stmt.executeQuery("select port_city_id from sustc.public.port_city where port_city_name = '" + city_name + "'");
            if (rs2.next()) city_id = rs2.getString("port_city_id");
            rs2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.export_list(export_time, export_tax, export_city_id, ship_id, used_con_code) values ("
                    + "'" + export_time + "'" + ",'" + export_tax + "','" + city_id + "','" + ship_id + "','" + con_code + "')");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Add_Export_N(String city_name, String export_tax)
    {
        String city_id = "null";
        ResultSet rs2;
        try {
            rs2 = stmt.executeQuery("select port_city_id from sustc.public.port_city where port_city_name = '" + city_name + "'");
            if (rs2.next()) city_id = rs2.getString("port_city_id");
            rs2.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        try {
            stmt.execute("insert into sustc.public.export_list(export_time, export_tax, export_city_id, ship_id, used_con_code) values (null,'" + export_tax + "','" + city_id + "',"+"null, null)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Add_Item(String item_name, String item_type,String item_price, String cnt, String log_time)
    {
        try {
            stmt.execute("insert into sustc.public.item(item_name, item_type, item_price, retri_id, deli_id, export_id, import_id, log_time) " +
                    "values('" + item_name + "','"+ item_type + "','" + item_price+ "','" +cnt+ "','" +cnt+ "','" +cnt+ "','" +cnt+ "','"+ log_time +"')");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private static void Replace() {
        try {
            stmt.execute("alter table sustc.public.courier disable trigger all");
            stmt.execute("alter table sustc.public.ship disable trigger all");
            stmt.execute("alter table sustc.public.delivery disable trigger all");
            stmt.execute("alter table sustc.public.retrieval disable trigger all");
            stmt.execute("alter table sustc.public.workpalce disable trigger all");
            stmt.execute("alter table sustc.public.import_list disable trigger all");
            stmt.execute("alter table sustc.public.export_list disable trigger all");
            stmt.execute("alter table sustc.public.item disable trigger all");

            stmt.execute("delete from sustc.public.item");

            stmt.execute("delete from sustc.public.import_list");
            stmt.execute("alter sequence sustc.public.import_list_import_id_seq restart with 1");
            stmt.execute("delete from sustc.public.export_list");
            stmt.execute("alter sequence sustc.public.export_list_export_id_seq restart with 1");

            stmt.execute("delete from sustc.public.workpalce");

            stmt.execute("delete from sustc.public.retrieval");
            stmt.execute("alter sequence sustc.public.retrieval_retri_id_seq restart with 1");

            stmt.execute("delete from sustc.public.delivery");
            stmt.execute("alter sequence sustc.public.delivery_deli_id_seq restart with 1");

            stmt.execute("delete from sustc.public.ship");
            stmt.execute("alter sequence sustc.public.ship_ship_id_seq restart with 1");

            stmt.execute("delete from sustc.public.courier");
            stmt.execute("alter sequence sustc.public.courier_courier_id_seq restart with 1");

            stmt.execute("delete from sustc.public.company");
            stmt.execute("alter sequence sustc.public.company_company_id_seq restart with 1");

            stmt.execute("delete from sustc.public.inland_city");
            stmt.execute("delete from sustc.public.port_city");
            stmt.execute("alter sequence  sustc.public.inland_city_inland_city_id_seq restart with 1");
            stmt.execute("alter sequence  sustc.public.port_city_port_city_id_seq restart with 1");
            stmt.execute("delete from sustc.public.container");
            System.out.println("Replace complete");
        } catch (SQLException e) {
            System.out.println("Replace break.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void Add_Static() {
        int cnt = 0;
        long start, end;
        try {
            file = new BufferedReader(new FileReader("source/data.csv"));
            file.mark(227000000);
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
            Add_Company(parts[24]);
            Add_InlandCity(parts[3]);
            Add_InlandCity(parts[10]);
            Add_PortCity(parts[15]);
            Add_PortCity(parts[18]);
            if (!parts[21].matches(""))
                Add_Container(parts[21], parts[22]);
            if (!parts[4].matches("")) {
                int birth = Integer.parseInt(parts[4].substring(0, 4)) - Integer.parseInt(parts[8]);
                String courier_birth = String.valueOf(birth);
                Add_Courier(parts[5], parts[6], parts[7].substring(5), courier_birth, parts[24]);
            }
            if (!parts[9].matches("")) {
                int birth = Integer.parseInt(parts[9].substring(0, 4)) - Integer.parseInt(parts[14].substring(0, 2));
                String courier_birth = String.valueOf(birth);
                Add_Courier(parts[11], parts[12], parts[13].substring(5), courier_birth, parts[24]);
            }
            if (!parts[23].equals(""))
                Add_Ship(parts[23], parts[24]);
            if (!parts[9].equals(""))
                Add_Delivery_Y(parts[9], parts[13].substring(5), parts[10]);
            else Add_Delivery_N(parts[10]);
            Add_Retrieval(parts[4], parts[7].substring(5), parts[3]);
            Add_Import(parts[20], parts[19], parts[18]);
            if(!parts[17].equals(""))
                Add_Export_Y(parts[17], parts[16], parts[15], parts[21], parts[23]);
            else
                Add_Export_N(parts[15], parts[16]);
            Add_Item(parts[0],parts[1],parts[2],Integer.toString(cnt), parts[25]);
        }
        end = System.currentTimeMillis();
        System.out.println("Tables are created completely.");
        System.out.println("Loading speed " + (cnt * 1000L) / (end - start) + " records/s");
    }

    public static void main(String[] args) {
        Create_Conn();
        Replace();
        Add_Static();
        //Create_Index();
        //Add_Foreign();
        try {
            stmt.execute("alter table sustc.public.courier enable trigger all");
            stmt.execute("alter table sustc.public.ship enable trigger all");
            stmt.execute("alter table sustc.public.delivery enable trigger all");
            stmt.execute("alter table sustc.public.retrieval enable trigger all");
            stmt.execute("alter table sustc.public.workpalce enable trigger all");
            stmt.execute("alter table sustc.public.import_list enable trigger all");
            stmt.execute("alter table sustc.public.export_list enable trigger all");
            stmt.execute("alter table sustc.public.item enable trigger all");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
