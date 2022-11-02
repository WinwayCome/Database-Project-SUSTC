import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;

public class SUSTC_Test_Insert {
    private static Connection conn = null;
    private static Statement stmt = null;
    private static BufferedReader file;

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

    public static void Restart() {
        try {
            stmt.execute("alter table sustc_test.public.company add constraint uni_com_name unique(company_name);");
            stmt.execute("alter table sustc_test.public.ship add constraint uni_ship_name unique (ship_name);");
            stmt.execute("alter table sustc_test.public.ship add foreign key (company_id) references company(company_id) on update cascade on delete cascade;");
            stmt.execute("alter table sustc_test.public.courier add constraint uni_ph_num unique (courier_ph_num);");
            stmt.execute("alter table sustc_test.public.courier add foreign key (company_id) references company(company_id) on update cascade on delete cascade;");
            stmt.execute("alter table sustc_test.public.inland_city add constraint uni_in_city_name unique(inland_city_name);");
            stmt.execute("alter table sustc_test.public.port_city add constraint uni_po_city_name unique (port_city_name);");
            stmt.execute("alter table sustc_test.public.export_list add foreign key (used_con_code) references container(con_code);");
            stmt.execute("alter table sustc_test.public.export_list add foreign key (export_city_id) references port_city(port_city_id);");
            stmt.execute("alter table sustc_test.public.import_list add foreign key (import_city_id) references port_city(port_city_id);");
            stmt.execute("alter table sustc_test.public.retrieval add foreign key (courier_id) references courier(courier_id);");
            stmt.execute("alter table sustc_test.public.retrieval add foreign key (retri_city_id) references inland_city(inland_city_id);");
            stmt.execute("alter table sustc_test.public.delivery add foreign key (courier_id ) references  courier(courier_id);");
            stmt.execute("alter table sustc_test.public.delivery add foreign key (deli_city_id) references inland_city(inland_city_id);");
            stmt.execute("alter table sustc_test.public.workpalce add foreign key (courier_id) references courier(courier_id);");
            stmt.execute("alter table sustc_test.public.workpalce add foreign key (workcity_id) references inland_city(inland_city_id);");
            stmt.execute("alter table sustc_test.public.item add foreign key (retri_id) references retrieval(retri_id);");
            stmt.execute("alter table sustc_test.public.item add foreign key (deli_id) references delivery(deli_id);");
            stmt.execute("alter table sustc_test.public.item add foreign key (export_id) references export_list(export_id);");
            stmt.execute("alter table sustc_test.public.item add foreign key (import_id) references import_list(import_id);");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public static void main(String[] args) throws SQLException {
        Create_Conn();
        //Restart();
        try {
            file = new BufferedReader(new FileReader("source/test_1w.csv"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        String line = null;
        try {
            line = file.readLine();
            System.out.println("Test file is read successfully.");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
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
                    ".public.company(company_name) values(?) on conflict (company_name) do nothing ");
            Add_inlandCity = conn.prepareStatement("insert into sustc_test" +
                    ".public.inland_city(inland_city_name) values (?)on conflict (inland_city_name) do nothing");
            Add_portCity = conn.prepareStatement("insert into sustc_test" +
                    ".public.port_city(port_city_name) values(?)on conflict (port_city_name) do nothing");
            Add_container = conn.prepareStatement("insert into sustc_test" +
                    ".public.container(con_code, con_type) values (?, ?)on conflict (con_code) do nothing");
            Add_ship = conn.prepareStatement("insert into sustc_test" +
                    ".public.ship(ship_name, company_id) values(?, ?) on conflict (ship_name) do nothing ");
            Add_export = conn.prepareStatement("insert into sustc_test" +
                    ".public.export_list(export_time, export_tax, export_city_id, ship_id, used_con_code) values(?, ?, ?, ?, ?)");
            Add_import = conn.prepareStatement("insert into sustc_test" +
                    ".public.import_list(import_time, import_tax, import_city_id) values (?, ?, ?)");
            Add_courier = conn.prepareStatement("insert into sustc_test" +
                    ".public.courier(courier_name, courier_gender, courier_ph_num, courier_birthyear, company_id) values (?, ?, ?, ?, ?)on conflict (courier_ph_num)do nothing ");
            Add_delivery = conn.prepareStatement("insert into sustc_test" +
                    ".public.delivery(deli_time, courier_id, deli_city_id) values (?, ?, ?)");
            Add_retrieval = conn.prepareStatement("insert into sustc_test" +
                    ".public.retrieval(retri_time, courier_id, retri_city_id) values (?, ?, ?)");
            Add_workplace = conn.prepareStatement("insert into sustc_test" +
                    ".public.workpalce(courier_id, workcity_id) values (?,?) on conflict (courier_id, workcity_id) do nothing ");
            Add_item = conn.prepareStatement("insert into sustc_test" +
                    ".public.item(item_name, item_type, item_price, retri_id, deli_id, export_id, import_id, log_time) values (?, ?, ?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long start = System.currentTimeMillis();
        while (true) {
            try {
                if ((line = file.readLine()) == null) break;
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
            String[] parts = line.split(",", Integer.MAX_VALUE);
            Add_company.setString(1, parts[24]);
            Add_company.execute();
            Add_inlandCity.setString(1, parts[3]);
            Add_inlandCity.execute();
            Add_inlandCity.setString(1, parts[10]);
            Add_inlandCity.execute();
            Add_portCity.setString(1, parts[15]);
            Add_portCity.execute();
            Add_portCity.setString(1, parts[18]);
            Add_portCity.execute();
            if (!parts[21].matches("")) {
                Add_container.setString(1, parts[21]);
                Add_container.setString(2, parts[22]);
                Add_container.execute();
            }
            if (!parts[4].matches("")) {
                int birth = Integer.parseInt(parts[4].substring(0, 4)) - Integer.parseInt(parts[8]);
                Add_courier.setString(1, parts[5]);
                Add_courier.setString(2, parts[6]);
                Add_courier.setString(3, parts[7].substring(5));
                Add_courier.setBigDecimal(4, BigDecimal.valueOf(birth));
                ResultSet rs = stmt.executeQuery("select company_id from sustc_test.public.company where company_name = '" + parts[24] + "'");
                rs.next();
                Add_courier.setInt(5, rs.getInt("company_id"));
                Add_courier.execute();
                rs = stmt.executeQuery("select courier_id from sustc_test.public.courier where courier_ph_num = '" + parts[7].substring(5) + "'");
                rs.next();
                Add_workplace.setInt(1, rs.getInt("courier_id"));
                rs = stmt.executeQuery("select inland_city_id from sustc_test.public.inland_city where inland_city_name = '" + parts[3] + "'");
                rs.next();
                Add_workplace.setInt(2, rs.getInt("inland_city_id"));
                Add_courier.execute();
            }
        }
        System.out.println("Time = " + (System.currentTimeMillis() - start) + "ms");
    }
}
