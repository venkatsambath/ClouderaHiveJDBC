
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJDBCExample {
    static String JDBCDriver = "com.cloudera.hive.jdbc41.HS2Driver";
    static String ConnectionURL = "jdbc:hive2://host-10-17-101-84.coe.cloudera.com:10000;AuthMech=1;KrbRealm=CITIDSE" +
            ".COM;KrbHostFQDN=host-10-17-101-84.coe.cloudera.com;KrbServiceName=hive;LogLevel=6;" +
            "LogPath=/Users/venkat/logs;RowsFetchedPerBlock=1000";
    public static void main(String[] args) {
        Connection con = null; Statement stmt = null; ResultSet rs = null;
        String query = "select * from $table";
        try {
            Class.forName(JDBCDriver);
            con = DriverManager.getConnection(ConnectionURL);
            stmt = con.createStatement();
            stmt.setFetchSize(1000);
            rs = stmt.executeQuery("select * from airline_data.airlines_240k");
            while(rs.next()) {
                System.out.printf(rs.getString(1)); }
        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {if (rs != null) {
                rs.close();
            }
            } catch (SQLException se1) {}
            try { if (stmt!=null) {stmt.close();}}
            catch (SQLException se2) {}
            try { if (con!=null) {con.close();}}
            catch (SQLException se3) {se3.printStackTrace();}
        }
    }
}