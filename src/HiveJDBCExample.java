
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJDBCExample {
    static String JDBCDriver = "com.cloudera.hive.jdbc.HS2Driver";
    static String ConnectionURL = "jdbc:hive2://xxx:10000;AuthMech=1;KrbRealm=C6CITIDSE" +
            ".COM;KrbHostFQDN=xxx;KrbServiceName=hive;LogLevel=6;" +
            "LogPath=/Users/svenkataramanasam/logs;RowsFetchedPerBlock=10000";
    public static void main(String[] args) {
        Connection con = null; Statement stmt = null; ResultSet rs = null;
        try {
            Class.forName(JDBCDriver);
            con = DriverManager.getConnection(ConnectionURL);
            stmt = con.createStatement();
            stmt.setFetchSize(10000);
            rs = stmt.executeQuery("select * from airline_data.airlines_1million limit 370000");
            long time_1 = System.currentTimeMillis();
            while(rs.next()) {
               // System.out.printf(rs.getString(1));
            }
            long time_2 = System.currentTimeMillis();
            long difference = time_1 - time_2;
            System.out.println( difference + "milliseconds" );
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
