package sn.intouch.gu.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;


public class App 
{   
    private Connection connection = null;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String DROP_KEYWORD = "DROP";
    private static final String TRANSACTIONGU = "transactiongu";

    private static final String USSD_OPERATIONS = "ussd_operations";

    private void connect(String database, String user, String password) {
        try {
            DriverManager.registerDriver(new com.mysql.cj.jdbc.Driver());
        } catch (SQLException e) {e.printStackTrace();}  
        String url = "jdbc:mysql://localhost/" + database;
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        this.connection = conn;
    }

    public void delete(String table, String dateColumn, long endDate, int limit, int interval, int stopHour) {
        String sql = "DELETE FROM " + table +" WHERE " + dateColumn + " < ? LIMIT ?" ;
        if (table.contains(TRANSACTIONGU)) {
            sql = "DELETE FROM " + table +" WHERE " + dateColumn + " < ? AND message_retour_moteur = 'SUCCESS' LIMIT ?" ;
        } else if (table.contains(USSD_OPERATIONS)){
            sql = "DELETE FROM " + table +" WHERE " + dateColumn + " < ? AND tag != 'sended' LIMIT ?" ;
        }
        int count = 1;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {   
            pstmt.setTimestamp(1, new Timestamp(endDate));
            pstmt.setInt(2, limit);
            System.out.println(sql);
            while (count != 0 && new Date().getHours() < stopHour) {
                count = pstmt.executeUpdate();
                System.out.println(DATE_FORMAT.format(new Date())  + " ::: " + table + " "+ count + " rows deleted.");
                Thread.sleep(1000 * interval);
            }
            connection.close();
        } catch (SQLException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    public void dropTable(String table) {
        String sql = "DROP TABLE " + table;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {                   
            System.out.println(DATE_FORMAT.format(new Date())  + " ::: DROP TABLE " + table + " "+ pstmt.executeUpdate());
            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void truncateTable(String table) {
        String sql = "TRUNCATE " + table;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {                   
            System.out.println(DATE_FORMAT.format(new Date())  + " ::: TRUNCATE TABLE " + table + " " + pstmt.executeUpdate());
            connection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void main(String[] args )
    {
        long now = new Date().getTime();
        App app = new App();
        app.connect(args[0], args[1], args[2]);

        String table = args[3];
        if (args[4].equals(DROP_KEYWORD)) {
            app.dropTable(table);    
            return;
        }
        
        
        long deleteBefore = Long.valueOf(args[5]).longValue();
        if (deleteBefore == 0) {
            app.truncateTable(table);
            return;
        }
        
        String column = args[4];

        int limit = Integer.valueOf(args[6]).intValue(), interval = Integer.valueOf(args[7]).intValue(), stopHour = Integer.valueOf(args[8]).intValue();
        app.delete(table, column, now - 1000 * 60 * 60 * deleteBefore, limit, interval, stopHour);
    }
}
