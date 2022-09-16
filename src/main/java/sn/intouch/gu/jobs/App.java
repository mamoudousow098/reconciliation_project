package sn.intouch.gu.jobs;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;


public class App 
{   
    private Connection connection = null;

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
        int count = 1;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {   
            pstmt.setTimestamp(1, new Timestamp(endDate));
            // pstmt.setDate(1, new java.sql.Date(endDate));
            pstmt.setInt(2, limit);
            Date now  = new Date();
            // execute the delete statement
            while (count != 0 && now.getHours() < stopHour) {
                count = pstmt.executeUpdate();
                System.out.println("" + count + " rows deleted.");
                Thread.sleep(1000 * interval);
            }
            connection.close();
        } catch (SQLException | InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
    public static void main(String[] args )
    {
        long now = new Date().getTime();
        App app = new App();
        app.connect(args[0], args[1], args[2]);

        String table = args[3], column = args[4];
        long deleteBefore = Long.valueOf(args[5]).longValue();
        int limit = Integer.valueOf(args[6]).intValue(), interval = Integer.valueOf(args[7]).intValue(), stopHour = Integer.valueOf(args[8]).intValue();
        app.delete(table, column, now - 1000 * 60 * 60 * deleteBefore, limit, interval, stopHour);
    }
}
