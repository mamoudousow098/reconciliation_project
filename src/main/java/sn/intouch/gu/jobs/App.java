package sn.intouch.gu.jobs;

import com.mysql.cj.jdbc.Driver;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;


public class App
{
    //private Connection connection1 = null;
    //private  Connection connection2 = null ;
    //private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Connection connect(String database, String user, String password) {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {e.printStackTrace();}
        String url = "jdbc:mysql://" + database + "?autoReconnect=true&failOverReadOnly=false&maxReconnects=10&serverTimezone=UTC";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Succesful connection");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn ;
    }

    private void updateParameter(String databaseMaster, String userMaster, String passwordMaster, String prmCode, String prmStringValue) throws SQLException {

        Connection connection = connect(databaseMaster, userMaster,passwordMaster);
        String updatePrmStmt = "UPDATE parametre SET prm_stringvalue = ? WHERE prm_code = ?";
        PreparedStatement myStmt2 = connection.prepareStatement(updatePrmStmt);
        myStmt2.setString(1, prmStringValue);
        myStmt2.setString(2, prmCode);

        int rowsAffected = myStmt2.executeUpdate();

        if (rowsAffected > 0) {
            System.out.println("Update successful.");
        } else {
            System.out.println("Update failed. No records found with the provided ID.");
        }

        myStmt2.close();
        connection.close();
    }


    private String retrieveParameterStringValue(String databaseMaster, String userMaster, String passwordMaster,String prmCode) throws SQLException {

        Connection connection = connect(databaseMaster, userMaster,passwordMaster);
        String selectPrmStmt = "SELECT prm_stringvalue FROM parametre WHERE prm_code = ?";
        PreparedStatement myStmt = connection.prepareStatement(selectPrmStmt);
        myStmt.setString(1, prmCode);

        ResultSet myRs = myStmt.executeQuery();
        String prmStringValue = null;

        if (myRs.next()) {
            prmStringValue = myRs.getString("prm_stringvalue");
        }

        myRs.close();
        myStmt.close();
        connection.close();
        return prmStringValue;
    }

    public void changePointeur(String databaseMaster, String userMaster, String passwordMaster,
                               String databaseReplica, String userReplica, String passwordReplica,
                               String table, String columnName, long secondToPassToMaster, long secondToPassToReplica) throws SQLException {

        Connection connection2 = connect(databaseReplica, userReplica,passwordReplica);

        // you can use differents schemas retrieve, modify value from master and to replica
        //String statement = "SELECT transaction_date FROM " + table + " ORDER BY " + columnName + " DESC LIMIT 1";
        String statement = "SELECT max(" + columnName + ") as " + columnName + " FROM " + table ;
        PreparedStatement myStmt = connection2.prepareStatement(statement);

        ResultSet myRs = myStmt.executeQuery();
        Timestamp datetimeValue = null;

        if (myRs.next()) {
            datetimeValue = myRs.getTimestamp("transaction_date");
        }

        System.out.println(datetimeValue);

        long diff = new Date().getTime() - (datetimeValue != null ? datetimeValue.getTime() : 0);

        String prmStringValue = retrieveParameterStringValue(databaseMaster, userMaster, passwordMaster, "B2B_IS_BOGERANT_ACTIVATED");
        System.out.println(prmStringValue);

        if ("0".equals(prmStringValue)) System.out.println("I am on the master");
        else  {
            System.out.println("I am on the replica");
        }

        // if you are at the master and the difference between now and the last record date
        // is lower than the intended second so switch to Replica
        if ("0".equals(prmStringValue) && (diff < secondToPassToReplica * 1000)) {

            updateParameter(databaseMaster, userMaster, passwordMaster,"B2B_IS_BOGERANT_ACTIVATED", "1");
            System.out.println("Switch to Replica ");

        }
        // if you are at the Replica and the difference between now and teh last record date
        // is greater than the intended seconds so switch to Master
        else if ("1".equals(prmStringValue) && (diff > secondToPassToMaster * 1000)) {
            updateParameter(databaseMaster, userMaster, passwordMaster,"B2B_IS_BOGERANT_ACTIVATED", "0");
            System.out.println("Switch to Master ");
        }

        myRs.close();
        myStmt.close();
        connection2.close();
    }


    public static void main(String[] args ) throws SQLException {

        App app = new App();
        int  PassToMaster = Integer.parseInt(args[6]) ;
        int PassToReplica = Integer.parseInt(args[7]) ;

        while ( true ) {

            app.changePointeur(args[0], args[1],args[2],args[3], args[4],args[5],"transactiongu", "transaction_date", PassToMaster, PassToReplica);

            try {
                // Sleep for 5 seconds
                long sleepTimeMillis = Integer.parseInt(args[8]) * 1000L; // 5 seconds
                Thread.sleep(sleepTimeMillis);
                System.out.println("Thread slept for " + sleepTimeMillis + " milliseconds.");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
