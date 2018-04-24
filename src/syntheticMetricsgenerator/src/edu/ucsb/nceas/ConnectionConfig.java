package edu.ucsb.nceas;

import java.sql.Connection;
import java.sql.DriverManager;


public class ConnectionConfig {
    public static Connection getConnection() {
        Connection conn = null;
        System.out.println("Begenning the connection process!");

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mdc", "rushirajnenuji", "");
            conn.setAutoCommit(false);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        return conn;
    }
}
