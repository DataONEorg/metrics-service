package edu.ucsb.nceas;
import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DBHandler {

    // Method to run the insertion in the table.
    public void runInsertion(String tableName) {

        ConnectionConfig cg = new ConnectionConfig();
        Connection cn;
        cn = cg.getConnection();
        generateSubjects gs = new generateSubjects();
        randomDateGen rd = new randomDateGen();
        List<Date> dates = null;
        List<String> codes;
        DateFormat formatter ;
        csvReader c = new csvReader();

        //Get random values to be populated in the database
        String[] metrics = {"Citations", "Total_Dataset_Investigations", "Unique_Dataset_Investigations", "Total_Dataset_Requests", "Unique_Dataset_Requests" };
        String[] repo = gs.Subject("Repo", 10);
        String[] users = gs.Subject("User", 10);
        String[] funding = gs.Subject("Funding", 10);
        String[] award = gs.Subject("Award", 100);
        String[] dataset = gs.Subject("Dataset", 100);
        formatter = new SimpleDateFormat("yyyy-MM-dd");
        codes = c.getCountries();

        // Get random dates between these range
        try {
            dates = rd.randomDate("01-01-2010", "03-03-2018");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Populating the db with synthetic data
        try {
            cn.setAutoCommit(false);
            Statement stmt = cn.createStatement();
            // Inserting into the database
            for(int i = 0; i < 100000000; i++) {
                Date lDate =(Date)dates.get(rd.randBetween(0,dates.size() - 1));
                String ds = formatter.format(lDate);

                String sql = "INSERT INTO" + tableName + " (serial_no, dataset_id, user_id, repository, funding_number,"
                        + " award_number, day, month, year, location, metrics_name, metrics_value) "
                        + "VALUES (DEFAULT, "
                        + "'" + dataset[rd.randBetween(0,dataset.length - 1 )] + "'" + ","
                        + "'" + users[rd.randBetween(0,users.length - 1)] + "'" + ","
                        + "'" + repo[rd.randBetween(0,repo.length - 1)] + "'" + ","
                        + "'" + funding[rd.randBetween(0,funding.length - 1)] + "'" + ","
                        + "'" + award[rd.randBetween(0,award.length - 1)] + "'" + ","
                        + Integer.parseInt(ds.substring(8,10)) + ","
                        + Integer.parseInt(ds.substring(5,7)) + ","
                        + Integer.parseInt(ds.substring(0,4)) + ","
                        + "'" + codes.get(rd.randBetween(1,codes.size() - 1)) + "'" + ","
                        + "'" + metrics[rd.randBetween(0,metrics.length - 1)] + "'" + ","
                        + rd.randBetween(0,100) + ");" ;

                stmt.executeUpdate(sql);

                System.out.println("Row inserted : " + i);
            }

            stmt.close();
            cn.commit();
            cn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("All rows inserted!");
    }



    // This method takes sql file as a parameter and executes all the queries from that file in batches.
    public void runSQL(File file) {
        ConnectionConfig cg = new ConnectionConfig();
        Connection cn;
        cn = cg.getConnection();
        String s = new String();
        StringBuffer sb = new StringBuffer();

        FileReader fr = null;
        try {
            fr = new FileReader(file);
            // be sure to not have line starting with "--" or "/*" or any other non aplhabetical character

            BufferedReader br = new BufferedReader(fr);

            while((s = br.readLine()) != null)
            {
                sb.append(s);
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        // here is our splitter ! We use ";" as a delimiter for each request
        // then we are sure to have well formed statements
        String[] inst = sb.toString().split(";");

        try {
            cn.setAutoCommit(false);
            Statement stmt = cn.createStatement();

            for(int i = 0; i < inst.length; i++) {
                String query = inst[i] + ";";
                stmt.addBatch(query);

            }

            stmt.executeBatch();
            stmt.close();
            cn.commit();
            cn.close();
            System.out.println("Batch executed! ");
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
    }
}
