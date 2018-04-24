import edu.ucsb.nceas.*;
import java.io.File;

public class Main {

    public static void main(String[] args) {
        System.out.println("Hello World!");

        DBHandler dbHandler = new DBHandler();

        try {
            // dbHandler.runInsertion("metricsMillion");
            // the run insertion function to populate the database table

            // the method runs the queries from the SQL file.
            dbHandler.runSQL(new File("SQLqueries/indexing.sql"));

        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }

    }

}
