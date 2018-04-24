package edu.ucsb.nceas;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class csvReader {

    // This method gets the country code from a list of csv file and returns the list of codes to the calling function.
    public static List<String> getCountries() {

        String csvFile = "countries.csv";
        String line = "";
        String cvsSplitBy = ",";
        List<String > Codes =  new ArrayList<>();;

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(cvsSplitBy);

                Codes.add(country[country.length - 1]);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return Codes;
    }
}
