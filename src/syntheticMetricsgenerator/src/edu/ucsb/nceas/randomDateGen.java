package edu.ucsb.nceas;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class randomDateGen {
    public static List<Date> randomDate(String str_date, String end_date) throws ParseException {
        List<Date> dates = new ArrayList<Date>();

        DateFormat formatter ;

        formatter = new SimpleDateFormat("dd-MM-yyyy");
        Date  startDate = (Date)formatter.parse(str_date);
        Date  endDate = (Date)formatter.parse(end_date);
        long interval = 24*1000 * 60 * 60; // 1 hour in millis
        long endTime =endDate.getTime() ; // create your endtime here, possibly using Calendar or Date
        long curTime = startDate.getTime();
        while (curTime <= endTime) {
            dates.add(new Date(curTime));
            curTime += interval;
        }
        return dates;

    }

    public static int randBetween(int start, int end) {
        return start + (int)Math.round(Math.random() * (end - start));
    }

}
