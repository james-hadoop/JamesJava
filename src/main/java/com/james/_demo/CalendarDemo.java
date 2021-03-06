package com.james._demo;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class CalendarDemo {

    public static void main(String[] args) {
        CalendarDemo test = new CalendarDemo();
        // 当前日期前10天的日期
        List<String> dateList = test.getDatePeriod(new Date(), 10, "yyyy-MM-dd");
        for (String date : dateList) {
            System.out.println(date);
        }
        System.out.println();

        Calendar settledTime = Calendar.getInstance();
        System.out.println("settledTime: " + settledTime);

        Date dDate = settledTime.getTime();
        System.out.println("dDate: " + dDate);

        String date = new SimpleDateFormat("yyyyMMdd").format(dDate);
        System.out.println("date: " + date);

        String hour = new SimpleDateFormat("HH").format(dDate);
        System.out.println("hour: " + hour);
    }

    /**
     * created by James on 2020-05-07.
     * <p>
     * Get previousDays days before the given date
     */
    public List<String> getDatePeriod(Date date, int previousDays, String targetDateFormat) {
        List<String> datePeriodList = new ArrayList<String>();
        DateFormat dateFormat = new SimpleDateFormat(targetDateFormat);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int inputDayOfYear = cal.get(Calendar.DAY_OF_YEAR);
        for (int i = previousDays - 1; i >= 0; i--) {
            cal.set(Calendar.DAY_OF_YEAR, inputDayOfYear - i);
            datePeriodList.add(dateFormat.format(cal.getTime()));
        }
        return datePeriodList;
    }
}