package com.bigdata.framework.worker.Session;

import scala.Tuple2;
import scala.Tuple3;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Operators {

    public static final String ERROR_ROW="error";

    private static final String Time_Format = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    private static final int window_size = 15 * 60 * 1000;

    private static final Pattern LogRegex = Pattern.compile(
            "^(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\d{3}Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/.+"
    );

    /**
     * functions for data cleaning, currently only does reg-matching, can also do anomaly detection
     * @param row
     * @return
     */
    public static String cleanRow(String row) {

        if (!LogRegex.matcher(row).matches()) {
            return ERROR_ROW;
        }
        return row;
    }

    /**
     * mapping function, used to parse string row to tuple
     * @param row
     * @return touple of (IP, SessionList)
     */
    public static Tuple2<String, List<Tuple3<Date, Date, List<String>>>> parseRow(String row) {

        SimpleDateFormat dateFormat = new SimpleDateFormat(Time_Format);
        Matcher m = LogRegex.matcher(row);
        if (m.find()) {
            try {
                Date accessTime = dateFormat.parse(m.group(1));
                String ip = m.group(2);
                String url = m.group(3);
                LinkedList<String> urls = new LinkedList<>();
                urls.add(url);
                LinkedList<Tuple3<Date, Date, List<String>>> sessions = new LinkedList<>();
                sessions.add(new Tuple3<>(accessTime, accessTime, urls));
                return new Tuple2<>(ip, sessions);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return new Tuple2<>(null, null);
    }

    /**
     * reducing function, used to Sessionize the web log .
     * @param mergedList
     * @param tempList
     * @return a touple with (starttime, endtime, urls)
     */
    public static List<Tuple3<Date, Date, List<String>>> mergeSession(List<Tuple3<Date, Date, List<String>>> mergedList,
                                                                       List<Tuple3<Date, Date, List<String>>> tempList) {
        Tuple3<Date, Date, List<String>> tempAccess = tempList.get(0);
        String tempAccUrl = tempAccess._3().get(0);
        long tempAccTime = tempAccess._1().getTime();
        long startTime, endTime;
        int insertIndex = -1, index = 0;
        Tuple3<Date, Date, List<String>> mergedSession = null;
        for (Tuple3<Date, Date, List<String>> preAccess : mergedList) {
            startTime = preAccess._1().getTime();
            endTime = preAccess._2().getTime();
            if (tempAccTime >= startTime && tempAccTime <= endTime) {//in between, merge, finished
                mergedSession = preAccess;
                break;
            }
            if (tempAccTime < startTime) {//tempAcc is on the left
                if (startTime - tempAccTime > window_size) {//no merge, insert
                    insertIndex = index;
                    break;
                } else if (endTime - tempAccTime <= window_size) {//merge
                    preAccess._1().setTime(tempAccTime);
                    mergedSession = preAccess;
                    break;
                }
            } else {//tempAcc is on the right
                if (tempAccTime - endTime > window_size) {//no merge, insert
                    insertIndex = index + 1;
                    break;
                } else if (tempAccTime - startTime <= window_size) {//merge
                    preAccess._2().setTime(tempAccTime);
                    mergedSession = preAccess;
                    break;
                }
            }
            index++;
        }
        if (insertIndex >= 0) {
            mergedList.add(insertIndex, tempAccess);
        } else {
            if (!mergedSession._3().contains(tempAccUrl)) {
                mergedSession._3().add(tempAccUrl);
            }
        }
        return mergedList;
    }


}
