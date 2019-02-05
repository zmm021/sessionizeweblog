package com.bigdata.framework.worker.Session;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JobSessionAnaly {



    /**
     * the entrance of this job
     * @param args args[0] is the application name
     *             args[1] is the file path
     *             args[2] is the output file path of unique urls
     */
    public static void main(String[] args) {
        //configure spark
        SparkConf sparkConf = new SparkConf()
                .setAppName(args[0])
                .setMaster("local[4]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //load the file
        JavaRDD<String> rawLog = sparkContext.textFile(args[1]);

        //Sessionize the web log by IP.
        JavaPairRDD<String, Tuple4<Date,Date,Long,List<String>>> sessionList = rawLog
                //cleaning the data
                .map(s -> Operators.cleanRow(s))
                .filter(s -> !s.equals(Operators.ERROR_ROW))
                //mapping, parsing the rows
                .mapToPair(s -> Operators.parseRow(s))
                //reducing, merging the sessions
                .reduceByKey((a, b) -> Operators.mergeSession(a,b))
                .flatMapValues(
                        (Function<List<Tuple3<Date, Date, List<String>>>, Iterable<Tuple4<Date, Date, Long, List<String>>>>) tuple3s -> {
                            ArrayList<Tuple4<Date, Date, Long, List<String>>> newTupleList = new ArrayList<>();
                            for (Tuple3<Date, Date, List<String>> i : tuple3s) {
                                Long duration = (i._2().getTime() - i._1().getTime()) / 1000;
                                newTupleList.add(new Tuple4<>(i._1(),i._2(), duration , i._3()));
                            }
                            return newTupleList;
                        })
                .filter(s -> s._2()._3()>0)
                .cache()
                ;

        //calculate the average session time
        JavaRDD<Long> sessionTime = sessionList.map(s -> s._2._3());
        Long totalTime = sessionTime.reduce((a,b) -> a + b);
        System.out.println("Average session duration: " + totalTime/sessionList.count() + " seconds");
        /**
         * calculate the unique URL visits per session
         * Note that during the sessionize phase, the urls of each session are already being de-duplicated
         * hereby it only needs to count the size of the url list
         */
        JavaPairRDD<String, Integer> uniqueUrls = sessionList.mapValues(s -> s._4().size());
        output(args[2],uniqueUrls);
        /**
         *Find the most engaged users, ie the IPs with the longest session times
         */
        JavaPairRDD<Long,String> engagedUsers = sessionList
                .mapToPair(s -> new Tuple2<>(s._2._3(),s._1()))
                .sortByKey(false)
                ;

        System.out.println("Top 10 engaged users: " + engagedUsers.take(10));
    }

    private static void output(String filePath, JavaPairRDD<String, Integer> uniqueUrls){
         //output the file here
    }

}

