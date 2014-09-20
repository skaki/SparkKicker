package com.rw;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SparkKicker {

	public static void main(String[] args) {
	    String logFile = "/tmp/*.profile"; // Should be some file on your system
	    SparkConf conf = new SparkConf().setAppName("EventScout SparkKicker");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile).cache();

	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("Life"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("Realtor"); }
	    }).count();

	    System.out.println("Lines with Life: " + numAs + ", lines with Coach: " + numBs);
	  }
}

