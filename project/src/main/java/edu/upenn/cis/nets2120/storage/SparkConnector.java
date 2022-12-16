package edu.upenn.cis.nets2120.storage;

import java.io.File;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConnector {
	static SparkSession spark = null;
	static JavaSparkContext context = null;
	
	public static SparkSession getSparkConnection() {
		return getSparkConnection(null);
	}
	
	public static synchronized SparkSession getSparkConnection(String host) {
		if (spark == null) {
			if (System.getenv("HADOOP_HOME") == null) {
				File workaround = new File(".");
				
				System.setProperty("hadoop.home.dir", workaround.getAbsolutePath() + "/native-libs");
			}
			
		    spark = SparkSession
		            .builder()
		            .appName("Homework2")
		            .master((host == null) ? "local[*]" : host)
		            .getOrCreate();
		}
		
	    return spark;
	}

	public static synchronized JavaSparkContext getSparkContext() {
		if (context == null)
			context = new JavaSparkContext(getSparkConnection().sparkContext());
		
		return context;
	}
}
