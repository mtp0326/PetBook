package edu.upenn.cis.nets2120.hw2;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import edu.upenn.cis.nets2120.storage.SparkConnector;

public class RDDTest {
	
	@Test
	public void testMyCounter() {
		SparkSession spark = SparkConnector.getSparkConnection();
		JavaSparkContext sparkContext = null;
		
		try {
			sparkContext = new JavaSparkContext(spark.sparkContext());
			sparkContext.setLogLevel("DEBUG");
	
			// Simple data -- two row RDD
			List<String[]> simpleData = new ArrayList<>();
			simpleData.add(new String[] { "1", "string1" });
			simpleData.add(new String[] { "2", "string2" });
			
			JavaRDD<Row> rowRDD = sparkContext
					.parallelize(simpleData)
					.map((String[] row) -> RowFactory.create(row[0], row[1]));
	
			// Create schema
			StructType schema = DataTypes
					.createStructType(new StructField[] {
							DataTypes.createStructField("left", DataTypes.StringType, false),
							DataTypes.createStructField("right", DataTypes.StringType, false)
					});
	
			// Create a high level dataframe to test
			Dataset<Row> df = spark.sqlContext().createDataFrame(rowRDD, schema).toDF();
			
			List<Row> results = df.collectAsList();
			
			assertEquals(2, results.size());
		} finally {
			if (sparkContext != null)
				sparkContext.close();
		}
	}
}
