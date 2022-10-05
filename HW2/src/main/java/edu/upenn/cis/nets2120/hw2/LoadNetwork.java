package edu.upenn.cis.nets2120.hw2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class LoadNetwork {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(LoadNetwork.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	Table talks;
	
	CSVParser parser;
	
	
	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	/**
	 * Helper function: swap key and value in a JavaPairRDD
	 * 
	 * @author zives
	 *
	 */
	static class SwapKeyValue<T1,T2> implements PairFunction<Tuple2<T1,T2>, T2,T1> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<T2, T1> call(Tuple2<T1, T2> t) throws Exception {
			return new Tuple2<>(t._2, t._1);
		}
		
	}
	
	
	public LoadNetwork() {
		System.setProperty("file.encoding", "UTF-8");
		parser = new CSVParser();
	}
	
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			talks = db.createTable("ted_talks", Arrays.asList(new KeySchemaElement("talk_id", KeyType.HASH)), // Partition
																												// key
					Arrays.asList(new AttributeDefinition("talk_id", ScalarAttributeType.N)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			talks.waitForActive();
		} catch (final ResourceInUseException exists) {
			talks = db.getTable("ted_talks");
		}

	}
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, DynamoDbException, InterruptedException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		initializeTables();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		// Read into RDD with lines as strings
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split(" "));
		
    // TODO Complete this method
    return NULL;
	}

	/**
	 * Returns an RDD of parsed talk data
	 * 
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	JavaRDD<Row> getTalks(String filePath) throws IOException {
		CSVReader reader = null;
		Reader fil = null;

		try {
			fil = new BufferedReader(new FileReader(new File(filePath)));
			reader = new CSVReader(fil);
			// Read + ignore header
			try {
				reader.readNext();
			} catch (CsvValidationException e) {
				// This should never happen but Java thinks it could
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
	
			String[] nextLine = null;
			try {
				do {
					try {
						nextLine = reader.readNext();
						//TODO Do something with the line
					} catch (CsvValidationException e) {
						e.printStackTrace();
					}					
				} while (nextLine != null);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} finally {
			if (reader != null)
				reader.close();
			
			if (fil != null)
				fil.close();
		}
		
    return null;
	}
	
	
	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run() throws IOException, DynamoDbException, InterruptedException {
		logger.info("Running");

		// Load + store the TED talks
		JavaRDD<Row> tedTalks = this.getTalks(Config.TED_TALK_PATH);

		// Load the social network
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(Config.SOCIAL_NET_PATH);

		// TODO Add your code here
		
	}

	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");
		
		DynamoConnector.shutdown();
		
		if (spark != null)
			spark.close();
	}
	
	public static void main(String[] args) {
		final LoadNetwork ln = new LoadNetwork();

		try {
			ln.initialize();

			ln.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			ln.shutdown();
		}
	}

}
