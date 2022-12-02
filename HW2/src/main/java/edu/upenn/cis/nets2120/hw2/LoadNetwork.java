package edu.upenn.cis.nets2120.hw2;


import org.apache.spark.sql.catalyst.expressions.GenericRow;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
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
import java.util.*;
import java.util.stream.Collectors;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
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
		JavaPairRDD<Integer, Integer> result = file.mapToPair(line ->{
			return new Tuple2<>(Integer.parseInt(line[0]), Integer.parseInt(line[1]));
		});

    return result;
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
		List<String[]> readLines = new ArrayList<>();  
		
		                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
		final StructType schema = new StructType()
			.add("talk_id", "int")
			.add("description", "string")
			.add("title", "string")
			.add("speaker_1", "string")
			.add("views", "int")
			.add("duration", "int")
			.add("topics", "string")
			.add("related_talks", "string")
			.add("url", "string");

		try {
			fil = new BufferedReader(new FileReader(new File(filePath)));
			reader = new CSVReader(fil);
			int category[] = new int[9];
			// Read + ignore header
			try {
				String[] input = reader.readNext();
				
				for(int i = 0; i < input.length; i++) {
					if((input[i]).equals("talk_id")){
						category[0] = i;
					} else if((input[i]).equals("description")){
						category[1] = i;
					} else if((input[i]).equals("title")){
						category[2] = i;
					} else if((input[i]).equals("speaker_1")){
						category[3] = i;
					} else if((input[i]).equals("views")){
						category[4] = i;
					} else if((input[i]).equals("duration")){
						category[5] = i;
					} else if((input[i]).equals("topics")){
						category[6] = i;
					} else if((input[i]).equals("related_talks")){
						category[7] = i;
					} else if((input[i]).equals("url")){
						category[8] = i;
					} 
						
				}
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
						String []s = new String[9];
						if(nextLine!=null) {
							for(int i = 0; i < 9; i++) {
								s[i] = nextLine[category[i]];

							}
							readLines.add(s);
						
						}
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
		List<Row> lines = readLines.parallelStream()
				.map(l -> {
					return new GenericRowWithSchema(l, schema);
				}).collect(Collectors.toList());
		JavaRDD<Row> lineRDD = context.parallelize(lines);
		
		
    return lineRDD;
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
		JavaRDD<Row> sortedLine = tedTalks.sortBy(row -> row.getAs("views"), false, Config.PARTITIONS);
	

		
		
	
		
		sortedLine.foreachPartition(iter ->{
			DynamoDB dbi = DynamoConnector.getConnection(Config.DYNAMODB_URL);
			TableWriteItems table = new TableWriteItems("ted_talks");
			Set<Item> tempSet = new HashSet<>();
			while(iter.hasNext()) {
				Row l = iter.next();
				if(tempSet.size()==24) {
					table.withItemsToPut(tempSet);
					BatchWriteItemOutcome result = dbi.batchWriteItem(table);
					while(result.getUnprocessedItems().size() > 0) {
						result = dbi.batchWriteItemUnprocessed(result.getUnprocessedItems());
					}
					tempSet.clear();
				}
				tempSet.add(new Item()
					.withPrimaryKey("talk_id", Integer.parseInt(l.getAs("talk_id")))
					.withString("description", l.getAs("description"))
					.withString("title", l.getAs("title"))
					.withString("speaker_1", l.getAs("speaker_1"))
					.withInt("views", Integer.parseInt(l.getAs("views")))
					.withInt("duration", Integer.parseInt(l.getAs("duration")))
					.withString("topics", l.getAs("topics"))
					.withString("related_talks", l.getAs("related_talks"))
					.withString("url", l.getAs("url"))
					); 
				if(tempSet.size()!=0 && !iter.hasNext()) {
					table.withItemsToPut(tempSet);
					BatchWriteItemOutcome result = dbi.batchWriteItem(table);
					while(result.getUnprocessedItems().size() > 0) {
						result = dbi.batchWriteItemUnprocessed(result.getUnprocessedItems());
					}
					tempSet.clear();
				}
			}
			
		}); 
				
			
	
		JavaPairRDD<Integer, Integer> networkSortedRDD = network.mapToPair(
					f -> {
						return new Tuple2<>(f._1, 1);
					});
		JavaPairRDD<Tuple2<Integer, Integer>, Long> countRDD = networkSortedRDD.reduceByKey((i1, i2) -> i1 + i2)
				.mapToPair(t -> {
					return new Tuple2<Integer, Integer>(t._2, t._1);
				})
				.sortByKey(false)
				.zipWithIndex();
		JavaPairRDD<Long, Integer> viewRDD = countRDD
				.mapToPair(t -> {
					return new Tuple2<Long, Integer>( t._2, t._1._2 );
				});
				
		
		JavaPairRDD<Long, Row> tedTalksRDD = tedTalks.zipWithIndex()
				.mapToPair(t ->{
					return new Tuple2<Long, Row>(t._2, t._1);
				});
		JavaPairRDD<Integer, Row> mergeTable;
		if(viewRDD.count() < tedTalksRDD.count()) {
			mergeTable = viewRDD.cogroup(tedTalksRDD)
				.filter(f -> {
					return (f._2._1.iterator().hasNext() && f._2._2.iterator().hasNext());
					})
					.mapToPair( r -> {
				return new Tuple2<Integer,Row>(r._2._1.iterator().next(), r._2._2.iterator().next());
			});
		}else {
			mergeTable = tedTalksRDD.cogroup(viewRDD)
					.filter(f -> {
						return (f._2._1.iterator().hasNext() && f._2._2.iterator().hasNext());
						})
					.mapToPair( r -> {
				return new Tuple2<Integer, Row>(r._2._2.iterator().next(), r._2._1.iterator().next());
			});
		}
		
		mergeTable.foreachPartition(itr -> {
				while(itr.hasNext()) {
					DynamoDB dbt = DynamoConnector.getConnection(Config.DYNAMODB_URL);
					Table t = dbt.getTable("ted_talks");
					Tuple2<Integer, Row>i = itr.next();
					Row r = i._2;
					Integer in = i._1;
					
					AttributeUpdate u = new AttributeUpdate("social_id").addNumeric(in);	
					UpdateItemSpec uis = new UpdateItemSpec()
							.withPrimaryKey("talk_id", Integer.parseInt(r.getAs("talk_id")))
							.withAttributeUpdate(u);
					t.updateItem(uis);
				}
				
			});
	
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
