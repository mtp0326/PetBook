package edu.upenn.cis.nets2120.hw3;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;

public class ComputeRanks {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(ComputeRanks.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	static int maxIters;
	static double delta;
	static boolean debugMode;
	
	public ComputeRanks() {
		System.setProperty("file.encoding", "UTF-8");
		maxIters = 25;
		delta = 30;
		debugMode =false;
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {

    // TODO Load the file filePath into an RDD (take care to handle both spaces and tab characters as separators)
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().split("\\s+"));
		JavaPairRDD<Integer, Integer> result = file.mapToPair(line ->{
			return new Tuple2<>(Integer.parseInt(line[1]), Integer.parseInt(line[0]));
		});

    return result;
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		// TODO Find the sinks in the provided graph
		JavaRDD<Integer> followers = network.distinct().mapToPair( r->{
			return new Tuple2<Integer, Integer>( r._2, r._1 );
		}).reduceByKey((i1, i2) -> i1 + i2).map(t -> {
			return t._1;
		});
		JavaRDD<Integer> followed = network.distinct().reduceByKey((i1, i2) -> i1 + i2)
				.map(t->{
					return t._1;
				});
		return followed.subtract(followers);
		
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run() throws IOException, InterruptedException {
		logger.info("Running");

		// Load the social network
		// followed, follower
		// TODO find the sinks
	    // TODO add back-edges
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(Config.SOCIAL_NET_PATH);
		JavaRDD<Integer> sinks = getSinks(network);
	
		JavaPairRDD<Integer, String> sinkPair = sinks.mapToPair(r ->{
				return new Tuple2<Integer,String>(r, "s");
		});
		
	
		JavaPairRDD<Integer, Tuple2<String, Integer>> joined = sinkPair.join(network);
		JavaPairRDD<Integer, Integer> backEdges = joined.mapToPair( r ->{
			return new Tuple2<Integer, Integer>(r._2._2, r._1 );
		});
		System.out.println("Added "+backEdges.count()+" backlinks");
		backEdges.collect().stream().forEach( item ->{
			System.out.println(item._1 + ", " + item._2 );
		}
				);
		
		JavaPairRDD<Integer, Integer> edgeRDD = network.union(backEdges)
				.mapToPair(t -> new Tuple2<Integer, Integer>(t._2, t._1));
		
		JavaRDD<Integer> nodes = edgeRDD.reduceByKey((a, b) -> a+b).keys();
		System.out.println("This graph contains "+ nodes.count() +" nodes and "+ network.count()+ " edges");
		System.out.println("all edges: ");
		edgeRDD.collect().stream().forEach( item ->{
			System.out.println( "("+ item._1 + ", " + item._2 + ")");
		});
		
		//assign weight to each back link
		JavaPairRDD<Integer, Double> nodeTransferRDD = edgeRDD
				.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0/item._2));
		//join the two RDDs together by key
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = edgeRDD.join(nodeTransferRDD);
		edgeTransferRDD.collect().stream().forEach( item ->{
			System.out.println(item._1 + ": (" + item._2._1 + ", " + item._2._2 + ")");
		}
				);
		
		double d = 0.15;
		
		int i = 1;
		boolean converge = false;
		JavaPairRDD<Integer,Double> pageRankRDD = edgeRDD.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0));
		while(i <= maxIters && !converge) {
		
	
			JavaPairRDD<Integer, Double> propogateRDD = edgeTransferRDD
					.join(pageRankRDD)
					.distinct()
					.mapToPair(item -> new Tuple2<Integer, Double>(item._2._1._1, item._2._2 * item._2._1._2));
			JavaPairRDD<Integer, Double> pageRankRDD2 = propogateRDD
					.reduceByKey((a,b) -> a+b)
					.mapToPair(item -> new Tuple2<Integer, Double>(item._1, d + (1-d) * item._2));
			JavaPairRDD<Double, Integer> difference = pageRankRDD.union(pageRankRDD2)
					.reduceByKey((a,b) -> a-b)
					.mapToPair(item -> 
						new Tuple2<Double, Integer>(item._2, item._1))
					.sortByKey(false, Config.PARTITIONS);
			pageRankRDD = pageRankRDD2;		
			
			if(difference.keys().take(1).get(0) <= delta) {
				converge = true;	
			}
			if(debugMode) {
				System.out.println("round " + i +":");
				pageRankRDD2.collect().stream().forEach(item ->{
					
					System.out.println(item._1 + ": "+item._2);
				}
						);
			
			}
		
		i++;	
		}
				
				

		logger.info("*** Finished social network ranking! ***");
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
	}
	
	

	public static void main(String[] args) {
		final ComputeRanks cr = new ComputeRanks();
		for(int i = 0; i < args.length; i ++) {
			System.out.println(args[i]);
		}
		
		try {
			
			System.out.println("main!!");
			cr.initialize();
			if(args.length == 1) {
				delta = Double.parseDouble(args[0]);
				System.out.println("maxIters: " + maxIters +" delta: "+ delta+ " debug: "+ debugMode);
				
			}else if (args.length == 2) {
				delta = Double.parseDouble(args[0]);
				maxIters = Integer.parseInt(args[1]);
				System.out.println("maxIters: " + maxIters +" delta: "+ delta+ " debug: "+ debugMode);
				
			}else if(args.length>=3){
				debugMode = true; 
				delta = Double.parseDouble(args[0]);
				maxIters = Integer.parseInt(args[1]);
				System.out.println("maxIters: " + maxIters +" delta: "+ delta+ " debug: "+ debugMode);
				
				
			}
			else if(args.length ==0){
				System.out.println("no args!");
			}
		
			
			cr.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}
	}

}
