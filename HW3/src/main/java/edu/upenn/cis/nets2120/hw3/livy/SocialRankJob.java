package edu.upenn.cis.nets2120.hw3.livy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class SocialRankJob implements Job<List<MyPair<Integer,Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;

	private boolean useBacklinks;

	private String source;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		System.out.println("Connected!");
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
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<MyPair<Integer,Double>> run() throws IOException, InterruptedException {
		System.out.println("Running");

		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(source);
		JavaPairRDD<Integer, Integer> edgeRDD;
		System.out.println("useBackLinks: " + useBacklinks);
		if(useBacklinks) {
			// Take the TO nodes
			JavaRDD<Integer> sinks = getSinks(network);

			// TODO Your code from ComputeRanks here

			JavaPairRDD<Integer, String> sinkPair = sinks.mapToPair(r ->{
					return new Tuple2<Integer,String>(r, "s");
			});
			
			
			
			JavaPairRDD<Integer, Tuple2<String, Integer>> joined = sinkPair.join(network);
			JavaPairRDD<Integer, Integer> backEdges = joined.mapToPair( r ->{
				return new Tuple2<Integer, Integer>(r._2._2, r._1 );
			});
			System.out.println("Added "+backEdges.count()+" backlinks");
//			backEdges.collect().stream().forEach( item ->{
//				System.out.println(item._1 + ", " + item._2 );
//			}
//					);
			
		 edgeRDD = network.union(backEdges)
					.mapToPair(t -> new Tuple2<Integer, Integer>(t._2, t._1));
		}
		
		else {
			edgeRDD = network
					.mapToPair(t -> new Tuple2<Integer, Integer>(t._2, t._1));
		}

		
				
		JavaRDD<Integer> nodes = edgeRDD.reduceByKey((a, b) -> a+b).keys();
		System.out.println("This graph contains "+ nodes.count() +" nodes and "+ network.count()+ " edges");
//		System.out.println("all edges: ");
//		edgeRDD.collect().stream().forEach( item ->{
//			System.out.println( "("+ item._1 + ", " + item._2 + ")");
//		});
		
		//assign weight to each back link
		JavaPairRDD<Integer, Double> nodeTransferRDD = edgeRDD
				.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(item -> new Tuple2<Integer, Double>(item._1, 1.0/item._2));
		//join the two RDDs together by key
		JavaPairRDD<Integer, Tuple2<Integer, Double>> edgeTransferRDD = edgeRDD.join(nodeTransferRDD);
//		edgeTransferRDD.collect().stream().forEach( item ->{
//			System.out.println(item._1 + ": (" + item._2._1 + ", " + item._2._2 + ")");
//		}
//				);
		
		double d = 0.15;
		
		int i = 1;
		int maxIters = 25;
		boolean converge = false;
		int delta = 30;
		double dround;
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
					.reduceByKey((a,b) -> Math.abs(a-b))
					.mapToPair(item -> 
						new Tuple2<Double, Integer>(item._2, item._1))
					.sortByKey(false, Config.PARTITIONS);
			dround = difference.keys().take(1).get(0);
			System.out.println("round "+i+" delta: "+ dround);
			if(dround <= delta) {
				converge = true;	
			}
			else {
				pageRankRDD = pageRankRDD2;	
			}
			
		i++;	
		}
		List<Tuple2<Integer, Double>> topTenTuple = pageRankRDD
				.mapToPair(t -> new Tuple2<Double, Integer>(t._2, t._1))
				.sortByKey(false, Config.PARTITIONS)
				.mapToPair(t -> new Tuple2<Integer, Double>(t._2, t._1))
				.take(10);
		
		List<MyPair<Integer, Double>> topTen = new ArrayList<>();
		
		topTenTuple.forEach(t ->{
			topTen.add(new MyPair<Integer, Double>(t._1, t._2));
		});
		
			
		System.out.println("*** Finished social network ranking! ***");

    return topTen;
	}

	/**
	 * Graceful shutdown
	 */
//	public void shutdown() {
//		System.out.println("Shutting down");
//	}
	
	public SocialRankJob(boolean useBacklinks, String source) {
		System.setProperty("file.encoding", "UTF-8");
		
		this.useBacklinks = useBacklinks;
		this.source = source;
	}

	@Override
	public List<MyPair<Integer,Double>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
