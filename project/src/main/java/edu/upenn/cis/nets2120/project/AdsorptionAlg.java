package edu.upenn.cis.nets2120.project;

import java.io.IOException;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import edu.upenn.cis.nets2120.storage.DynamoConnector;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;

public class AdsorptionAlg implements Job<List<String>>{
	
	private static final long serialVersionUID = 1L;
	
	// DynamoDB
	DynamoDB db;
	Table users;
	Table news;
	
	// Apache Spark
	SparkSession spark;
	JavaSparkContext context;
	
	static LocalDate currentDate;
		
	public AdsorptionAlg() {
		currentDate = LocalDate.now();
	}
	
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting...");
		
		db = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
		users = db.getTable("users");
		news = db.getTable("news");
		
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		System.out.println("Connected!");
	}
	
	@SuppressWarnings("unchecked")
	public JavaPairRDD<String, String> initializeGraph() {
		List<Tuple2<String, String>> nodes = new LinkedList<>();
		Iterator<Page<Item, ScanOutcome>> usersResult = users.scan(new ScanSpec()).pages().iterator();
		while (usersResult.hasNext()) {			
			Iterator<Item> itr = usersResult.next().iterator();
			
			while (itr.hasNext()) {
				
				
				Map<String, Object> next = itr.next().asMap();
				
				users.updateItem(new PrimaryKey("username", next.get("username")), new AttributeUpdate("recommended").delete());
				users.updateItem(new PrimaryKey("username", next.get("username")), new AttributeUpdate("graphWeights").delete());
				
				if (next.containsKey("friends")) {
					Set<String> list = (Set<String>) next.get("friends");
					if (list.size() != 0) {
						for (String s : list) {
							nodes.add(new Tuple2<>("U: " + next.get("username"), "U: " + s));
						}
					}
				}
				
				if (next.containsKey("likedArticles")) {
					Set<String> list = (Set<String>) next.get("likedArticles");
					if (list.size() != 0) {
						for (String s : list) {
							nodes.add(new Tuple2<>("U: " + next.get("username"), "A: " + s));
							nodes.add(new Tuple2<>("A: " + s, "U: " + next.get("username")));
						}
					}
				}
				
				if (next.containsKey("interest")) {
					List<String> list = (List<String>) next.get("interest");
					if (list.size() != 0) {
						for (String s : list) {
							nodes.add(new Tuple2<>("U: " + next.get("username"), "C: " + s));
							nodes.add(new Tuple2<>("C: " + s, "U: " + next.get("username")));
						}
					}
				}
			}			
		}
		
		Iterator<Page<Item, ScanOutcome>> newsResult = news.scan(new ScanSpec()).pages().iterator();
		while (newsResult.hasNext()) {
			Iterator<Item> itr = newsResult.next().iterator();
			while (itr.hasNext()) {
				Map<String, Object> next = itr.next().asMap();
				if (next.containsKey("datePosted") && 
						currentDate.compareTo(LocalDate.parse(next.get("datePosted").toString())) <= 0) {
					if (next.containsKey("category")) {
						nodes.add(new Tuple2<>("C: " + next.get("category").toString().toLowerCase(), "A: " + next.get("headline")));
						nodes.add(new Tuple2<>("A: " + next.get("headline"), "C: " + next.get("category").toString().toLowerCase()));
					}
				}
			}
		}
		
		JavaPairRDD<String, String> nodesRDD = context.parallelize(nodes).mapToPair(tuple -> {
			return new Tuple2<>(tuple._1(), tuple._2());
		});
				
		return nodesRDD;
	}
	
	JavaPairRDD<Tuple2<String, String>, Double> addEdges(JavaPairRDD<String, String> nodes) {
		JavaPairRDD<String, String> nodeCategoryArticle = nodes.filter(tuple -> {
			if (tuple._1().charAt(0) != 'U') {
				return true;
			}
			return false;
		});
		
		JavaPairRDD<Tuple2<Character, String>, String> userNodes = nodes.filter(tuple -> {
			if (tuple._1().charAt(0) == 'U') {
				return true;
			} else {
				return false;
			}
		}).mapToPair(tuple -> {
			return new Tuple2<>(new Tuple2<>(tuple._2().charAt(0), tuple._1()), tuple._2());
		});
		
		JavaPairRDD<Tuple2<String, String>, Double> weightCategoryArticle = nodeCategoryArticle.mapToPair(tuple -> {
			return new Tuple2<>(tuple._1(), 1);
		}).reduceByKey((a, b) -> a + b).mapToPair(tuple -> {
			return new Tuple2<>(tuple._1(), 1.0/tuple._2());
		}).join(nodeCategoryArticle).mapToPair(tuple -> {
			return new Tuple2<>(new Tuple2<>(tuple._1(), tuple._2._2()), tuple._2()._1());
		});
						
		JavaPairRDD<Tuple2<String, String>, Double> weightUsers = userNodes.mapToPair(tuple -> {
			return new Tuple2<>(new Tuple2<>(tuple._1()._1(), tuple._1()._2()), 1);
		}).reduceByKey((a, b) -> a + b).mapToPair(tuple -> {
			if (tuple._1()._1() == 'A') {
				return new Tuple2<>(tuple._1(), new Tuple2<>(0.4/tuple._2(), tuple._1()._1()));
			}
			return new Tuple2<>(tuple._1(), new Tuple2<>(0.3/tuple._2(), tuple._1()._1()));
		}).join(userNodes).mapToPair(tuple -> {
			return new Tuple2<>(new Tuple2<>(tuple._1()._2(), tuple._2()._2()), tuple._2()._1()._1());
		});
				
		return weightCategoryArticle.union(weightUsers);
	}
	
	public void run() {
		System.out.println("Running!");
		JavaPairRDD<String, String> nodes = initializeGraph();
		
		// start, (end, edgeWeight)
		JavaPairRDD<String, Tuple2<String, Double>> weightedGraph = addEdges(nodes).mapToPair(tuple -> {
			return new Tuple2<>(tuple._1()._1(), new Tuple2<>(tuple._1()._2(), tuple._2()));
		});
				
		// user, (currNode, weight)
		JavaPairRDD<Tuple2<String, String>, Double> pageRank = weightedGraph.mapToPair(tuple -> {
			if (tuple._1().charAt(0) == 'U') {
				return new Tuple2<>(new Tuple2<>(tuple._1(), tuple._2()._1()), tuple._2()._2());
			}
			
			return new Tuple2<>(new Tuple2<>(tuple._1(), tuple._2()._1()), 0.0);
		});
				
		boolean checkEnd = false;
		
		for (int i = 0; i < 15 && !checkEnd; i++) {
			System.out.println(i + " loop");
			JavaPairRDD<String, Tuple2<String, Double>> propagateRDD = pageRank.mapToPair(tuple -> {
				return new Tuple2<>(tuple._1()._2(), new Tuple2<>(tuple._1()._1(), tuple._2()));
			}).join(weightedGraph).mapToPair(tuple -> {	
				return new Tuple2<>(tuple._2()._1()._1(), new Tuple2<>(tuple._2()._2()._1(), tuple._2()._1()._2() * tuple._2()._2()._2()));
			});
			
			System.out.println("propagated");
						
			JavaPairRDD<String, Double> combineRDD = propagateRDD.mapToPair(tuple -> {
				return new Tuple2<>(tuple._2()._1(), tuple._2()._2());
			}).reduceByKey((a, b) -> {
				return a + b;
			});
			
			System.out.println("combined");
			
			JavaPairRDD<Tuple2<String, String>, Double> normalizeRDD = propagateRDD.mapToPair(tuple -> {
				return new Tuple2<>(tuple._2()._1(), new Tuple2<>(tuple._1(), tuple._2()._2()));
			}).join(combineRDD).mapToPair(tuple -> {
				if (tuple._2()._2() != 0) {
					return new Tuple2<>(new Tuple2<>(tuple._2()._1()._1(), tuple._1()), tuple._2()._1()._2()/tuple._2()._2());
				} else {
					return new Tuple2<>(new Tuple2<>(tuple._2()._1()._1(), tuple._1()), 0.0);
				}
			});
			
			System.out.println("normalized");
			
			Double d = normalizeRDD.join(pageRank).map(tuple -> {
				return Math.abs(tuple._2()._1() - tuple._2()._2());			
			}).reduce((a, b) -> {
				if (a > b) {
					return a;
				} else {
					return b;
				}
			});
			
			System.out.println(d);
					
			checkEnd = d <= .001;
			
			pageRank = normalizeRDD.cogroup(pageRank).mapToPair(tuple -> {
				if (!tuple._2()._1().iterator().hasNext()) {
					return new Tuple2<>(tuple._1(), tuple._2()._2().iterator().next());
				} else {
					return new Tuple2<>(tuple._1(), tuple._2()._1().iterator().next());
				}				
			});
		}
		
		System.out.println("exited loop!");
				
		JavaPairRDD<String, Tuple2<String, Double>> userNews = pageRank.filter(tuple -> {
			return tuple._1()._1().charAt(0) == 'U' && tuple._1()._2().charAt(0) == 'A';
		}).mapToPair(tuple -> {
			return new Tuple2<>(tuple._2(), tuple._1());
		}).sortByKey(true).mapToPair(tuple -> {
			return new Tuple2<>(tuple._2()._1().substring(3), new Tuple2<>(tuple._2()._2().substring(3), tuple._1()));
		});
		
		System.out.println("sorted");
		
		userNews.groupByKey().mapToPair(tuple -> {
			List<Tuple2<String, Double>> list = new LinkedList<>();
			Iterator<Tuple2<String, Double>> itr = tuple._2().iterator();
			
			while (itr.hasNext()) {
				if (list.size() < 10) {
					list.add(itr.next());
				} else {
					Tuple2<String, Double> next = itr.next();
					for (int i = 0; i < 10; i++) {
						if (list.get(i)._2() < next._2()) {
							list.remove(i);
							list.add(next);
							break;
						}
					}
				}
			}
			
			return new Tuple2<>(tuple._1(), list);
		}).foreachPartition(iter -> {
			DynamoDB tempDB = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
			Table tempTable = tempDB.getTable("users");

			while (iter.hasNext()) {
				Tuple2<String, List<Tuple2<String, Double>>> next = iter.next();
				for (int i = 0; i < next._2().size(); i++) {
					Thread.sleep(2500);
					AttributeUpdate attributeUpdates = new AttributeUpdate("recommended")
							.addElements(next._2().get(i)._1());
					if (attributeUpdates != null) {
						tempTable.updateItem(new PrimaryKey("username", next._1()), attributeUpdates);
					}
				}
			}
		});
		
		System.out.println("10 added!");
				
		userNews.foreachPartition(itr -> {
			DynamoDB tempDB = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
			Table tempTable = tempDB.getTable("users");
			
			while (itr.hasNext()) {
				Thread.sleep(5000);
				Tuple2<String, Tuple2<String, Double>> next = itr.next();
				AttributeUpdate attributeUpdates = new AttributeUpdate("graphWeights")
						.addElements(next._2()._1() + ":" + next._2()._2());
				if (attributeUpdates != null) {
					tempTable.updateItem(new PrimaryKey("username", next._1()), attributeUpdates);
				}
			}
		});
		
		System.out.println("updated + done!");
	}
	
	public void shutdown() {
		System.out.println("Shutting down");
		
		DynamoConnector.shutdown();
		
		if (spark != null)
			spark.close();
	}
	
	public static void main(String[] args) {
		AdsorptionAlg aa = new AdsorptionAlg();
		try {
			aa.initialize();
			aa.run();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			aa.shutdown();
		}
	}

	@Override
	public List<String> call(JobContext arg0) throws Exception {
		initialize();
		run();
		
		return null;
	}
}
