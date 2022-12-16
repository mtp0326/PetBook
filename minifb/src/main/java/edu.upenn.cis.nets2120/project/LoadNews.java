package edu.upenn.cis.nets2120.project;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.mortbay.util.ajax.JSON;

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

import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import edu.upenn.cis.nets2120.storage.SparkConnector;

public class LoadNews {
	
	// DynamoDB
	DynamoDB db;
	Table news;
	Table newsKeyword;
	
	// Apache Spark
	SparkSession spark;
	JavaSparkContext context;
	
	JavaRDD<Row> newsArticles;
	
	public LoadNews() {
		
	}
	
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			news = db.createTable("news", Arrays.asList(new KeySchemaElement("headline", KeyType.HASH)),					
					Arrays.asList(new AttributeDefinition("headline", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); 
			news.waitForActive();
		} catch (final ResourceInUseException exists) {
			news = db.getTable("news");
		}
		
		try {
			newsKeyword = db.createTable("newsKeyword", Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH)),					
					Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S)),
					new ProvisionedThroughput(25L, 25L)); 
			newsKeyword.waitForActive();
		} catch (final ResourceInUseException exists) {
			newsKeyword = db.getTable("newsKeyword");
		}

	}
	
	public void initialize() throws IOException, DynamoDbException, InterruptedException {
		System.out.println("Connecting...");
		db = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
		
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		initializeTables();
		
		System.out.println("Connected!");
	}
	
	@SuppressWarnings("unchecked")
	JavaRDD<Row> getNews(String filename) {
				
		BufferedReader br;
		List<String[]> lines = new LinkedList<>();

		try {
			br = new BufferedReader(new InputStreamReader(new URL(Config.NEWS_PATH).openStream()));

			String nextLine = br.readLine();
			Map<String, String> map;
			String[] linesArr = null;
			
			while (nextLine != null) {
				map = (Map<String, String>) JSON.parse(nextLine);
				
				linesArr = new String[6];
				linesArr[0] = map.get("category");
				linesArr[1] = map.get("headline");
				linesArr[2] = map.get("authors");
				linesArr[3] = map.get("link");
				linesArr[4] = map.get("short_description");
				linesArr[5] = map.get("date");
				linesArr[5] = (Integer.parseInt(linesArr[5].substring(0, 4))+5)+linesArr[5].substring(4);
				if (linesArr[5].substring(5).equals("02-29") && Integer.parseInt(linesArr[5].substring(0, 4)) % 4 != 0) {
					System.out.println(linesArr[5]);
					linesArr[5] = linesArr[5].substring(0, 8) + "28";
					System.out.println("new date!" + linesArr[5]);
				}
				
				if (!linesArr[1].equals("")) {
					lines.add(linesArr);
				}
				
				nextLine = br.readLine();
			}
			
			br.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
				
		StructType schema = new StructType()
				.add("category", "string")
				.add("headline", "string")
				.add("authors", "string")
				.add("link", "string")
				.add("short_description", "string")
				.add("datePosted", "string");
		
		List<Row> listOfLines = lines.parallelStream()
				.map(line -> {
					return new GenericRowWithSchema(line, schema);
				}).collect(Collectors.toList());
		JavaRDD<Row> linesRDD = context.parallelize(listOfLines).distinct();
		
		return linesRDD;
	}
	
	void loadKeyword(Row newsArticle) {
		SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		Stemmer stemmer = new PorterStemmer();
				
		Map<String, String> newsContents = new HashMap<>();
		newsContents.put("headline", newsArticle.getAs("headline"));
		newsContents.put("authors", newsArticle.getAs("authors"));
		newsContents.put("link", newsArticle.getAs("link"));
		newsContents.put("short_description", newsArticle.getAs("short_description"));
		newsContents.put("datePosted", newsArticle.getAs("date"));
		newsContents.put("category", newsArticle.getAs("category"));
		Set<String> keySet = newsContents.keySet();
		
		boolean accept;
		Set<Item> set = new HashSet<>();
		Set<Item> smallSet = new HashSet<>();
		TableWriteItems twi = new TableWriteItems("newsKeyword");
		
		for (String key : keySet) {
			if (!key.equals("link")) {
				String[] arr = tokenizer.tokenize(newsContents.get(key));
				for (String s : arr) {
					accept = true;
					
					// check if all values of the word are a-z and A-Z
					for (int j = 0; j < s.length(); j++) {
						if (accept && (s.charAt(j) >= 'a' && s.charAt(j) <= 'z' || 
								s.charAt(j) >= 'A' && s.charAt(j) <= 'Z')) {
							s = s.toLowerCase();
						} else {
							accept = false; // if not, set accept to false
						}
					}
					
					// check for stop words
					if (s.equals("a") || s.equals("all") || s.equals("any") ||  s.equals("but") || s.equals("the")) {
						accept = false;
					} 
					
					// stem the word
					s = stemmer.stem(s).toString();
					
					// if word is not rejected, do the following
					if (accept) {
						// create new item with all items
						Item item;
						item = new Item()
								.withPrimaryKey("keyword", s)
								.withString("link", newsContents.get("link"))
								.withString("headline", newsContents.get("headline"))
								.withString("datePosted", newsContents.get("datePosted"));
						
						// add the item to the set, if the item is not added, it will not be added to smallSet
						if (set.add(item)) {
							smallSet.add(item);
						}
					}
					
					// smallSet has size 25 since batchwrite can only write 25 entries at a time
					if (smallSet.size() == 20) {
	  					twi.withItemsToPut(smallSet);
						BatchWriteItemOutcome outcome = db.batchWriteItem(twi);
						while (outcome.getUnprocessedItems().size() != 0) {
							outcome = db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
						}
						
						// after batchwrite is finished, clear smallSet and start again
						smallSet.clear();
					}
				}			
			}
			
			// add remaining items in smallSet to the table
			if (smallSet.size() != 0) {
				twi.withItemsToPut(smallSet);
				BatchWriteItemOutcome outcome = db.batchWriteItem(twi);
				while (outcome.getUnprocessedItems().size() != 0) {
					outcome = db.batchWriteItemUnprocessed(outcome.getUnprocessedItems());
				}
			}
			
			smallSet.clear();
			set.clear();
		}
	}
	
	public void run() {
		JavaRDD<Row> news = getNews(Config.NEWS_PATH);
		Set<String> headlines = new HashSet<>();
		Set<String> descriptions = new HashSet<>();
		news.foreachPartition(iter -> {
			DynamoDB tempDB = DynamoConnector.getConnection("https://dynamodb.us-east-1.amazonaws.com");
			Table tempTable = tempDB.getTable("news");
			
			Set<Item> set = new HashSet<>();
			TableWriteItems twi = new TableWriteItems("news");
			while (iter.hasNext()) {
				Row r = iter.next();
				if (set.size() == 20) {
					Thread.sleep(3000);
					System.out.println("added");
					twi.withItemsToPut(set);
					BatchWriteItemOutcome outcome = tempDB.batchWriteItem(twi);
					while (outcome.getUnprocessedItems().size() != 0) {
						outcome = tempDB.batchWriteItem(twi);
					}
					set.clear();
				}
				
				Item i = new Item()
						.withPrimaryKey("headline", r.getAs("headline"))
						.withString("category", r.getAs("category"))
						.withString("authors", r.getAs("authors"))
						.withString("link", r.getAs("link"))
						.withString("short_description", r.getAs("short_description"))
						.withString("datePosted", r.getAs("datePosted"));
				
				if (headlines.add(r.getAs("headline")) && descriptions.add(r.getAs("short_description"))) {
					set.add(i); 
				}
			}

			if (set.size() != 0 && !iter.hasNext()) {
				twi.withItemsToPut(set);
				BatchWriteItemOutcome outcome = tempDB.batchWriteItem(twi);
				while (outcome.getUnprocessedItems().size() != 0) {
					outcome = tempDB.batchWriteItem(twi);
				}				
			}
			
			set.clear();
		});
		
		Iterator<Row> itr = news.collect().iterator();
		while (itr.hasNext()) {
			loadKeyword(itr.next());
		}
	}
	
	public void shutdown() {
		System.out.println("Shutting down");
		
		DynamoConnector.shutdown();
		
		if (spark != null) {
			spark.close();
		}
	}
	
	public static void main(String[] args) {
		final LoadNews ln = new LoadNews();

		try {
			ln.initialize();
			ln.run();
		} catch (final IOException ie) {
			System.out.println("I/O error: ");
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