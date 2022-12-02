package edu.upenn.cis.nets2120.hw1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class QueryForWord {
	/**
	 * A logger is useful for writing different types of messages
	 * that can help with debugging and monitoring activity.  You create
	 * it and give it the associated class as a parameter -- so in the
	 * config file one can adjust what messages are sent for this class. 
	 */
	static Logger logger = LogManager.getLogger(QueryForWord.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	
	/**
	 * Inverted index
	 */
	Table iindex;
	
	Stemmer stemmer;

	/**
	 * Default loader path
	 */
	public QueryForWord() {
		stemmer = new PorterStemmer();
	}
	
	/**
	 * Initialize the database connection
	 * 
	 * @throws IOException
	 */
	public void initialize() throws IOException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);
		logger.debug("Connected!");
		iindex = db.getTable("inverted");
	}
	
	public Set<Set<String>> query(final String[] words) throws IOException, DynamoDbException, InterruptedException {
		// TODO implement query() in QueryForWord
		Set<Set<String>> finalTable = new HashSet<>();
		for(int j = 0; j < words.length; j++) {
			int stringCount=0;
			boolean found = false;
			//check for invalid strings
			while(!found && stringCount<words[j].length())  {
				
				if(words[j]!= null ) {
					words[j] = words[j].toLowerCase();
					words[j] = stemmer.stem(words[j]).toString();
					System.out.println(words[j]);
				}
				
				if((words[j].charAt(stringCount)>='A'&& words[j].charAt(stringCount)<='Z') || 
						(words[j].charAt(stringCount)>='a'&& words[j].charAt(stringCount)<='z')) {
					stringCount++;
				}
				else {
					found = true;
					words[j] = null;
				}
			
		}
			
			
			if(words[j].equals("a") || words[j].equals("all") || words[j].equals("any") || words[j].equals("but") || words[j].equals("the")) {
				words[j] = null;
			}
			//find the urls for each string
			if(words[j]!=null) {
				ItemCollection<QueryOutcome> items; 
				Iterator<Item> iter;
				Item i = null;
				Set<String> urlForString = new HashSet<>();
				KeyAttribute key = new KeyAttribute("keyword", words[j]);
				items = iindex.query(key);
				iter = items.iterator();
				while(iter.hasNext()) {
					i = iter.next();
					urlForString.add(i.getString("url"));
				}
				finalTable.add(urlForString);
				
				
			}
			
		}
    return finalTable;
	}

	/**
	 * Graceful shutdown of the DynamoDB connection
	 */
	public void shutdown() {
		logger.info("Shutting down");
		DynamoConnector.shutdown();
	}

	public static void main(final String[] args) {
		final QueryForWord qw = new QueryForWord();

		try {
			qw.initialize();

			final Set<Set<String>> results = qw.query(args);
			for (Set<String> s : results) {
				System.out.println("=== Set");
				for (String url : s)
				  System.out.println(" * " + url);
			}
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			qw.shutdown();
		}
	}

}
