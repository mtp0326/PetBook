package edu.upenn.cis.nets2120.hw1;

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
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

    return null;
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
