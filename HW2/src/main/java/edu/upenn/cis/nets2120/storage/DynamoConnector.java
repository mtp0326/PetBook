package edu.upenn.cis.nets2120.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;

import edu.upenn.cis.nets2120.config.Config;

/**
 * A factory 
 * @author zives
 *
 */
public class DynamoConnector {
	/**
	 * A logger is useful for writing different types of messages
	 * that can help with debugging and monitoring activity.  You create
	 * it and give it the associated class as a parameter -- so in the
	 * config file one can adjust what messages are sent for this class. 
	 */
	static Logger logger = LogManager.getLogger(DynamoConnector.class);
	
	/**
	 * This inner class is responsible for setting up a local copy of
	 * DynamoDB.  We don't really want the average developer to use it
	 * so we'll make it a private inner class, only used by the factory
	 * as need be.
	 *  
	 * @author zives
	 *
	 */
	private static class LocalServer {
		DynamoDBProxyServer localServer = null;
		String[] arr = {"-port", "8000"};
		
		public LocalServer() {
			System.setProperty("sqlite4java.library.path", "native-libs");

			try {
				localServer = ServerRunner.createServerFromCommandLineArgs(arr);
				localServer.start();
			} catch (final Exception e) {
				System.err.println("Unable to initialize local server");
			}
		}

		/**
		 * We can do an orderly shutdown
		 */
		void shutdown() {
			try {
				localServer.stop();
				localServer = null;
			} catch (final Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * If our object is being deleted, we should shut down
		 */
		protected void finalize() {
			if (localServer != null)
				try {
					localServer.stop();
					System.out.println("Shut down local server");
				} catch (final Exception e) {
					e.printStackTrace();
				}
		}
	}

	/**
	 * In case we need to run a DynamoDB Local server, here's an object
	 */
	static LocalServer local = null;

	/**
	 * This is our connection
	 */
	static DynamoDB client;

	/**
	 * Singleton pattern: get the client connection if one exists, else create one
	 * 
	 * @param url
	 * @return
	 */
	public static DynamoDB getConnection(final String url) {
		if (client != null)
			return client;
		
		// Are we running the local "dummy" client?  If so we put in fake AWS credentials and a fake region.
		if (Config.LOCAL_DB) {
			local = new LocalServer();
			System.setProperty("aws.accessKeyId", "dummy");
			System.setProperty("aws.secretKey", "dumm-value");
			
	    	client = new DynamoDB( 
	    			AmazonDynamoDBClientBuilder.standard()
	    			.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
						Config.DYNAMODB_URL, "us-east-1"))
        			.withCredentials(new SystemPropertiesCredentialsProvider())
					.build());
					
		// This is a real connection.  We're constrained to the us-east-1 region for Amazon AWS Educate Classrooms.
		} else {
	    	client = new DynamoDB( 
	    			AmazonDynamoDBClientBuilder.standard()
					.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
						Config.DYNAMODB_URL, "us-east-1"))
        			.withCredentials(new DefaultAWSCredentialsProviderChain())
					.build());
		}

    	return client;
	}
	
	/**
	 * Orderly shutdown
	 */
	public static void shutdown() {
		if (client != null) {
			client.shutdown();
			client = null;
		}
		if (local != null) {
			local.shutdown();
			local = null;
		}
		logger.info("Shut down DynamoDB factory");
	}
}
