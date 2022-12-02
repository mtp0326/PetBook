	
	

package edu.upenn.cis.nets2120.hw1.tests;

import static org.junit.Assert.assertEquals;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import edu.upenn.cis.nets2120.config.Config;

public class TestDynamoDb {
	@Before
	public void setup() throws URISyntaxException {
		System.setProperty("sqlite4java.library.path", "native-libs");
		
		// You may want to remove these two lines if you want to test
		// connected to the real server
		Config.LOCAL_DB = true;
		Config.DYNAMODB_URL = "http://localhost:8000";
	}

	String[] arr = {"-inMemory", "-port", "8000"};
	
	/**
	 * Test that we can launch the local Dynamo and then connect 
	 * @throws Exception 
	 */
	@Test
	public void testLocalDynamoFromClient() throws Exception {
		DynamoDBProxyServer server = ServerRunner.createServerFromCommandLineArgs(arr);
		server.start();
		
		System.setProperty("aws.accessKeyId", "dummy");
		System.setProperty("aws.secretKey", "dumm-value");

        try {
        	DynamoDB client = new DynamoDB( 
        			AmazonDynamoDBClientBuilder.standard()
        			.withEndpointConfiguration(
        					new AwsClientBuilder.EndpointConfiguration(Config.DYNAMODB_URL, "us-east-1"))
        			.withCredentials(new SystemPropertiesCredentialsProvider())
        			.build());

        	TableCollection<ListTablesResult> tables = client.listTables(); 
        	System.out.println("Tables");
        	IteratorSupport<Table,ListTablesResult> it = tables.iterator();
        	while (it.hasNext())
        		System.out.println(it.next().toString());
        	client.shutdown();
        } finally {
        	if (server != null)
				try {
					server.stop();
				} catch (Exception e) {
					e.printStackTrace();
				}
        }
	}

	/**
	 * We are using the basic test cases from AWS DynamoDB local
	 */
	
    @Test
    public void createTableTest() {
        AmazonDynamoDB ddb = DynamoDBEmbedded.create().amazonDynamoDB();
        try {
            String tableName = "Movies";
            String hashKeyName = "film_id";
            CreateTableResult res = createTable(ddb, tableName, hashKeyName);

            TableDescription tableDesc = res.getTableDescription();
            assertEquals(tableName, tableDesc.getTableName());
            assertEquals("[{AttributeName: " + hashKeyName + ",KeyType: HASH}]", tableDesc.getKeySchema().toString());
            assertEquals("[{AttributeName: " + hashKeyName + ",AttributeType: S}]",
                tableDesc.getAttributeDefinitions().toString());
            assertEquals(Long.valueOf(1000L), tableDesc.getProvisionedThroughput().getReadCapacityUnits());
            assertEquals(Long.valueOf(1000L), tableDesc.getProvisionedThroughput().getWriteCapacityUnits());
            assertEquals("ACTIVE", tableDesc.getTableStatus());
            assertEquals("arn:aws:dynamodb:ddblocal:000000000000:table/Movies", tableDesc.getTableArn());

            ListTablesResult tables = ddb.listTables();
            assertEquals(1, tables.getTableNames().size());
        } finally {
            ddb.shutdown();
        }
    }

    private static CreateTableResult createTable(AmazonDynamoDB ddb, String tableName, String hashKeyName) {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition(hashKeyName, ScalarAttributeType.S));

        List<KeySchemaElement> ks = new ArrayList<KeySchemaElement>();
        ks.add(new KeySchemaElement(hashKeyName, KeyType.HASH));

        ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput(1000L, 1000L);

        CreateTableRequest request =
            new CreateTableRequest()
                .withTableName(tableName)
                .withAttributeDefinitions(attributeDefinitions)
                .withKeySchema(ks)
                .withProvisionedThroughput(provisionedthroughput);

        return ddb.createTable(request);
    }
}

