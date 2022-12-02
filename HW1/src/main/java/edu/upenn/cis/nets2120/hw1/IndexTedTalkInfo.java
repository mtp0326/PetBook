package edu.upenn.cis.nets2120.hw1;

import java.util.*;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

import edu.upenn.cis.nets2120.hw1.files.TedTalkParser.TalkDescriptionHandler;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

/**
 * Callback handler for talk descriptions.  Parses, breaks words up, and
 * puts them into DynamoDB.
 * 
 * @author zives
 *
 */
public class IndexTedTalkInfo implements TalkDescriptionHandler {
	static Logger logger = LogManager.getLogger(TalkDescriptionHandler.class);

  final static String tableName = "inverted";
	int row = 0;
	
	SimpleTokenizer model;
	Stemmer stemmer;
	DynamoDB db;
	Table iindex;
	
	public IndexTedTalkInfo(final DynamoDB db) throws DynamoDbException, InterruptedException {
		model = SimpleTokenizer.INSTANCE;
		stemmer = new PorterStemmer();
		this.db = db;
		
		
		
		initializeTables();
	}

	/**
	 * Called every time a line is read from the input file. Breaks into keywords
	 * and indexes them.
	 * 
	 * @param csvRow      Row from the CSV file
	 * @param columnNames Parallel array with the names of the table's columns
	 */
	@Override
	public void accept(final String[] csvRow, final String[] columnNames) {
		int [] index = new int [9];
		int counter = 0;
		String keyword = "";
		String url = "";
		int talk_id;
		Collection<Item> finalSet = new HashSet<>();
		Collection<Item> tempSet = new HashSet<>();
		TableWriteItems table = new TableWriteItems(tableName);
		talk_id = Integer.parseInt(lookup(csvRow, columnNames, "talk_id"));
		url = lookup(csvRow, columnNames, "url");
		//find the indexes of the column names we want
		for(int i = 0 ; i<columnNames.length; i++) {
			
			if(columnNames[i] == "title" || columnNames[i] == "speaker_1" || columnNames[i] == "all_speakers"
					|| columnNames[i] == "occupations" ||columnNames[i] == "about_speakers" ||columnNames[i] == "topics" ||
					columnNames[i] == "description" ||columnNames[i] == "transcript" ||columnNames[i] == "related_talks") {
				index[counter] = i;
				counter++;
			}
		}
		//tokenize the row and check for invalid strings. If it's invalid, set it to null
		for(int i = 0; i < index.length; i++) {
			String lineRead[] = model.tokenize(csvRow[i]);
			for(int j = 0; j < lineRead.length; j++) {
					int stringCount=0;
					boolean found = false;
					while(!found && stringCount<lineRead[j].length()) {
						if((lineRead[j].charAt(stringCount)>='A'&& lineRead[j].charAt(stringCount)<='Z') || 
								(lineRead[j].charAt(stringCount)>='a'&& lineRead[j].charAt(stringCount)<='z')) {
							stringCount++;
						}
						else {
							found = true;
							lineRead[j] = null;
						}
						
					
				}
					if(lineRead[j]!= null ) {
						lineRead[j] = lineRead[j].toLowerCase();
						lineRead[j] = stemmer.stem(lineRead[j]).toString();
						if(lineRead[j] == "a" || lineRead[j] == "all" || lineRead[j] == "any" || lineRead[j] == "but" || lineRead[j] == "the") {
							lineRead[j] = null;
						}
					}
					//add the item into the set
					if(lineRead[j]!= null ) {
						Item item = new Item().withPrimaryKey("keyword", lineRead[j], "inxid", talk_id)
								.withString("url",url); 
						//check if it already exists in the finalset, if not, add it into the temp set
						if(finalSet.add(item)) {
							tempSet.add(item);
						}
					}
					//if the tempset its the size of 25, clear it and use a new tempset
					if(tempSet.size()>=25) {
						table.withItemsToPut(tempSet);
						BatchWriteItemOutcome result = db.batchWriteItem(table);
						while(result.getUnprocessedItems().size() > 0) {
							result = db.batchWriteItemUnprocessed(result.getUnprocessedItems());
						}
						tempSet.clear();
					}
			}
			
			
		}
		if(tempSet.size()!=0) {
			BatchWriteItemOutcome result;
			table.withItemsToPut(tempSet);
			result = db.batchWriteItem(table);
			while(result.getUnprocessedItems().size() > 0) {
				result = db.batchWriteItemUnprocessed(result.getUnprocessedItems());
			}
			tempSet.clear();
			finalSet.clear();
		}
	}
	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			iindex = db.createTable(tableName, Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH), // Partition
																												// key
					new KeySchemaElement("inxid", KeyType.RANGE)), // Sort key
					Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S),
							new AttributeDefinition("inxid", ScalarAttributeType.N)),
					new ProvisionedThroughput(100L, 100L));

			iindex.waitForActive();
		} catch (final ResourceInUseException exists) {
			iindex = db.getTable(tableName);
		}

	}

	/**
	 * Given the CSV row and the column names, return the column with a specified
	 * name
	 * 
	 * @param csvRow
	 * @param columnNames
	 * @param columnName
	 * @return
	 */
	public static String lookup(final String[] csvRow, final String[] columnNames, final String columnName) {
		final int inx = Arrays.asList(columnNames).indexOf(columnName);
		
		if (inx < 0)
			throw new RuntimeException("Out of bounds");
		
		return csvRow[inx];
	}
}
