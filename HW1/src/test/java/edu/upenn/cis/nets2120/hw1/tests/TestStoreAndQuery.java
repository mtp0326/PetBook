package edu.upenn.cis.nets2120.hw1.tests;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.hw1.LoadData;
import edu.upenn.cis.nets2120.hw1.QueryForWord;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;


public class TestStoreAndQuery {
	static LoadData ld = null;
	static QueryForWord qw = null;
	
	final static String[] qstring = {"abacadabra"};
	final static String dummyUrl = "http://my.site";

	@Before
	public void setup() throws DynamoDbException, IOException, InterruptedException {
		Config.LOCAL_DB = true;
		Config.DYNAMODB_URL = "http://localhost:8000";
		
		if (ld == null) {
			ld = new LoadData();
			ld.initialize();
		}
		
		if (qw == null) {
			qw = new QueryForWord();
			qw.initialize();
		}
	}

	@Test
	public void testStoreAndQuery() throws DynamoDbException, IOException, InterruptedException {
		indexThisLine(qstring[0]);
		
		Set<Set<String>> results = qw.query(qstring);
		
		System.out.println(results);
	
		// One keyword match
		assertTrue(results.size() == 1);
		
		// One result in the keyword match
		assertTrue(results.iterator().next().size() == 1);

		String match = results.iterator().next().iterator().next();
		
		System.out.println(match);
		// And it's "my.site"
		assertTrue(match.contains(dummyUrl));
	}

	/**
	 * Create a dummy CSV line to index our results
	 * @param str
	 */
	static void indexThisLine(String str) {
		String[] csvRow = {
				str,
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"99999",
				dummyUrl
		};
		String[] columnNames = {
				"title",
				"speaker_1",
				"all_speakers",
				"occupations",
				"about_speakers",
				"topics",
				"description",
				"transcript",
				"related_talks",
				"talk_id",
				"url"
		};
		ld.indexThisLine(csvRow, columnNames);
	}
	
	
	@After
	public void shutdown() {
		if (ld != null) {
			ld.shutdown();
			ld = null;
		}
		if (qw != null) {
			qw.shutdown();
			qw = null;
		}
	}
}
