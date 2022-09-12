package edu.upenn.cis.nets2120.hw1.files;

import java.io.IOException;
import java.io.Reader;
import java.util.function.BiConsumer;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

//import edu.upenn.cis.nets2120.config.Config;

public class TedTalkParser {
	
	public static interface TalkDescriptionHandler extends BiConsumer<String[], String[]> {
		
	};
	
	CSVReader reader;
	String[] headerLine;
	
	/**
	 * Initialize a reader for the CSV file
	 * 
	 * @param reader
	 * @throws IOException
	 */
	public TedTalkParser(Reader reader) throws IOException {
		System.setProperty("file.encoding", "UTF-8");
		this.reader = new CSVReader(reader); 
		
		try {
			headerLine = this.reader.readNext();
			
			
		} catch (CsvValidationException e) {
			// This should never happen but Java thinks it could
			e.printStackTrace();
		}
	}
	
	/**
	 * Read talks, one at a time, from the input file.  Call
	 * the processTalk handler if the line is OK, or processError if
	 * the line isn't parseable.
	 * 
	 * @param processTalk Function that takes an array of info about the talk, plus
	 * a (parallel) array of column names.
	 * 
	 * @throws IOException I/O error reading the file.
	 */
	public void readTalkDescriptions(BiConsumer<String[], String[]> processTalk) throws IOException {
		String [] nextLine;
		try {
//			int rowsRead = 0;
			while ((nextLine = reader.readNext()) != null) {
				// nextLine[] is an array of values from the line
				// headerLine is the array of column names
				processTalk.accept(nextLine, headerLine);
				
				// If you are running locally, you can use this line to short circuit
				// because performance is otherwise awful.
//				if (Config.LOCAL_DB && rowsRead++ > 10)
//					break;
			}
		} catch (CsvValidationException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Close the reader
	 */
	public void shutdown() {
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
