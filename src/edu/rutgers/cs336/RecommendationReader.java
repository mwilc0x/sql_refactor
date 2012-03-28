package edu.rutgers.cs336;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Used to read the RECS_FILE containing recommendations
 *
 */
public class RecommendationReader {
	private static final String RECS_FILE = EnvManager.getStringVariable("RECS_FILE");
	
	/**
	 * Reads the RECS_FILE file line by line
	 * Creates a new recommendation object (which populates the domain and return fields) for each line
	 *   That recommendation is then inserted into the database
	 */
	public void readFile() {
		BufferedReader bufReader = null;
		try{
			bufReader = new BufferedReader(new FileReader(RECS_FILE));
			bufReader.readLine(); //skip header line
			String line;
			while((line = bufReader.readLine()) != null) {
				//extract fields from each line of the RECS_FILE
				Pattern pattern = Pattern.compile( "\"([^\"]+)\",\"([^\"]+)\",\"([^\"]+)\",\"([^\"]+)\"");
				Matcher matcher = pattern.matcher(line); 
				if(!matcher.matches()) {
					System.err.println("Unexpected line in "+RECS_FILE+": \""+line+"\"");
					continue;
				}
				String stockSymbol = matcher.group(1);
				String recDateStr = matcher.group(2);
				String direction = matcher.group(3);
				String completeUrl = matcher.group(4);
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
				Date recDate = null;
				try {
					recDate = simpleDateFormat.parse(recDateStr);
				} catch (ParseException e) {
					System.err.println("Unable to parse date in line of "+RECS_FILE+": "+line);
					continue;
				}
				
				//create recommendation object to populate required fields
				//  and insert it into the database
				Recommendation rec = new Recommendation(stockSymbol, recDate, direction, completeUrl);
				rec.insertToDb();
			}
		} catch (IOException e) {
			System.err.println("Unable to read "+RECS_FILE);
			e.printStackTrace();
		} finally {
			if(bufReader != null) {
				try{
					bufReader.close();
				} catch (IOException e) {
				}
			}
		}
		
	}
	
}
