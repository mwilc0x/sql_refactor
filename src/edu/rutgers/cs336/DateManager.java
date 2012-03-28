package edu.rutgers.cs336;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is used to determine business days (which are not Saturday, Sunday or a Holiday)
 * It makes use of the HOLIDAY_FILE
 */
public class DateManager {
	
	private static LinkedList<Date> mHolidays = new LinkedList<Date>(); 
	private static final String HOLIDAY_FILE = EnvManager.getStringVariable("HOLIDAY_FILE");
	
	static {
		//initialize the DateManager by reading in the HOLIDAY_FILE
		//  and caching the dates 
		try{
			FileReader fileReader = new FileReader(HOLIDAY_FILE);
			BufferedReader bufReader = new BufferedReader(fileReader);
			String line;
			while((line = bufReader.readLine()) != null) {
				Pattern pattern = Pattern.compile( "\"([^\"]+)\",\"[^\"]+\"");
				Matcher matcher = pattern.matcher(line); 
				if(!matcher.matches()) {
					System.err.println("Unexpected line in "+HOLIDAY_FILE+": \""+line+"\"");
					continue;
				}
				String dateStr = matcher.group(1);
				//change English spoken dates to numbers
				dateStr = dateStr.replace("0th", "0");
				dateStr = dateStr.replace("1st", "1");
				dateStr = dateStr.replace("2nd", "2");
				dateStr = dateStr.replace("3rd", "3");
				dateStr = dateStr.replace("4th", "4");
				dateStr = dateStr.replace("5th", "5");
				dateStr = dateStr.replace("6th", "6");
				dateStr = dateStr.replace("7th", "7");
				dateStr = dateStr.replace("8th", "8");
				dateStr = dateStr.replace("9th", "9");
				//parse the date
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("E, MMM dd, yyyy");
				Date nextDate = null;
				try {
					nextDate = simpleDateFormat.parse(dateStr);
				} catch (ParseException e) {
					System.err.println("Unable to parse date in line of "+HOLIDAY_FILE+": "+line);
					continue;
				}
				mHolidays.add(nextDate);
			}
		} catch (IOException e) {
			System.err.println("Unable to read "+HOLIDAY_FILE);
			e.printStackTrace();
		}
	}
	
	private DateManager() {
		//hide constructor, so no instances
	}
	
	/**
	 * @param date - the date from which we would like to find the business date 'offset' days away
	 * @param offset - the number of days from 'date' from which we would 
	 *                 like to find the business dates  
	 *                 offset can be positive or negative
	 * @return the business date 'offset' days from 'date'  
	 *         (there will be 'offset' business days between 'date' and the return value) 
	 */
	public static Date getTradeDateOffestFromDate(Date date, final int offset) {
		int increment = offset < 0 ? -1 : 1;
		
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.setTime(date);
		for(int daysLeft = offset; daysLeft != 0; daysLeft -= increment) {
			do {
				calendar.add(Calendar.DATE, increment);
				
			} while(   calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
				    || calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY
				    || mHolidays.contains(calendar.getTime()));
		}
		
		return calendar.getTime();
	}

	/**
	 * @param date - a Date
	 * @return true if the date is a trading date, 
	 *         false otherwise (i.e. Saturday, Sunday, Holiday) 
	 */
	public static boolean isBusinessDay(Date date) {
		Calendar calendar = GregorianCalendar.getInstance();
		calendar.setTime(date);
		return !(   calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
			     || calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY
			     || mHolidays.contains(calendar.getTime()));		
	}

	/**
	 * @return a string with a comma-separated list of holidays for use in SQL statements
	 */
	public static String getSqlListOfHolidays() {
		String ans = "";
		if(mHolidays.isEmpty()) return ans;
		for(Date holiday : mHolidays) {
			if(ans != "") ans += ",";
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			ans += "'"+sdf.format(holiday)+"'";
		}
			
		return ans;
	}
}
