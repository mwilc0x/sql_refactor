package edu.rutgers.cs336;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  A class to represent a single recommendation
 *  Retrieves prices and computes returns 1, 5, 20, 60 and 125 trading days from the recommendation's initiation
 *  Computes adjusted returns, which remove the S&P return
 *  
 *  Has a method to insert a record with its fields into the database
 */
public class Recommendation {
	private static final String SPX_FILE = EnvManager.getStringVariable("SPX_FILE"); 
	
	private String mStockSymbol; 
	private Date mRecDate;
	private Date mEffectiveDate;
	private String mDirection;
	private String mCompleteUrl;
	private String mDomainName;
	
	private Double mRawReturn1day;
	private Double mRawReturn5day;
	private Double mRawReturn20day;
	private Double mRawReturn60day;
	private Double mRawReturn125day;
	
	private Double mAdjReturn1day;
	private Double mAdjReturn5day;
	private Double mAdjReturn20day;
	private Double mAdjReturn60day;
	private Double mAdjReturn125day;
	
	/**
	 * This constructor 
	 * Special logic is used for CAPS and web.archive.org recommendations
	 * 
	 * @param stockSymbol - the stock's trading ticker
	 * @param recDate - the date the recommendation was made
	 * @param direction - the indicated sentiment of the prediction (positive or negative)
	 * @param completeUrl - the website address of containing the recommendation
	 */
	public Recommendation(String stockSymbol, Date recDate, String direction, String completeUrl) {
		mStockSymbol = stockSymbol;
		
		mRecDate = recDate;
		//If the recommendation falls on a weekend or holiday, 
		//  we need to start from the next business day
		if(DateManager.isBusinessDay(recDate)) {
			mEffectiveDate = recDate;
		} else {
			mEffectiveDate = DateManager.getTradeDateOffestFromDate(recDate, 1);
		}
		mDirection = direction;
		mCompleteUrl = completeUrl;
		
		// the web.archive.org URL contains the original URL from when the recommendation was made
		//   for example - http://web.archive.org/web/20090426202707/http://www.zacks.com/research/broker.php
		//           we want www.zacks.com as the domain name, not web.archive.org
		String urlToSplit;
		if(completeUrl.startsWith("http://web.archive.org")) {
			urlToSplit = completeUrl.substring(completeUrl.indexOf("http://", 7));
		} else {
			urlToSplit = completeUrl; 
		}
		
		//extract the domain name from the URL
		String urlPieces[] = urlToSplit.split("/");
		mDomainName = urlPieces[2];

		//fix any delisted CAPS tickers
		//  CAPS adds .DL to the end of the ticker when the stock is delisted
		//  we want to remove the .DL since we will be using the ticker
		//    at the time of the recommendation to look up the price
		if(mDomainName.equals("caps.fool.com") && mStockSymbol.endsWith(".DL")) {
			mStockSymbol = mStockSymbol.substring(0, mStockSymbol.length() - 3);
		}
		
		//find the future business dates for the return calculations
		Date date1d = DateManager.getTradeDateOffestFromDate(mEffectiveDate, 1);
		Date date5d = DateManager.getTradeDateOffestFromDate(mEffectiveDate, 5);
		Date date20d = DateManager.getTradeDateOffestFromDate(mEffectiveDate, 20);
		Date date60d = DateManager.getTradeDateOffestFromDate(mEffectiveDate, 60);
		Date date125d = DateManager.getTradeDateOffestFromDate(mEffectiveDate, 125);

		//get the prices for each date
		double priceRecDate = getPrice(mEffectiveDate);
		double price1d = getPrice(date1d);
		double price5d = getPrice(date5d);
		double price20d = getPrice(date20d);
		double price60d = getPrice(date60d);
		double price125d = getPrice(date125d);
		
		//compute raw returns
		mRawReturn1day = (price1d - priceRecDate)/priceRecDate;
		mRawReturn5day = (price5d - priceRecDate)/priceRecDate;
		mRawReturn20day = (price20d - priceRecDate)/priceRecDate;
		mRawReturn60day = (price60d - priceRecDate)/priceRecDate;
		mRawReturn125day = (price125d - priceRecDate)/priceRecDate;
		
		//get the SPX price on each date
		double spxRecDate = getSpx(mEffectiveDate);
		double spx1d = getSpx(date1d);
		double spx5d = getSpx(date5d);
		double spx20d = getSpx(date20d);
		double spx60d = getSpx(date60d);
		double spx125d = getSpx(date125d);
		
		//compute the return to SPX over corresponding future intervals 
		double spxRet1d = (spx1d - spxRecDate)/spxRecDate;
		double spxRet5d = (spx5d - spxRecDate)/spxRecDate;
		double spxRet20d = (spx20d - spxRecDate)/spxRecDate;
		double spxRet60d = (spx60d - spxRecDate)/spxRecDate;
		double spxRet125d = (spx125d - spxRecDate)/spxRecDate;
		
		//compute adjusted returns, which remove the market return
		mAdjReturn1day = mRawReturn1day - spxRet1d;
		mAdjReturn5day = mRawReturn5day - spxRet5d;
		mAdjReturn20day = mRawReturn20day - spxRet20d;
		mAdjReturn60day = mRawReturn60day - spxRet60d;
		mAdjReturn125day = mRawReturn125day - spxRet125d;
	}

	
	
	/**
	 * Changes made here: 
	 * 		I changed the Statement to a Prepared Statement
	 */
	
	
	/**
	 * Retrieve the stock price on the given date by doing a query in the DB
	 * 
	 * @param date - the date for which to find the stock price
	 * @return  the stock price on 'date'.  If no price can be found Double.NaN is returned
	 */
	private double getPrice(Date date) {
		Connection connection = null;
		PreparedStatement getPrice = null;
		double price = Double.NaN;
		try {
			connection  = DriverManager.getConnection(EnvManager.getDbUrl());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

			String query = "SELECT close " +
						   "FROM price.daily_price " +
						   "WHERE trade_date = ? AND stock_symbol = ?";

			getPrice = connection.prepareStatement(query);
			getPrice.setString(1, sdf.format(date));
			getPrice.setString(2, mStockSymbol);
			
			ResultSet rs = getPrice.executeQuery();
			if(rs.next()) {
				price = rs.getDouble("close");
				if(rs.wasNull()) {
					price = Double.NaN;
				}
			}
			rs.close();
		} catch (SQLException e) {
			System.err.println("Could not get price for "+mStockSymbol+" on "+date);
			e.printStackTrace();
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
		}
		return price;
	}
	
	/**
	 * Retrieve the level of the S&P 500 on the given date by doing a lookup in the file
	 * 
	 * @param date - the date for which to find the level of the S&P 500
	 * @return  the level of the S&P 500 on 'date'
	 */
	private double getSpx(Date date) {
		BufferedReader reader = null;
		double price = Double.NaN;
		try {
			reader = new BufferedReader(new FileReader(SPX_FILE));
			String line;
			while((line = reader.readLine()) != null) {
				String pieces[] = line.split(",");
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
				Date lineDate = null;
				try {
					 lineDate = dateFormat.parse(pieces[0]);
				} catch (ParseException e) {
					System.err.println("Error parsing SPX price at line "+line);
					e.printStackTrace();
					continue;
				}
				if(lineDate.equals(date)) {
					price = Double.parseDouble(pieces[1]);
					break;
				}
			}
			
		} catch (IOException e) {
			System.err.println("Could not get SPX price for date "+date);
			e.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
				}
			}
		}
		return price;
	}
	
	/**
	 * Changes made here: 
	 * 		Inserted Prepared Statement for Statement
	 * 		TODO LOAD INFILE?
	 */
	
	
	/**
	 * Add this recommendation to the recommendations table in the database
	 */
	public void insertToDb() {
		Connection connection = null;
		PreparedStatement insert = null;
		try {
			connection  = DriverManager.getConnection(EnvManager.getDbUrl());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); 
			String recDateStr = sdf.format(mRecDate);
			
			String query = "INSERT INTO recommendations (stock_symbol, rec_date, direction, complete_url, domain_name, " +
					"raw_ret1d, raw_ret5d, raw_ret20d, raw_ret60d, raw_ret125d, " +
					"adj_ret1d, adj_ret5d, adj_ret20d, adj_ret60d, adj_ret125d)" +
					"VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			
			insert = connection.prepareStatement(query);
			insert.setString(1, mStockSymbol);
			insert.setString(2, recDateStr);
			insert.setString(3, mDirection);
			insert.setString(4, mCompleteUrl);
			insert.setString(5, mDomainName);
			insert.setString(6, makeDoubleString(mRawReturn1day));
			insert.setString(7, makeDoubleString(mRawReturn5day));
			insert.setString(8, makeDoubleString(mRawReturn20day));
			insert.setString(9, makeDoubleString(mRawReturn60day));
			insert.setString(10, makeDoubleString(mRawReturn125day));
			insert.setString(11, makeDoubleString(mAdjReturn1day));
			insert.setString(12, makeDoubleString(mAdjReturn5day));
			insert.setString(13, makeDoubleString(mAdjReturn20day));
			insert.setString(14, makeDoubleString(mAdjReturn60day));
			insert.setString(15, makeDoubleString(mAdjReturn125day));
			
			
			insert.executeUpdate();
			
			
		} catch (SQLException e) {
			System.err.println("Could not insert record "+mStockSymbol+" on "+mRecDate+" from "+mCompleteUrl);
			e.printStackTrace();
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
		}
	}
	
	/**
	 * Converts a double to a string, except if the double is NaN, it returns the string "NULL"
	 * 
	 * @param val - the double to convert
	 * @return - a string representation of val
	 */
	private String makeDoubleString(double val) {
		return Double.isNaN(val) ? "NULL" : Double.toString(val);
	}
	
}
