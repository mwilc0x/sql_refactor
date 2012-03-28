package edu.rutgers.cs336;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Used to create the database tables and indices
 */
public class DbSchemaCreator {

	private DbSchemaCreator() {
		//hide constructor, so no instances
	}
	
	/**
	 * Creates the recommendation table in the database
	 */
	public static void createRecommendationTable() {
		Connection connection = null;
		try {
			connection  = DriverManager.getConnection(EnvManager.getDbUrl());
			Statement statement = connection.createStatement();
			statement.executeUpdate(
					"CREATE TABLE recommendations ("+
					"  stock_symbol   VARCHAR(12), " +
					"  rec_date       DATE, " +
					"  direction      VARCHAR(8), "+
					"  complete_url   VARCHAR(64000), "+
					"  domain_name    VARCHAR(1000), "+
					"  raw_ret1d      DOUBLE, " +
					"  raw_ret5d      DOUBLE, " +
					"  raw_ret20d     DOUBLE, " +
					"  raw_ret60d     DOUBLE, " +
					"  raw_ret125d    DOUBLE, "+
		            "  adj_ret1d      DOUBLE, "+
					"  adj_ret5d      DOUBLE, "+
		            "  adj_ret20d     DOUBLE, "+
					"  adj_ret60d     DOUBLE, "+
		            "  adj_ret125d    DOUBLE, " +
		            "  nearest_bizdate   DATE," +
		            "  date1d     DATE," +
		            "  date5d     DATE," +
		            "  date20d    DATE," +
		            "  date60d    DATE," +
		            "  date125d   DATE," +
		            "  priceRecDate     DOUBLE," +
		            "  price1d     DOUBLE," +
		            "  price5d     DOUBLE," +
		            "  price20d    DOUBLE," +
		            "  price60d    DOUBLE," +
		            "  price125d   DOUBLE," +
		            "  spxRecDate     DOUBLE," +
		            "  spx1d     DOUBLE," +
		            "  spx5d     DOUBLE," +
		            "  spx20d    DOUBLE," +
		            "  spx60d    DOUBLE," +
		            "  spx125d   DOUBLE," +
		            "  spxRet1d    DOUBLE," +
		            "  spxRet5d    DOUBLE," +
		            "  spxRet20d   DOUBLE," +
		            "  spxRet60d   DOUBLE," +
		            "  spxRet125d  DOUBLE)");
			
			statement.executeUpdate(
					"CREATE TABLE spx ("+
					"  date   VARCHAR(44), " +
					"  price       DOUBLE)");
			
			statement.executeUpdate(
					"CREATE TABLE holidays ("+
					"  date   VARCHAR(44))");
			
		} catch (SQLException e) {
			System.err.println("Could not setup recommendation tables");
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
	 * Adds an index on the specified column to the recommendation table
	 * @param colName - the name of the column to index
	 */
	public static void createRecommendationIndex(String colName) {
		Connection connection = null;
		try {
			connection  = DriverManager.getConnection(EnvManager.getDbUrl());
			Statement statement = connection.createStatement();
			statement.executeUpdate(
					"CREATE INDEX ix_recommendations_"+colName+" ON recommendations ("+colName+")");
		} catch (SQLException e) {
			System.err.println("Could not create index on recommendations table for column "+colName);
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
	
}
