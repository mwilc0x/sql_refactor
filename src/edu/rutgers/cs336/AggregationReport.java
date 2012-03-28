package edu.rutgers.cs336;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Used to create the report and place it in OUTFILE
 *
 */
public class AggregationReport {

	private AggregationReport() {
		//hide constructor, so no instances
	}
	
	/* INSERT PREPARED STATEMENTS HERE */
	/**
	 * Makes the output report and places it in OUT_FILE
	 * The recommendations table must already be loaded
	 */
	public static void createReport() {
		Connection connection = null;
		FileWriter fileWriter = null;
		try {
			connection  = DriverManager.getConnection(EnvManager.getDbUrl());
			Statement statement = connection.createStatement();
			statement.executeUpdate(
				" CREATE FUNCTION get_nearest_business_date(_date DATE) "+ 
				" RETURNS DATE "+ 
				" BEGIN "+
				"   DECLARE return_date_ DATE; "+
				" "+
				"   SET return_date_ = _date; "+
				" "+
				"   WHILE DAYOFWEEK(return_date_) IN (1,7) " +
				"         OR return_date_ IN ("+DateManager.getSqlListOfHolidays()+") "+ 
				"   DO "+
				"     SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY); "+
				"   END WHILE; "+
				" "+
				"   RETURN return_date_; "+
				" END; "
				);
			
			//execute the SQL to get the tuples for the report
			// multiply by 100 to convert to percent
			ResultSet rs = statement.executeQuery(
					" SELECT domain_name, "+
					"        COUNT(1) as count, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*raw_ret1d),3) AS raw_ret1d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*raw_ret5d),3) AS raw_ret5d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*raw_ret20d),3) AS raw_ret20d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*raw_ret60d),3) AS raw_ret60d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*raw_ret125d),3) AS raw_ret125d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*adj_ret1d),3) AS adj_ret1d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*adj_ret5d),3) AS adj_ret5d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*adj_ret20d),3) AS adj_ret20d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*adj_ret60d),3) AS adj_ret60d, "+
					"        ROUND(100*AVG(IF(direction = 'negative', -1, 1)*adj_ret125d),3) AS adj_ret125d "+
					" FROM recommendations r "+
					" WHERE (r.stock_symbol, get_nearest_business_date(r.rec_date)) " +
					"            IN (SELECT stock_symbol, trade_date " +
					"                FROM price.daily_price" +
					"                WHERE close IS NOT NULL) "+
					" GROUP BY domain_name "+
					" ORDER BY adj_ret5d DESC");
			
			//open the output file
			fileWriter = new FileWriter(EnvManager.getStringVariable("OUT_FILE"));

			//get the column names and use them to create a header
			ResultSetMetaData rsmd = rs.getMetaData();
			fileWriter.append(rsmd.getColumnName(1));
			for(int c=2; c <= rsmd.getColumnCount(); c++) {
				fileWriter.append(',');
				fileWriter.append(rsmd.getColumnName(c));
			}
			fileWriter.append('\n');
			
			//put the tuples into the report, column separated
			while(rs.next()) {
				String val = rs.getString(1);
				if(!rs.wasNull()) fileWriter.append(val);
				for(int c=2; c <= rsmd.getColumnCount(); c++) {
					fileWriter.append(',');
					val = rs.getString(c);
					if(!rs.wasNull()) fileWriter.append(val);
				}
				fileWriter.append('\n');
			}
		} catch (SQLException e) {
			System.err.println("Could not create aggregation report");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Could not create aggregation report");
			e.printStackTrace();
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
			if(fileWriter != null) {
				try {
					fileWriter.close();
				} catch (Exception e) {
				}
			}
		}
	}

}
