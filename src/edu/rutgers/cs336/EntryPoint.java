package edu.rutgers.cs336;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * This class contains main() and is where execution of the program begins
 */
public class EntryPoint {
	private static final String RECS_FILE = EnvManager.getStringVariable("RECS_FILE");
	private static final String SPX_FILE = EnvManager.getStringVariable("SPX_FILE");
	private static final String HOLIDAY_FILE = EnvManager.getStringVariable("HOLIDAY_FILE");
	
	public static void main(String[] args) {
		// load the MySql driver
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.err.println("Cannot load the MySQL driver");
			e.printStackTrace();
			System.exit(1);
		}
		
		//setup the DB tables & indexes
		DbSchemaCreator.createRecommendationTable();
		DbSchemaCreator.createRecommendationIndex("stock_symbol");
		DbSchemaCreator.createRecommendationIndex("rec_date");
		
		Connection connection = null;
		try {
			connection  = DriverManager.getConnection(EnvManager.getDbUrl());
			Statement insert = connection.createStatement();
			insert.executeUpdate("LOAD DATA LOCAL INFILE '" + RECS_FILE + "' INTO TABLE recommendations FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\' LINES TERMINATED BY '\n' IGNORE 1 LINES");
			
			
			insert.executeUpdate(
					" CREATE FUNCTION get_biz_date(_date DATE) "+ 
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
			
			insert.executeUpdate(
					" CREATE FUNCTION get_domain_name(_name VARCHAR(64000)) "+ 
					" RETURNS VARCHAR(1000) "+ 
					" BEGIN "+
					"   DECLARE return_name_ VARCHAR(64000); "+
					" " +
					"   SET return_name_ = _name; "+
					" " +
					"   IF LOCATE('http://web.archive.org', return_name_) != 0 THEN SET return_name_ = 'www.zacks.com';" +
					"   ELSE WHILE LOCATE('/', return_name_) != 0" +
					"  " +
					"        DO " +
					"         IF LOCATE('http://web.archive.org',return_name_) = 0 AND LOCATE('http://',return_name_) != 0 THEN SET return_name_ = SUBSTRING(return_name_,8);" +
					"         END IF;" +
					"         SET return_name_ = LEFT(return_name_, LOCATE('/', return_name_)-1);" +
					"        END WHILE;" +
					"   END IF;" +
					"   RETURN return_name_; "+
					" END; "
					);

			insert.executeUpdate(
					" CREATE FUNCTION get_one_day(_date DATE) "+ 
					" RETURNS DATE "+ 
					" BEGIN "+
					"   DECLARE return_date_ DATE; " +
					"   DECLARE start_date INT;"+
					" "+
					"   SET return_date_ = _date; " +
					"   SET start_date = DAYOFWEEK(return_date_);"+
					" "+
					"   WHILE DAYOFWEEK(return_date_) IN (1,7) OR DAYOFWEEK(return_date_) = start_date" +
					"         OR return_date_ IN ("+DateManager.getSqlListOfHolidays()+")"+ 
					"   DO "+
					"     SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY); "+
					"   END WHILE; "+
					" "+
					"   RETURN return_date_; "+
					" END; "
					);
			
			insert.executeUpdate(
					" CREATE FUNCTION get_five_day(_date DATE) "+ 
					" RETURNS DATE "+ 
					" BEGIN "+
					"   DECLARE return_date_ DATE; " +
					"   DECLARE count INT;" +
					" "+
					"   SET return_date_ = _date; " +
					"   SET count =1;" +
					"   SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					" "+
					"   WHILE count < 5" +
					"   DO " +
					"     IF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 3 DAY);" +
					"     ELSEIF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 4 DAY); " +
					"     ELSEIF DAYOFWEEK(return_date_) IN (2,3,4,5) AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					"     ELSE SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					"     END IF;"+
					"   END WHILE; "+
					" "+
					"   RETURN return_date_; "+
					" END; "
					);
			
			insert.executeUpdate(
					" CREATE FUNCTION get_twenty_day(_date DATE) "+ 
					" RETURNS DATE "+ 
					" BEGIN "+
					"   DECLARE return_date_ DATE; " +
					"   DECLARE count INT;" +
					" "+
					"   SET return_date_ = _date; " +
					"   SET count =1;" +
					"   SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					" "+
					"   WHILE count < 20" +
					"   DO " +
					"     IF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 3 DAY);" +
					"     ELSEIF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 4 DAY); " +
					"     ELSEIF DAYOFWEEK(return_date_) IN (2,3,4,5) AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					"     ELSE SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					"     END IF;"+
					"   END WHILE; "+
					" "+
					"   RETURN return_date_; "+
					" END; "
					);
			
			insert.executeUpdate(
					" CREATE FUNCTION get_sixty_day(_date DATE) "+ 
					" RETURNS DATE "+ 
					" BEGIN "+
					"   DECLARE return_date_ DATE; " +
					"   DECLARE count INT;" +
					" "+
					"   SET return_date_ = _date; " +
					"   SET count =1;" +
					"   SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					" "+
					"   WHILE count < 60" +
					"   DO " +
					"     IF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 3 DAY);" +
					"     ELSEIF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 4 DAY); " +
					"     ELSEIF DAYOFWEEK(return_date_) IN (2,3,4,5) AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					"     ELSE SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					"     END IF;"+
					"   END WHILE; "+
					" "+
					"   RETURN return_date_; "+
					" END; "
					);
			
			insert.executeUpdate(
					" CREATE FUNCTION get_onetwentyfive_day(_date DATE) "+ 
					" RETURNS DATE "+ 
					" BEGIN "+
					"   DECLARE return_date_ DATE; " +
					"   DECLARE count INT;" +
					" "+
					"   SET return_date_ = _date; " +
					"   SET count =1;" +
					"   SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
					" "+
					"   WHILE count < 125" +
					"   DO " +
                    "     IF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 3 DAY); " +
					"     ELSEIF DAYOFWEEK(return_date_) = 6 AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") AND DATE_ADD(return_date_, INTERVAL 3 DAY) IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 4 DAY); " +
                    "     ELSEIF DAYOFWEEK(return_date_) IN (2,3,4,5) AND return_date_ NOT IN ("+DateManager.getSqlListOfHolidays()+") THEN SET count = count + 1, return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
                    "     ELSE SET return_date_ = DATE_ADD(return_date_, INTERVAL 1 DAY);" +
                    "     END IF;"+
                    "   END WHILE; "+
					"   IF DAYOFMONTH(return_date_) = 3 AND return_date_ IN ("+DateManager.getSqlListOfHolidays()+") THEN SET return_date_ = DATE_ADD(return_date_, INTERVAL 3 DAY);" +
					"   END IF;"+
					" "+
					"   RETURN return_date_; "+
					" END; "
					);

			//Series of sequential SQL updates
			insert.executeUpdate("UPDATE recommendations SET domain_name = get_domain_name(complete_url)");
			insert.executeUpdate("UPDATE recommendations SET nearest_bizdate = get_biz_date(rec_date)");
			insert.executeUpdate("UPDATE recommendations SET date1d = get_one_day(nearest_bizdate)");
			insert.executeUpdate("UPDATE recommendations SET date5d = get_five_day(nearest_bizdate)");
			insert.executeUpdate("UPDATE recommendations SET date20d = get_twenty_day(nearest_bizdate)");
			insert.executeUpdate("UPDATE recommendations SET date60d = get_sixty_day(nearest_bizdate)");
			insert.executeUpdate("UPDATE recommendations SET date125d = get_onetwentyfive_day(nearest_bizdate)");
			insert.executeUpdate("UPDATE recommendations r JOIN price.daily_price p SET priceRecDate = p.close WHERE p.trade_date = r.nearest_bizdate AND p.stock_symbol = r.stock_symbol");
			insert.executeUpdate("UPDATE recommendations r JOIN price.daily_price p SET price1d = p.close WHERE p.trade_date = r.date1d AND p.stock_symbol = r.stock_symbol");
			insert.executeUpdate("UPDATE recommendations r JOIN price.daily_price p SET price5d = p.close WHERE p.trade_date = r.date5d AND p.stock_symbol = r.stock_symbol");
			insert.executeUpdate("UPDATE recommendations r JOIN price.daily_price p SET price20d = p.close WHERE p.trade_date = r.date20d AND p.stock_symbol = r.stock_symbol");
			insert.executeUpdate("UPDATE recommendations r JOIN price.daily_price p SET price60d = p.close WHERE p.trade_date = r.date60d AND p.stock_symbol = r.stock_symbol");
			insert.executeUpdate("UPDATE recommendations r JOIN price.daily_price p SET price125d = p.close WHERE p.trade_date = r.date125d AND p.stock_symbol = r.stock_symbol");
			insert.executeUpdate("UPDATE recommendations SET raw_ret1d = (price1d - priceRecDate)/ priceRecDate");
			insert.executeUpdate("UPDATE recommendations SET raw_ret5d = (price5d - priceRecDate)/ priceRecDate");
			insert.executeUpdate("UPDATE recommendations SET raw_ret20d = (price20d - priceRecDate)/ priceRecDate");
			insert.executeUpdate("UPDATE recommendations SET raw_ret60d = (price60d - priceRecDate)/ priceRecDate");
			insert.executeUpdate("UPDATE recommendations SET raw_ret125d = (price125d - priceRecDate)/ priceRecDate");
			insert.executeUpdate("LOAD DATA LOCAL INFILE '" + SPX_FILE + "' INTO TABLE spx FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\' LINES TERMINATED BY '\n' (@var1) SET date = STR_TO_DATE(@var1, '%m/%d/%Y')");
			insert.executeUpdate("LOAD DATA LOCAL INFILE '" + SPX_FILE + "' INTO TABLE holidays FIELDS TERMINATED BY ',' ENCLOSED BY '\"' ESCAPED BY '\' LINES TERMINATED BY '\n' (@var1) SET date = STR_TO_DATE(@var1, '%m/%d/%Y')");
			insert.executeUpdate("UPDATE recommendations r JOIN mwilcox.spx p SET spxRecDate = p.price WHERE r.nearest_bizdate = p.date");
			insert.executeUpdate("UPDATE recommendations r JOIN mwilcox.spx p SET spx1d = p.price WHERE r.date1d = p.date");
			insert.executeUpdate("UPDATE recommendations r JOIN mwilcox.spx p SET spx5d = p.price WHERE r.date5d = p.date");
			insert.executeUpdate("UPDATE recommendations r JOIN mwilcox.spx p SET spx20d = p.price WHERE r.date20d = p.date");
			insert.executeUpdate("UPDATE recommendations r JOIN mwilcox.spx p SET spx60d = p.price WHERE r.date60d = p.date");
			insert.executeUpdate("UPDATE recommendations r JOIN mwilcox.spx p SET spx125d = p.price WHERE r.date125d = p.date");
			insert.executeUpdate("UPDATE recommendations SET spxRet1d = (spx1d - spxRecDate)/ spxRecDate");
			insert.executeUpdate("UPDATE recommendations SET spxRet5d = (spx5d - spxRecDate)/ spxRecDate");
			insert.executeUpdate("UPDATE recommendations SET spxRet20d = (spx20d - spxRecDate)/ spxRecDate");
			insert.executeUpdate("UPDATE recommendations SET spxRet60d = (spx60d - spxRecDate)/ spxRecDate");
			insert.executeUpdate("UPDATE recommendations SET spxRet125d = (spx125d - spxRecDate)/ spxRecDate");
			insert.executeUpdate("UPDATE recommendations SET adj_ret1d = raw_ret1d - spxRet1d");
			insert.executeUpdate("UPDATE recommendations SET adj_ret5d = raw_ret5d - spxRet5d");
			insert.executeUpdate("UPDATE recommendations SET adj_ret20d = raw_ret20d - spxRet20d");
			insert.executeUpdate("UPDATE recommendations SET adj_ret60d = raw_ret60d - spxRet60d");
			insert.executeUpdate("UPDATE recommendations SET adj_ret125d = raw_ret125d - spxRet125d");
			
			//drop all of the temporary columns and the temporary table spx
			insert.executeUpdate("alter table recommendations drop column nearest_bizdate");
			insert.executeUpdate("alter table recommendations drop column date1d");
			insert.executeUpdate("alter table recommendations drop column date5d");
			insert.executeUpdate("alter table recommendations drop column date20d");
			insert.executeUpdate("alter table recommendations drop column date60d");
			insert.executeUpdate("alter table recommendations drop column date125d");
			insert.executeUpdate("alter table recommendations drop column priceRecDate");
			insert.executeUpdate("alter table recommendations drop column price1d");
			insert.executeUpdate("alter table recommendations drop column price5d");
			insert.executeUpdate("alter table recommendations drop column price20d");
			insert.executeUpdate("alter table recommendations drop column price60d");
			insert.executeUpdate("alter table recommendations drop column price125d");
			insert.executeUpdate("alter table recommendations drop column spxRecDate");
			insert.executeUpdate("alter table recommendations drop column spx1d");
			insert.executeUpdate("alter table recommendations drop column spx5d");
			insert.executeUpdate("alter table recommendations drop column spx20d");
			insert.executeUpdate("alter table recommendations drop column spx60d");
			insert.executeUpdate("alter table recommendations drop column spx125d");
			insert.executeUpdate("alter table recommendations drop column spxRet1d");
			insert.executeUpdate("alter table recommendations drop column spxRet5d");
			insert.executeUpdate("alter table recommendations drop column spxRet20d");
			insert.executeUpdate("alter table recommendations drop column spxRet60d");
			insert.executeUpdate("alter table recommendations drop column spxRet125d");
			insert.executeUpdate("drop table spx");
			
			
			System.out.println("Loaded data successfully");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
		} 
		
		//create the report
		AggregationReport.createReport();

	}
}
