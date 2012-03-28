package edu.rutgers.cs336;


/**
 * Used to retrieve environment variables
 * Has a method to build a connection string to the database 
 */
public class EnvManager {

	private EnvManager() {
		//hide constructor, so no instances
	}

	/**
	 * Used to retrieve values for environment variables.
	 * Throws a runtime exception if the environment variable is null
	 * 
	 * @param envVar - the environment variable 
	 * @return the string representation of the value associated with environment variable envVar
	 */
	public static String getStringVariable(String envVar) {
		String rv = System.getenv(envVar);
		if(rv == null) {
			throw new RuntimeException("Environment variable '"+envVar+"' is null");
		}
		return rv.trim();
	}
	
	/**
	 * Creates a connection string used to connect to the database
	 *  The string is constructed from environment variables
	 * @return the database URL
	 */
	public static String getDbUrl() {
		return "jdbc:mysql://"+
				getStringVariable("RECS_DB_HOST")+":"+
				getStringVariable("RECS_DB_PORT")+"/"+
				getStringVariable("RECS_DB_NAME")+"?"+
				"user="+getStringVariable("RECS_DB_USER")+"&"+
				"password="+getStringVariable("RECS_DB_PASSWORD");
	}

}
