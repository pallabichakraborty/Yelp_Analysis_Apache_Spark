package org.apache.spark.yelp.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/*
 *  Description: Summarize the number of reviews by US city, by business category.
 */

public class yelp_Q1 {

	public static void main(String[] args) {
		
		//Spark Configuration with Master as local
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
		//Create Java Spark Context
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    //Create SQL Context
	    SQLContext sqlContext = new SQLContext(ctx);
	    
	    //Create dataframe to read the file contains the state codes of the US states
	    DataFrame usStates=sqlContext.read().text("resources/usstates.csv");
	    //Generate a temporary table for the US States' Code
	    usStates.registerTempTable("usStates");
	    
	    //Read the Business JSON file
	    String path = "resources/yelp_academic_dataset_business.json";
	    DataFrame businessFromJsonFile = sqlContext.read().json(path);
	    //Cache the data read
	    businessFromJsonFile.cache();
	    //Generate temporary table for business
	    businessFromJsonFile.registerTempTable("business");

	    // Dataframe for Summarize the number of reviews by US city, by business category.
	    DataFrame business = sqlContext.sql(
	    		"SELECT city,category,SUM(review_count) TOT_REVIEW_COUNT"
	    		+ " FROM "
	    		+ " (SELECT UPPER(city) city,review_count,categories[0] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[0] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[1] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[1] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[2] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[2] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[3] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[3] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[4] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[4] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[5] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[5] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[6] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[6] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[7] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[7] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[8] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[8] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT UPPER(city) city,review_count,categories[9] category FROM business JOIN usStates on (business.state=usStates.value) WHERE categories[9] IS NOT NULL"
	    		+ ") I"
	    		+ " GROUP BY city,category"
	    		+ " ORDER BY city,category"
	    		);

	    //Output the rdd with the output data
	    if(!business.rdd().isEmpty()){
	    	business.rdd().coalesce(1,true,null).saveAsTextFile("output/yelp_Q1"); }    
	    
	    ctx.stop();

	}

}
