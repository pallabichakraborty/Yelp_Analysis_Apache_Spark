package org.apache.spark.yelp.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/*
 * Rank the cities by # of stars for each category, by city.
 */

public class yelp_Q2 {

	public static void main(String[] args) {
		
		//Spark Configuration with Master as local
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
		//Create Java Spark Context
	    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	    //Initialize Hive Context
	    HiveContext hivesqlContext = new org.apache.spark.sql.hive.HiveContext(ctx); 
	    
	  //Read the Business JSON file
	    DataFrame businessFromJsonFile = hivesqlContext.read().json("resources/yelp_academic_dataset_business.json");
	  //Generate temporary table for business
	    businessFromJsonFile.registerTempTable("business");
	    
	  //Read the Review JSON file
	    DataFrame reviewFromJsonFile = hivesqlContext.read().json("resources/yelp_academic_dataset_review.json");
	  //Generate temporary table for review
	    reviewFromJsonFile.registerTempTable("review");

	    // Create Dataframe with the join the business table with review table
	    DataFrame businessreviewstars = hivesqlContext.sql(
	    		"SELECT UPPER(city) city,review.stars,categories"
	    		+ " FROM business "
	    		+ "JOIN "
	    		+ "review "
	    		+ "on (business.business_id=review.business_id)"
	    		);
	    //Cache the joined data of business and review data
	    businessreviewstars.cache();
	    //Register the table
	    businessreviewstars.registerTempTable("businessreviewstars");
	    
	    //Dataframe with Ranking of the cities by # of stars for each category, by city.
	    DataFrame businessstars = hivesqlContext.sql(
	    		"SELECT category,city,tot_stars,"
	    		+ "rank() OVER (partition by category order by tot_stars) rank "
	    		+ "FROM "
	    		+ "(SELECT city,category,SUM(stars) tot_stars"
	    		+ " FROM "
	    		+ "(SELECT city,stars,categories[0] category FROM businessreviewstars WHERE categories[0] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[1] category FROM businessreviewstars WHERE categories[1] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[2] category FROM businessreviewstars WHERE categories[2] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[3] category FROM businessreviewstars WHERE categories[3] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[4] category FROM businessreviewstars WHERE categories[4] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[5] category FROM businessreviewstars WHERE categories[5] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[6] category FROM businessreviewstars WHERE categories[6] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[7] category FROM businessreviewstars WHERE categories[7] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[8] category FROM businessreviewstars WHERE categories[8] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT city,stars,categories[9] category FROM businessreviewstars WHERE categories[9] IS NOT NULL"
	    		+ ") I "
	    		+ " GROUP BY city,category) J"
	    		);

	    //Output the rdd with the output data
	    if(!businessstars.rdd().isEmpty()){
	    	businessstars.rdd().coalesce(1,true,null).saveAsTextFile("output/yelp_Q2"); }
	   
	    ctx.stop();

	}

}
