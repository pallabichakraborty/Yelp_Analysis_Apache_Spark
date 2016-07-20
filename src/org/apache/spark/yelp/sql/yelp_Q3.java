package org.apache.spark.yelp.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/*
 * What is the average rank (stars) for businesses within 5 miles of Carnegie Mellon University in Pittsburgh, PA, by type?
 * Center: Carnegie Mellon University, Pitsburgh, PA
 * Latitude: 40-26'28'' N, Longitude: 079-56'34'' W
 * Decimal Degrees: Latitude: 40.4411801, Longitude: -79.9428294
 * The bounding box for this problem is ~5 miles, which we will loosely define as 5 minutes. So the bounding box is a square box, 10 minutes each side (of longitude and latitude), with CMU at the center.
 */

public class yelp_Q3 {

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

	    // Dataframe from business for the businesses near CMU filtering using the latitude and longitude
	    DataFrame businesscmu = hivesqlContext.sql(
	    		"SELECT categories,business_id,name,state,latitude,longitude"
	    		+ " FROM business"
	    		+ " WHERE latitude BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279)"
	    		+ " AND longitude BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279)"
	    		+ " AND STATE='PA'");
	    //Cache the dataframe
	    businesscmu.cache();
	    //Register the temporary table with dataframe with the businesses near CMU
	    businesscmu.registerTempTable("businesscmu");
	    
	    //Dataframe with average rank (stars) for businesses within 5 miles of Carnegie Mellon University in Pittsburgh, PA, by type
	    DataFrame businessreviewavgstars = hivesqlContext.sql(
	    		"SELECT CATEGORY,NAME,ROUND(AVGSTARS,2),"
	    		+ "RANK() over (PARTITION BY CATEGORY ORDER BY AVGSTARS DESC) as rank"
	    		+ " FROM "
	    		+ " (SELECT BUSINESS.CATEGORY,BUSINESS.NAME,AVG(REVIEW.STARS) AVGSTARS"
	    		+ " FROM"
	    		+ "(SELECT business_id,name,categories[0] category FROM businesscmu WHERE categories[0] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[1] category FROM businesscmu WHERE categories[1] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[2] category FROM businesscmu WHERE categories[2] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[3] category FROM businesscmu WHERE categories[3] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[4] category FROM businesscmu WHERE categories[4] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[5] category FROM businesscmu WHERE categories[5] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[6] category FROM businesscmu WHERE categories[6] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[7] category FROM businesscmu WHERE categories[7] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[8] category FROM businesscmu WHERE categories[8] IS NOT NULL"
	    		+ " UNION ALL "
	    		+ "SELECT business_id,name,categories[9] category FROM businesscmu WHERE categories[9] IS NOT NULL"
	    		+ ") BUSINESS"
	    		+ " JOIN"
	    		+ " review REVIEW"
	    		+ " ON (BUSINESS.BUSINESS_ID=REVIEW.BUSINESS_ID)"
	    		+ " GROUP BY BUSINESS.CATEGORY,BUSINESS.NAME) I"
	    		);
	    

	  //Output the rdd with the output data
	    if(!businessreviewavgstars.rdd().isEmpty()){
	    	businessreviewavgstars.rdd().coalesce(1,true,null).saveAsTextFile("output/yelp_Q3"); }
	   
	    ctx.stop();

	}

}
