package org.apache.spark.yelp.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/*
 * Rank reviewers by number of reviews. For the top 10 reviewers, show their average number of stars, by category.
 */

public class yelp_Q4 {

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
	    
	    //Read the User JSON file
	    DataFrame userFromJsonFile = hivesqlContext.read().json("resources/yelp_academic_dataset_user.json");
	    //Generate temporary table for user
	    userFromJsonFile.registerTempTable("user");

	    // Dataframe with the data for the top reviewers on the basis of the number of reviews and the category level data and corresponding stars
	    DataFrame businessreviewuser = hivesqlContext.sql(
	    		"SELECT reviewdata.name,reviewdata.rank,reviewdata.stars,business.categories"
	    		+ " FROM "
	    		+ "(SELECT userdetails.rank,userdetails.name,reviewdata.business_id,reviewdata.stars"
	    		+ " FROM "
	    		+ "(SELECT topusers.user_id,topusers.rank,users.name "
	    		+ " FROM "
	    		+ "(SELECT user_id,"
	    		+ " DENSE_RANK() over (ORDER BY numreviews DESC) as rank"
	    		+ " FROM "
	    		+ "(SELECT reviewdata.user_id,count(*) numreviews"
	    		+ " FROM review reviewdata "
	    		+ " GROUP BY reviewdata.user_id) reviewdata "
	    		+ " ORDER BY numreviews DESC "
	    		+ " LIMIT 10) topusers "
	    		+ " JOIN user users "
	    		+ " ON (users.user_id=topusers.user_id)) userdetails "
	    		+ " JOIN "
	    		+ " review reviewdata "
	    		+ " ON (reviewdata.user_id=userdetails.user_id)) reviewdata "
	    		+ " JOIN "
	    		+ " business business "
	    		+ " ON (reviewdata.business_id=business.business_id)");
	    businessreviewuser.cache();
	    businessreviewuser.registerTempTable("businessreviewuser");
	    
	   //Dataframe for the top 10 reviewers, with average number of stars, by category.
	   DataFrame businessreviewavgstars = hivesqlContext.sql
			   (
				"SELECT name,rank, category,ROUND(AVG(stars),2) "
				+ " FROM "
				+ " (SELECT name,rank,stars,categories[0] category FROM businessreviewuser WHERE categories[0] IS NOT NULL"
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[1] category FROM businessreviewuser WHERE categories[1] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[2] category FROM businessreviewuser WHERE categories[2] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[3] category FROM businessreviewuser WHERE categories[3] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[4] category FROM businessreviewuser WHERE categories[4] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[5] category FROM businessreviewuser WHERE categories[5] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[6] category FROM businessreviewuser WHERE categories[6] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[7] category FROM businessreviewuser WHERE categories[7] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[8] category FROM businessreviewuser WHERE categories[8] IS NOT NULL "
				+ " UNION ALL "
				+ " SELECT name,rank,stars,categories[9] category FROM businessreviewuser WHERE categories[9] IS NOT NULL"
				+ ") data "
				+ " GROUP BY name,rank, category"
				+ " ORDER BY rank ,name, category");
	    
	    //Output the rdd with the output data
	    if(!businessreviewavgstars.rdd().isEmpty()){
	    	businessreviewavgstars.rdd().coalesce(1,true,null).saveAsTextFile("output/yelp_Q4"); }
	   
	    ctx.stop();

	}

}
