package org.apache.spark.yelp.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;


public class yelp_Q5 {

	public static void main(String[] args) {
		
		//Spark Configuration with Master as local
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
		//Create Java Spark Context
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		//Initialize Hive Context
		HiveContext hivesqlContext = new org.apache.spark.sql.hive.HiveContext(ctx); 
	    
	    //Read the Review JSON file
	    DataFrame reviewFromJsonFile = hivesqlContext.read().json("resources/yelp_academic_dataset_review.json");
	   //Generate temporary table for review
	    reviewFromJsonFile.registerTempTable("review");

	    
	    hivesqlContext.sql("CREATE TABLE IF NOT EXISTS json_table ( json string )");
	    hivesqlContext.sql("LOAD DATA LOCAL INPATH  'resources/yelp_academic_dataset_business.json' INTO TABLE json_table");
	    // Dataframe containing food business near CMU 
	    DataFrame businesscmu=hivesqlContext.sql
	    		("SELECT distinct business_id,name FROM  "
	    		+ "(select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[0]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[1]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[2]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[3]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[4]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[5]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[6]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[7]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[8]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ " union all "
	    		+ "select  get_json_object(json_table.json, '$.business_id') business_id,"
	    		+ "get_json_object(json_table.json, '$.name') name "
	    		+ "from json_table "
	    		+ "where get_json_object(json_table.json, '$.categories[9]') IS NOT NULL "
	    		+ "and get_json_object(json_table.json, '$.latitude') BETWEEN  (40.4411801-0.07279) AND (40.4411801+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.longitude') BETWEEN  (-79.9428294-0.07279) AND (-79.9428294+0.07279) "
	    		+ "AND get_json_object(json_table.json, '$.state')='PA' "
	    		+ "and (get_json_object(json_table.json, '$.attributes.Good For.dessert')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.latenight')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.lunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.dinner')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.brunch')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Good For.breakfast')=='true' "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Take-out')=='true'  "
	    		+ "     or get_json_object(json_table.json, '$.attributes.Drive-Thru')=='true')"
	    		+ ") BUSINESS");
	    //Cache the dataframe
	    businesscmu.cache();
	    //Register Temporary table
	    businesscmu.registerTempTable("businesscmu");
	    
	    //Dataframe for reviews for food business near cmu
	    DataFrame businessreviewcmu= hivesqlContext.sql
	    		("SELECT business.business_id,business.name,review.date,review.stars"
	    		+ " FROM businesscmu business "
	    		+ " JOIN "
	    		+ " review review "
	    		+ " ON (business.business_id=review.business_id)");
       //Cache the dataframe
	    businessreviewcmu.cache();
	    //Create the temporary table
	    businessreviewcmu.registerTempTable("businessreviewcmu");
	   
	   //Dataframe for top 10 food business near CMU (in terms of stars) with average star rating by of month.
	   DataFrame businessreviewcmutop10 = hivesqlContext.sql
			   ("SELECT topbusiness.rank,"
			   		+ "businessreviewcmu.name,"
			   		+ "YEAR(businessreviewcmu.date) YEAR,"
			   		+ "MONTH(businessreviewcmu.date) MONTH,"
			   		+ "AVG(businessreviewcmu.stars) "
			   	+ " FROM "
			   	+ " (SELECT business_id,avg_stars,"
			   	+ " DENSE_RANK() over (ORDER BY avg_stars,business_id DESC) as rank"
			   	+ " FROM "
			   	+ " (SELECT business_id,avg(stars) avg_stars "
			   	+ " FROM businessreviewcmu "
			   	+ " GROUP BY business_id) I "
			   	+ " ORDER BY avg_stars DESC "
			   	+ " LIMIT 10) topbusiness "
			   	+ " JOIN "
			   	+ " businessreviewcmu businessreviewcmu "
			   	+ " ON (topbusiness.business_id=businessreviewcmu.business_id) "
			   	+ " GROUP BY topbusiness.rank,businessreviewcmu.name,YEAR(businessreviewcmu.date),MONTH(businessreviewcmu.date)"
			   	+ " ORDER BY topbusiness.rank DESC,businessreviewcmu.name,YEAR(businessreviewcmu.date),MONTH(businessreviewcmu.date)");
	    
	    //Output the rdd with the output data
	    if(!businessreviewcmutop10.rdd().isEmpty()){
	    	businessreviewcmutop10.rdd().coalesce(1,true,null).saveAsTextFile("output/yelp_Q5_top10"); }
	    
	    //Dataframe for bottom 10 food business near CMU (in terms of stars) with average star rating by of month.
	    DataFrame businessreviewcmubottom10 = hivesqlContext.sql
				   ("SELECT topbusiness.rank,"
				   		+ "businessreviewcmu.name,"
				   		+ "YEAR(businessreviewcmu.date) YEAR,"
				   		+ "MONTH(businessreviewcmu.date) MONTH,"
				   		+ "AVG(businessreviewcmu.stars) "
				   	+ " FROM "
				   	+ " (SELECT business_id,avg_stars,"
				   	+ " DENSE_RANK() over (ORDER BY avg_stars,business_id) as rank"
				   	+ " FROM "
				   	+ " (SELECT business_id,avg(stars) avg_stars "
				   	+ " FROM businessreviewcmu "
				   	+ " GROUP BY business_id) I "
				   	+ " ORDER BY avg_stars"
				   	+ " LIMIT 10) topbusiness "
				   	+ " JOIN "
				   	+ " businessreviewcmu businessreviewcmu "
				   	+ " ON (topbusiness.business_id=businessreviewcmu.business_id) "
				   	+ " GROUP BY topbusiness.rank,businessreviewcmu.name,YEAR(businessreviewcmu.date),MONTH(businessreviewcmu.date)"
				   	+ " ORDER BY topbusiness.rank,businessreviewcmu.name,YEAR(businessreviewcmu.date),MONTH(businessreviewcmu.date)");
		    
	        //Output the rdd with the output data
		    if(!businessreviewcmubottom10.rdd().isEmpty()){
		    	businessreviewcmubottom10.rdd().coalesce(1,true,null).saveAsTextFile("output/yelp_Q5_bottom10"); }
	  
	    ctx.stop();

	}

}
