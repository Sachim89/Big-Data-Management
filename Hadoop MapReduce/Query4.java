package Query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 
 *Write a job(s) that reports for every country	code, the number of	customers having this code as well	as	
 *the min and max of TransTotal	fields for the transactions	done by those customers. The output	file should	have
 *one line for each	country	code containing: CountryCode, NumberOfCustomers, MinTransTotal,	MaxTransTotal
 *
 *@author Saranya, Rishitha
 */
public class Query4 {
	public static class CustCountryCode extends Mapper<Object, Text, Text,FloatWritable>
	{
		/** Create Hash Map to store the Customer details: Customer ID and Country code*/
		private HashMap<Integer,Integer> CustCode =  new HashMap<Integer,Integer>();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException 
		{
			    Configuration conf = context.getConfiguration();
		        FileSystem fs = FileSystem.get(conf);
		        URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		        Path getPath = new Path(cacheFiles[0].getPath());
		      
		        /**Read the Customer file using BufferedReader and store it in HashMap*/
		        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
		        String setupData = null;
		        String [] tokens;
		        
		        while ((setupData = bf.readLine()) != null)
		        {
		        	tokens = setupData.split(","); //Read each line and split the values by comma.
					int custID_key = Integer.parseInt(tokens[0]);//First token is the customer id: Primary key for both data sets
					int country_code = Integer.parseInt(tokens[3]);//Third token is the country code.
					CustCode.put(custID_key, country_code);//Store these values in the HashMap
					
		        }
		       bf.close();
		    }
				
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			/**Read the Transaction data sets as usual*/
			String [] tokens = value.toString().split(","); 
			int custID = Integer.parseInt(tokens[1]);
			FloatWritable totalTrans = new FloatWritable(Float.parseFloat(tokens[2]));
				
			/**
			 * Send the custID obtained from Transactions data set and map to the ID in the Customer HashMap
			 * and retrieve the customer details (Customer ID and Country code).
			 */
			Text countyCode = new Text();
			countyCode.set(CustCode.get(custID).toString());
			context.write(countyCode,totalTrans);
		}
	}
	
	private static class TransMinMax extends Reducer<Text, FloatWritable, IntWritable, Text>
	{
		/**
		 * Perform Reducer to calculate the Minimum Transactions, Maximum Transactions 
		 * and count the number of transactions.
		 */
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)throws IOException, InterruptedException
		{
			int numOfCustomers = 0;
			float MaxTrans = Integer.MIN_VALUE;
			float MinTrans = Integer.MAX_VALUE;
	
			for(FloatWritable value: values)
			{
				numOfCustomers ++;
				float currentValue = value.get();
				if(currentValue > MaxTrans)
					MaxTrans = currentValue;
				if(currentValue < MinTrans)
					MinTrans = currentValue;
			}
			
			Text outValue = new Text();
			outValue.set(key.toString()+","+numOfCustomers+","+MinTrans+","+MaxTrans);
			context.write(null, new Text(outValue));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("/user/hadoop/input/Customers.txt"), conf);
		
		Job job = new Job(conf, "Query4");
		job.setJarByClass(Query4.class);
		
		job.setMapperClass(CustCountryCode.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setReducerClass(TransMinMax.class);
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}


