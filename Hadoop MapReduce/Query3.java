package Query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
/**
 * Write a	job(s) that joins the Customers and Transactions datasets (based on	the	customer ID)
 * and	reports	for	each customer the following	info:	
 * CustomerID,	Name,	Salary,	NumOf	Transactions,	TotalSum, MinItems
 * Where	NumOfTransactions	is	the	total	number	of	transactions	done	by	the	customer,
 * TotalSum	is	the	sum	of field “TransTotal” for that customer, and MinItems is the minimum number of items
 * in transactions done	by the customer.	

 * @author Saranya, Rishitha
 */

public class Query3 {
	public static class JoinMapperCustomer extends Mapper<LongWritable, Text, Text,Text>
	{
		/**
		 * The output of the Mapper is the CustInfo data (customer name, id, salary)
		 */
		@Override
		public void setup(Context context) throws IOException, InterruptedException 
		{
			    Configuration conf = context.getConfiguration();
		        FileSystem fs = FileSystem.get(conf);
		        URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
		        Path getPath = new Path(cacheFiles[0].getPath());
		        BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
		        String setupData = null;
		        while ((setupData = bf.readLine()) != null) {
		             custInfoDetails(setupData);
		        }
		       bf.close();
		    }
			
		/**Create HashMap to store the Customer details (CustID, Name, Salary) */
		private HashMap<Integer,String> customers =  new HashMap<Integer,String>();
		
		public void custInfoDetails(String line) throws IOException
		{
			String [] tokens = line.split(","); //Read the Input line and split according to comma.
			int custID = Integer.parseInt(tokens[0]);//First token is customer ID
			String name = tokens[1];//Second token is customer name.
			float salary = Float.parseFloat(tokens[4]);//Third token is salary
			
			/**
			 * Finally add these data in HashMap with key as customer ID and value as the set of 
			 * customer id, name and salary.
			 */
			Text CustInfo = new Text();
			CustInfo.set(custID+","+name+","+salary);
			customers.put(custID, CustInfo.toString());
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			//Read the Transaction dataset as usual.
			String [] tokens = value.toString().split(","); 
			int custID = Integer.parseInt(tokens[1]);
			int numofTrans = 1;
			float totalTrans = Float.parseFloat(tokens[2]);
			int minItems = Integer.parseInt(tokens[3]);
			
			/**
			 * Get the customer details from the HashMap by sending the customer ID obtained
			 * from Transaction dataset and the output is custInfo(custid,name,salary) and
			 * transactionInfo(numOfTrans,totalTrans,minItems)
			 **/
			String cust = customers.get(custID);
			
			Text result = new Text();
			result.set(numofTrans+","+totalTrans+","+minItems);
		//	System.out.println("Transaction "+numofTrans+","+totalTrans+","+minItems+","+cust+","+custID);
			context.write(new Text(cust),new Text(result));
		}
	}
	
	private static class TransReducer extends Reducer<Text, Text, IntWritable, Text>
	{
		/**
		 * Perform Reducer to calculate the number of transactions, total transaction
		 * and minimum number of transactions.
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			int numOfTransactions = 0;
			float totalTransSum = 0;
			int minItems = 10;
	
			for(Text value: values)
			{
				String[] recordFields = value.toString().split(",");
				numOfTransactions += Integer.parseInt(recordFields[0]);
				totalTransSum += Float.parseFloat(recordFields[1]);
				
				int currentMinItem = Integer.parseInt(recordFields[2]);
				minItems = currentMinItem >= minItems ? minItems:currentMinItem;
			}
			
			Text outValue = new Text();
			outValue.set(key.toString()+","+numOfTransactions+","+totalTransSum+","+minItems);
			context.write(null, new Text(outValue));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("/user/hadoop/input/Customers.txt"), conf);
		
		Job job = new Job(conf, "Query3");
		job.setJarByClass(Query3.class);
		
		job.setMapperClass(JoinMapperCustomer.class);
		job.setMapOutputKeyClass(Text.class);
		
		job.setReducerClass(TransReducer.class);
		job.setNumReduceTasks(2);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}



