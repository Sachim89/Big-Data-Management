package Query;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Write a job(s) that reports	for	every	customer, the number of	transactions that
 * customer	did	and	the	total	sum	of	these	transactions.	
 * The	output	file	should	have	one	line	for	each	customer	containing:		
 * CustomerID,	NumTransactions, TotalSum.
 * You	are	required to	use	a	Combiner	in	this	query.	

 * @author Saranya, Rishitha
 *
 */
public class Query2 {
   
	/**
	 * Mapper<Object, Text, Text, Text>
	 * Object , Text -> defines the input datatype.
	 * Text, Text -> defines the output datatype.
	 */
	public static class Query2Mapper extends Mapper<Object, Text, Text, FloatWritable>
	{
		private Text cust_id = new Text();
		/**
		   * Mapper function to identify the customer id and the Transaction total from the Transaction dataset.
		   * TransID, CustID, TransTotal, TransNumItems, TransDesc 
		   * map(Object key, Text value, Context context) -> first two arguments must match the Mapper input.
		   */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String [] tokens=value.toString().split(","); //Seperating the token with respect to comma
			cust_id.set(tokens[1]); //Assigning the first token to cust_id
			FloatWritable total = new FloatWritable(Float.parseFloat(tokens[2]));//Assigning the second token to total.
	        context.write(cust_id, total);
		}
	}

	/**
	 * 
	 * The output datatype of the mapper should match the input of the reducer.
	 *reduce(Text key, final Iterable<FloatWritable> values,final Context context) 
	 *-> first two arguments must match the Reducer input.
	 */
	public static class Query2Reducer extends Reducer<Text,FloatWritable,Text,Text> {
		 
		public void reduce(Text key, final Iterable<FloatWritable> values,final Context context) throws IOException, InterruptedException
		{
				Text result = new Text();
			 	float sum =0.0f;
			 	int count=0;
			 	for (FloatWritable val : values) 
			 	{
			 		sum += val.get();
			 		count++;
			 	}
			 	result.set(count+","+sum); //Concatenating the count and count and setting it to result which is passed the output of the reducer.
	        	context.write(key,result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
			if (args.length != 2) {
				System.err.println("Usage: Query2 <HDFS input file> <HDFS output file>");
				System.exit(2);
			}
			
			conf.set("mapred.textoutputformat.separator", ","); //Function used to remove the tab between "key and value" pair.
              
            Job job = new Job(conf, "Query2Problem");
			job.setJarByClass(Query2.class);
			job.setMapperClass(Query2Mapper.class);
			
			job.setReducerClass(Query2Reducer.class);
			job.setNumReduceTasks(2);
			
			job.setMapOutputKeyClass(Text.class);//Datatype of "key" Mapper class.
			job.setMapOutputValueClass(FloatWritable.class); // Datatype of "value" of mapper class
			
			job.setOutputKeyClass(Text.class); //Datatype of "key" for output
			job.setOutputValueClass(FloatWritable.class);//Datatype of "value" for output
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
