package Query;
/**
 * Write a job(s) that reports the customer	names whose	associated number of transactions is larger than
 * the average number of transactions of all customers. That is, calculate the average number of transactions
 * over	all	customers (Say value x),you	goal is	to report the customer names who have more than	x transactions.	
 * @author Saranya, Rishitha
 **/
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Query5 {
   
public static class Query5FMapper extends Mapper<LongWritable, Text,IntWritable, Text>
{
	private final static IntWritable one = new IntWritable(1);
	private Text cust_id = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String [] tokens=value.toString().split(",");
		cust_id.set(tokens[1]);
		context.write(one,cust_id);
	}
}
   
public static class Query5FReducer extends Reducer<IntWritable,Text,Text,Text> 
{
	private HashMap<Integer,String> customers =  new HashMap <Integer,String>();
	private HashMap<String,Integer> customers1 =  new HashMap <String,Integer>();
  
	public void custInfoDetails(String line) throws IOException
	{
		String [] tokens = line.split(",");
		int custID = Integer.parseInt(tokens[0]);
		String name = tokens[1].toString();
		customers.put(custID, name);       
		customers1.put(tokens[0], 0);
	}
         
	@Override
	public void setup(Context context)throws IOException, InterruptedException 
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
                
	public void reduce(IntWritable key, final Iterable<Text> values,final Context context) throws IOException, InterruptedException
	{
		Text result = new Text();                     
		int sum =0;
		for (Text val : values)
		{
			if(customers1.containsKey(val.toString()))
			{
				customers1.put(val.toString(), customers1.get(val.toString())+1);
			}
			sum++; 
		}
		float avg = sum/50000;            
		for( Entry<Integer, String> entry : customers.entrySet()){
			String cust_id = entry.getKey().toString();
			String name = entry.getValue();
			if(customers1.containsKey(cust_id)){
				Integer t = customers1.get(cust_id);
				float tr = Float.parseFloat(t.toString());
				if(tr > avg){
					result.set(name);
					context.write(null,result);
				}
			}
		}                
	}
}

public static void main(String[] args) throws Exception
{
	Configuration conf = new Configuration();    
	DistributedCache.addCacheFile(new URI("/user/hadoop/input/Customers.txt"), conf);
	if (args.length != 2) {
		System.err.println("Usage: Query5 <HDFS input file> <HDFS output file>");
		System.exit(2);
	}
	Job job = new Job(conf, "Query5");
	job.setJarByClass(Query5Final.class);
	job.setMapperClass(Query5FMapper.class);
	job.setReducerClass(Query5FReducer.class);
	job.setNumReduceTasks(1);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
}
}
