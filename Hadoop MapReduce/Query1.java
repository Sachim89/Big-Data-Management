package Query;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.hadoop.BigDataM.MapReduce;

/**
 * Write a job(s) that reports the customers whose CountryCode between 2 and 6 (inclusive). 
 * @author Saranya, Rishitha
 *
 */
public class Query1 {

	/**
	 * Mapper<Object, Text, Text, Text>
	 * Object , Text -> defines the input datatype.
	 * Text, Text -> defines the output datatype.
	 */
  public static class TransMapper extends Mapper<Object, Text, Text, Text>
  {
	  private Text cust_id = new Text();
    
	  /**
	   * Mapper function to identify the customer id and the country code from the Customers dataset.
	   * Customer Id, Customer Name, Age, COuntry code and Salary.
	   * 
	   * map(Object key, Text value, Context context) -> first two arguments must match the Mapper input.
	   */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString(),",");//Finding each token where the seperator is comma
    		cust_id.set(itr.nextToken());//Get the value of the token customer id.
    		itr.nextToken();
    		itr.nextToken();
    		Text county = new Text(itr.nextToken());// Get the value of the token country code.
    		int code = Integer.parseInt(county.toString());
    		if( code >= 2 && code <= 6)
        	{
        		context.write(cust_id,county);
        	}
      }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    Job job = new Job(conf,"Query1");
    job.setJarByClass(Query1.class); //Produce jar file for the class.
    
    job.setMapperClass(TransMapper.class);//Set the Mapper class.
    
    job.setOutputValueClass(Text.class);//Set the output datatype of the "value" of the mapper class.
    job.setOutputKeyClass(Text.class);//Set the output datatype of the "key" of the mapper class.
    
    job.setNumReduceTasks(0);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

