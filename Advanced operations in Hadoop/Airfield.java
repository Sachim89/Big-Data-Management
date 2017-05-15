package Project2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Write map-reduce job(s) that aggregates all records based on the "Flag" field, and for each
 * flag values report the number of corresponding records.
 * @author Saranya Rishitha
 */

public class Airfield {
	
	 public static class RecordMapper 
     extends Mapper<Object, Text, Text, IntWritable>{
  
		 private final static IntWritable one = new IntWritable(1);
		 private  IntWritable out = new IntWritable();
		 //private Text value = new Text();
    
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
	  String[] tokens= value.toString().split(",");
	  String str=tokens[5];
	  String[] tokens1= str.split(":");
	  Text flags=new Text(tokens1[1]);
      context.write(flags, one);
  }
}
	 
public static class q2Reducer 
     extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();

  public void reduce(Text key, Iterable<IntWritable> values, 
                     Context context
                     ) throws IOException, InterruptedException {
    int count = 0;
    for (IntWritable val : values) {
      count+= val.get();
    }
    result.set(count);
    context.write(key, result);
  }
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  if (args.length != 2) {
    System.err.println("Usage: json input question <HDFS input file> <HDFS output file>");
    System.exit(2);
  }
  Job job = new Job(conf, "json input format");
  job.setJarByClass(Airfield.class);
  job.setInputFormatClass(JsonInputFormat.class);
  job.setMapperClass(RecordMapper.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(IntWritable.class);
  job.setReducerClass(q2Reducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  job.setNumReduceTasks(1);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

