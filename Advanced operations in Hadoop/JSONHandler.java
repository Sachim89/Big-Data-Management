package Project2;


import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 * Write a custom input format file called JsonInputFormat to read lines from the input file until it 
 * gets a complete record, and then converts theses values to a list of comma separated values in a
 * single line.
 * @author Saranya Rishitha
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, Text>{

public Pattern[] pattern={Pattern.compile("\\{"),Pattern.compile("\\},*")};
private TextInputFormat textIF = new TextInputFormat();
public long Max_Split_size = 967618/5;

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException 
	{
		// TODO Auto-generated method stub
		return textIF.getSplits(context);
	}
	@Override
	protected long computeSplitSize(long blocksize, long minsize, long maxsize){
		return super.computeSplitSize(blocksize,minsize, Max_Split_size);
		
	}

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		JsonRecordReader reader = new JsonRecordReader();
		
		if (pattern == null){
			throw new IllegalStateException("Pattern not specified");
		}
		reader.setPattern(pattern);
		return reader;
	}


	public class JsonRecordReader extends RecordReader<LongWritable,Text>{
		
		private LineRecordReader lineRecordReader =new LineRecordReader();
		private Pattern[] pattern;
		Text value= new Text();
		
		public void setPattern(Pattern[] p){
			this.pattern=p;
		}
		

		@Override
		public void close() throws IOException {
			lineRecordReader.close();
			// TODO Auto-generated method stub
			
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return lineRecordReader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return lineRecordReader.getProgress();
		}

		@Override
		public void initialize(InputSplit genericSplit, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			 lineRecordReader.initialize(genericSplit, context);
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Text res = new Text();
			StringBuilder sb= new StringBuilder();
			boolean found= false;
			value = new Text();
			
			while(lineRecordReader.nextKeyValue()){
				Matcher matcher1, matcher2;
				String tmp = lineRecordReader.getCurrentValue().toString();
				matcher1 = pattern[0].matcher(tmp);
				matcher2 = pattern[1].matcher(tmp);
				
				if (matcher1.find()) found = true;
				if (found){
				String s = lineRecordReader.getCurrentValue().toString().replaceAll("(\\{)|(\\},*)", "");
				s = s.replaceAll("\\s*", "");
				if (s.length() > 0) sb.append(s);
				if (matcher2.find()) {
				res.set(sb.toString());
				value.append(res.getBytes(), 0, res.getLength());
				return true;
				}
		}		
	}
			return false;
	}
		}
}


