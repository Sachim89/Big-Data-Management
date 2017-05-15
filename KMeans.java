package Project2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Write map-reduce job(s) that implement the K-Means clustering algorithm as given in the course slides.
 * The algorithm should terminates if either of these two conditions become true:
 * a)The K centers did not change over two consecutive iterations
 * b) The maximum number of iterations (make it five (5) iterations) has reached.
 * Apply the tricks given in class and in the 2nd link above such as:
 * Use of a combiner, Use a single reducer, The reducer should indicate in its output file whether centers
 * @author Saranya Rishitha
 */
public class Kmeans {

    private static final int MAXITERATIONS = 6;
    private static final double THRESHOLD = 0.1;
    private static int ITERNUM = 1;
    private static String out = "/part-r-00000";
   
    /**Calculates the Euclidean distance between  points */
    private static double getDist(double[] points, List<Double> centroid)
    {
        double result_dist = 0.0;
        for (int i =0; i < centroid.size(); i++)
        {
            result_dist += Math.pow(points[i] - centroid.get(i), 2);
        }
        return Math.sqrt(Double.parseDouble(String.valueOf(result_dist)));
    }
   
    /**
     * Mapper is used to determine the distance between the centroid and the points.
     * The output of the mapper is the centroid's index and the set of all points which are close to it.
     */
    public static class KmeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>
    {
        private List<List<Double>> centers = new ArrayList<List<Double>>();
        private int k;
       
        /**Relate each centroid with all points so that it is easy to calculate the distance for the nearest centroid*/
        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
            try
            {
                k = 0;
                Configuration conf = context.getConfiguration();
                FileSystem fs = FileSystem.get(conf);
                URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
                Path getPath = new Path(cacheFiles[0].getPath());
               
                if(cacheFiles == null || cacheFiles.length <= 0)
                {
                    System.exit(1);
                }
                             
                //Read the Centroids file using BufferedReader and store it in ArrayList
                BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
                String setupData = null;
                while ((setupData = bf.readLine()) != null)
                {
                    centers.add(new ArrayList<Double>());
                    String[] str = setupData.split(",");
                                   
                    //For each centroid append all the points
                    for(int i = 0; i < str.length; i++)
                    {
                        centers.get(k).add(Double.parseDouble(str[i]));
                    }
                    //Increment the position in the ArrayList to store the centroids
                    k++;
                }
                bf.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
       
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //Get all the points for a particular centroid.
            String[] pointsForCentroid = value.toString().split(",");
           
            //Read the values and store those points in a double array.
            double[] points = new double[pointsForCentroid.length];
            for (int i = 0; i < points.length; i ++)
            {
                points[i] = Double.parseDouble(pointsForCentroid[i]);
            }
           
            double minimun_dist = Double.MAX_VALUE;
            double current_dist;
            int mappedCentroid = 0;
           
            //Calculate the distance and store the minimum distance and also map it to the centroid to which its distance is min
            for (int j = 0; j < centers.size(); j ++)
            {
                current_dist = getDist(points, centers.get(j));
           
                if (current_dist < minimun_dist)
                {
                    minimun_dist = current_dist;
                    mappedCentroid = j;
                }
            }
            Text result_points = new Text();
            for( int i = 0 ;i < points.length; i++)
            {
                result_points.set(result_points+","+String.valueOf(points[i]));
                
            }
           
            //For each a key(index which points to centroid in the array list) and a value (point close to that centroid) is returned
            context.write(new IntWritable(mappedCentroid), result_points);
        }
    }
       
    /**
     * Combiner will take the centroid's index and the set of values (points) 
     * and computes the sum of all the points for that centroid     
     */
    public static class KmeansCombiner extends Reducer<IntWritable,Text,IntWritable,Text>
    {
        private int d = 0;
       
        //Reading the Centroids
        public void setup(Context context) throws IOException,InterruptedException
        {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
            Path getPath = new Path(cacheFiles[0].getPath());
           
            if(cacheFiles == null || cacheFiles.length <= 0)
            {
                System.exit(1);
            }
           
            BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(getPath)));
            String setupData = null;
           
            while ((setupData = bf.readLine()) != null)
           
            if ((setupData = bf.readLine())!=null)
            {
                String[] str = setupData.split(",");
                d = str.length; //Obtain the total number of centroids
            }
            try{
            bf.close();
            }catch (Exception e){
            e.printStackTrace();
            }
        }
   
        public void reduce(IntWritable key,Iterable<Text> values,Context context)throws InterruptedException, IOException
        {
            double[] sum=new double [d];
            int combiner_count = 0;
            StringBuilder sb= new StringBuilder();
            	for (Text val: values)
            {
                String[] datastr = val.toString().split(","); //Points for particular centroid
           
               for(int i = 1; i <= d; i++)
                {
                	sum[i-1]+=Double.parseDouble((datastr[i]));
                }
               combiner_count++;
            }
            for(int i = 0; i < d; i++)
            {
            	sb.append(sum[i]+",");
            }
            sb.append(combiner_count);
            context.write(key, new Text(sb.toString())); //Output of the combiner is the centroid's index and the sum of two points 
        }
    }
   
    /**
     * Reducer will take the centroid's index and the sum of points that are close to that centroid
     * and computes the new centroid and reports if there is any change in the centroid     
     */
    public static class KmeansReducer extends Reducer<IntWritable,Text,NullWritable,Text>
    {
            private List<List<Double>> centers = new ArrayList<List<Double>>();
            private int k;
            private int d;
            private float diff = 0.0f;
       
            //Read the centroids and store it in ArrayList as like the Mapper
            public void setup(Context context) throws IOException,InterruptedException
            {
                k = 0;
                Path[] caches = DistributedCache.getLocalCacheFiles(context.getConfiguration());
           
                if(caches==null||caches.length<=0) System.exit(1);
       
                BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));
                String line;
           
                while((line=br.readLine())!=null)
                {
                    centers.add(new ArrayList<Double>());
                    String[] str = line.split(",");
                    d = str.length;
           
                    for(int i=0;i<str.length;i++)
                    {
                        centers.get(k).add(Double.parseDouble(str[i]));
                    }
                    //Increment the position in the ArrayList to store the centers for the inital interation
                    k++;
                }
            try{
            br.close();
            }catch (Exception e){
            e.printStackTrace();
            }
            }

            public void reduce(IntWritable key,Iterable<Text> values,Context context)throws InterruptedException, IOException
            {
                double[] sum = new double[d];
                String tmp=null;
                int reducer_count = 0; k = 0;
                StringBuilder sb= new StringBuilder();
                while(values.iterator().hasNext())
                {
                    String line = values.iterator().next().toString();
                    String[] datastr = line.toString().split(","); 
               
                    for(int i = 0; i <d; i++)
                    {
                    	sum[i] +=Double.parseDouble(datastr[i]);
                    }
                    reducer_count+=Integer.parseInt(datastr[2].toString()); //Counting the number of points
                }
                
                for(int i = 0; i < d; i++)
                {
                	sum[i] = sum[i]/reducer_count;
                	 sb.append(sum[i]);
                	 if(i< d-1)
                		 sb.append(",");
                }
                diff=(float) (getDist(sum, centers.get(key.get()))) ;
                if(diff>0.01f){
                	tmp="changed";
                }
                else
                	tmp="unchanged";
                context.write(null,new Text(sb.toString()+","+tmp));
            }
            
            public void cleanup(Context context){
            	context.getCounter("Result", "Result").increment((long)(diff)*1000);
            }
    }
   
    public static float doIteration(int iterNum, String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        boolean flag = false;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try {
            DistributedCache.addCacheFile(new URI("/user/hadoop/input/Initial_K.txt"), conf);
        } catch (URISyntaxException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
           
        Job job = new Job(conf,"kmeans"+""+iterNum);
        job.setJarByClass(Kmeans.class);
        FileInputFormat.setInputPaths(job, "/user/hadoop/input/XYPoints.txt");
        Path outDir = new Path("/user/hadoop/input/output/");
        fs.delete(outDir,true);
        FileOutputFormat.setOutputPath(job, outDir);
       
        job.setMapperClass(KmeansMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(KmeansCombiner.class);
        job.setReducerClass(KmeansReducer.class);
        job.setNumReduceTasks(1);
       
       
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        flag= job.waitForCompletion(true);
        job.waitForCompletion(true);
        return (float) (job.getCounters().findCounter("Result", "Result").getValue()/1000);
    }
       
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration(true);
        boolean flag = true;
        FileSystem fs = FileSystem.get(conf);
        try {
                DistributedCache.addCacheFile(new URI("/user/hadoop/input/Initial_K.txt"), conf);
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
           
        while (flag && ITERNUM <= Kmeans.MAXITERATIONS)
        {
            String[] path = new String[3];
            path[0] = args[0];
           
            if (ITERNUM % 2 == 1)
            {
                path[2] = args[1] + out;
                path[1] = args[2];
                fs.delete(new Path(path[1]), true);
            }
            else
            {
                path[1] = args[1];
                path[2] = args[2] + out;
                fs.delete(new Path(path[1]), true);
            }
           
            float Diff = doIteration(ITERNUM, path);
           
            if (Diff < THRESHOLD) flag = false;
            ITERNUM++;
        }
    }
}

