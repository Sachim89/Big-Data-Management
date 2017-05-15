import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Spatial join of points and rectangles.
 * @author Saranya, Rishitha
 *
 */
public class Spatialjoin1 {
   
    static HashMap<String,String> rectDetailsMap =  new HashMap<String,String>();
    static HashMap<String, List<String>> rect_result = new HashMap<String, List<String>>();
    static Text output = new Text();
   
    /**
     * Mapper should calculate which points lie within the window and the output of the mapper
     * is key and the points within it.
     */
    public static class SpatialJoinMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        String window_size;
       
        /**Gets the window point if given or else assigns to default overall window size*/
        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
                Configuration conf = context.getConfiguration();       
                if(conf.get("flag") != "user")
                {
                    window_size = conf.get("window");
                }
               
                else
                {
                   String x = conf.get("x1").toString();
                    String y = conf.get("y1").toString();
                    String x1 = conf.get("x2").toString();
                    String x2 = conf.get("y2").toString();
                    Text window = new Text();
                    window.set(x+","+y+","+x1+","+x2);
                    conf.set("window_user", window.toString());
                    window_size = conf.get("window_user");
                   }
        }
       
        protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException
        {
            //Get the window size and assign to the text
            Text window_values = new Text();
            window_values.set(window_size.toString());
           
            Text hvalue = new Text();
           
            //Split the window value for calculating the points within the window
            Integer wx1 = Integer.parseInt(window_values.toString().split(",")[0]);
            Integer wy1 = Integer.parseInt(window_values.toString().split(",")[1]);
            Integer wx2 = Integer.parseInt(window_values.toString().split(",")[2]);
            Integer wy2 = Integer.parseInt(window_values.toString().split(",")[3]);
           
            /**
             * Determine whether the points are within the window size.
             * if W(1,3,3,20) then point P1:<x1,y1>(2,4) i within the window.
             * So we calculate (x1 >= wx1 && y1 >=wy1 && x1<=wx2 && y1<=wy2)
             */
            String[] tokens = value.toString().split(",");
            int[] points = new int[tokens.length];
           
            for(int j = 0; j < points.length; j++)
            {
                points[j] = Integer.parseInt(tokens[j]);
            }
            for(int j = 0; j < points.length-1; j++)
            {
                if(points[j] >= wx1 && points[j+1] >= wy1 && points[j] <= wx2 && points[j+1]<=wy2)
                {
                    hvalue.set(points[j]+","+points[j+1]);
                }
            }
            Text wvalue = new Text();
            wvalue.set(wx1+","+wy1);
           
            //Output is points within the window
            context.write(wvalue, hvalue);   
        }
    }
   
    /**
     * Reducer should associate those mapper points with the related rectangles.
     * The output of the reducer is rectangles and points within the rectangle and window.
     */
    public static class SpatialJoinReducer extends Reducer<Text,Text,Text,Text>
    {
        int x = 0; int y = 0; int height = 0; int width = 0;
        HashMap<String,String> rectHashMap =  new HashMap<String,String>();
        String rectNo = null; Text rectDetails = new Text();
       
        /** Reading the rectangle points and storing it in HashMap*/
        @Override
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
            {
                String [] tokens = setupData.split(",");
                rectNo = String.valueOf(tokens[0]);
                x = Integer.parseInt(tokens[1]);
                y = Integer.parseInt(tokens[2]);
                height = Integer.parseInt(tokens[3]);
                width = Integer.parseInt(tokens[4]);
                rectDetails.set(x+","+y+","+height+","+width);
                rectHashMap.put(rectNo, rectDetails.toString());
            }
            try{
            bf.close();
            }catch (Exception e){
            e.printStackTrace();
            }
        }
       
        /**
         * Reducer reads the rectangle points from the HashMap and calculates the other points with height and width.
         * And finds the points within the rectangle. The output of the reducer is rectangle number and the points.
         */
        protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException
        {       
            Text rect = new Text(); int param_x = 0; int param_y = 0;
            int rx1; int ry1; String points_in_rect = null; String key_in_rect = null;
           
            for(Map.Entry<String, String> entry : rectHashMap.entrySet())
            {
                key_in_rect = entry.getKey();
                points_in_rect = entry.getValue();
                String[] rectTokens = points_in_rect.split(",");
                int[] rect_points = new int[rectTokens.length];               
                for(int j = 0; j < rectTokens.length; j++)
                {
                    rect_points[j] = Integer.parseInt(rectTokens[j]);
                }   
               
                //Calculation of other points
                rx1 = rect_points[0] + rect_points[3];
                ry1 = rect_points[1] + rect_points[2];
                rect.set(rect_points[0]+","+rect_points[1]+","+rx1+","+ry1);
                rectDetailsMap.put(key_in_rect.toString(), rect.toString());      
            }
           
            //Reading the mapper output of points within the window
            while(value.iterator().hasNext())
            {
                for( Text t : value)
                {       
                      String[] tokens = t.toString().split(",");
                        param_x = Integer.parseInt(tokens[0].toString());
                        param_y = Integer.parseInt(tokens[1].toString());
                         
                        //For each point find whether it is within the rectangle
                        inRectangle(param_x, param_y);
                       
                        List<String> points_final = null; String key_final = null;
                        for(Entry<String, List<String>> results : rect_result.entrySet())
                        {
                            key_final = results.getKey();
                            points_final = results.getValue();
                           if (points_final != null && !points_final.isEmpty())
                             {
                                 String[] phase1 = points_final.toString().split("(\\s)|(\\[)|(\\])");
                                 for(int h = 0 ; h < phase1.length ; h++)
                                {
                                     if(!(phase1[h].toString().isEmpty()))
                                     {
                                         context.write(null, new Text(phase1[h].toString()));
                                     }
                                     else
                                         continue;
                                 }
                               
                        }   
                    }
                }
            }
        }
    }

    /**
     * For given XY point find whether it is within or on the rectangle and output the rectangle and the associated points.
     */
    public static void inRectangle(int x, int y)
    {
        int[] win_rect_pts = null; List<String> result = new ArrayList<String>();
        String[] new_rect_points = null; String points_in_rect = null; String key_in_rect = null;
       
        for(Map.Entry<String, String> entry1 : rectDetailsMap.entrySet())
        {
            key_in_rect = entry1.getKey();
            points_in_rect = entry1.getValue();
   
            new_rect_points = points_in_rect.split(",");
            win_rect_pts = new int[new_rect_points.length];
           
            for(int j = 0; j < new_rect_points.length; j++)
            {
                win_rect_pts[j] = Integer.parseInt(new_rect_points[j]);
            }
           
            int rec_x = win_rect_pts[0];
            int rec_y = win_rect_pts[1];
            int rec_x1 = win_rect_pts[2];
            int rec_y1 = win_rect_pts[3];
           
            if(x >= rec_x && y >= rec_y
                    && x <= rec_x1 && y <= rec_y1)
            {
                output.set("<"+key_in_rect+",("+x+","+y+")>");
                result.add(new String(output.toString()));   
            }   
        }
        rect_result.put(key_in_rect, result);
    }
   
   
    /**Determine the window size*/
    public static void main(String[] args)
    {
        Configuration conf = new Configuration();
        //If the user did not specify the window size
        if (args.length != 6)
        {
            conf.set("window", "0,0,100,100");
        }
        //If the user specified the window size, read the values
        else
        {
            conf.set("x1", args[2]);
            conf.set("y1", args[3]);
            conf.set("x2", args[4]);
            conf.set("y2", args[5]);
            conf.set("flag", "user");
        }
        try
        {
            DistributedCache.addCacheFile(new URI("/home/hadoop/workspace/big_data2/R.txt"), conf);
            Job job = new Job(conf, "SpatialJoin");
           
            job.setJarByClass(Spatialjoin1.class);
               
            job.setMapperClass(SpatialJoinMapper.class);
            job.setReducerClass(SpatialJoinReducer.class);
           
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
           
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | URISyntaxException | ClassNotFoundException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
