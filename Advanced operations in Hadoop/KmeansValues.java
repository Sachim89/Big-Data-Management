package Asgmnt2;

import java.io.PrintWriter;
import java.util.Random;

/**
 * Create a dataset that consists of 2-dimenional points, i.e., each point has (x, y) values. X and Y values 
 * each range from 0 to 10,000. Each point is in a seperate line.
 * @author Rishitha, Saranya
 *
 */
public class Datasets {

	public static void dataset_intput(int k_initial)
	{
		int count = 1; int k = k_initial;
		float x;
		float y;
		
		try
		{
			PrintWriter Kwriter = new PrintWriter("/user/hadoop/input/Initial_K.txt", "UTF-8");
		
			while(count <= k)
			{
				x = new Random().nextFloat()*10000;
				y = new Random().nextFloat()*10000;
				Kwriter.println(x+","+y);
				count++;
			}
			Kwriter.close();
		
		count = 1;
		
		PrintWriter pointsWriter = new PrintWriter("/user/hadoop/input/XYPoints.txt", "UTF-8");
		
			while(count <= 5280000)
			{
				x = new Random().nextFloat()*10000;
				y = new Random().nextFloat()*10000;
				pointsWriter.println(x+","+y);
				count++;
			}
			pointsWriter.close();
		}
		catch (Exception e)
		{
		} 
	}
	
	public static void main(String[] args) 
	{
		dataset_intput(Integer.parseInt(args[0]));
	}
}

