import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Random;

public class dataset {
   
    public static void main(String[] args) {
        dataset ds= new dataset();
       
         
        PrintWriter writer1;
        try {
            writer1 = new PrintWriter("P.txt","UTF-8");
       
        for (int i=0; i<110000; i++){
            int x= ds.randInt(1, 50);
            int y= ds.randInt(1, 50);
            String point=""+x+","+y;
            if (i<11000000) writer1.println(point);
        }
        writer1.close();
       
        PrintWriter writer2= new PrintWriter("R.txt","UTF-8");
        for (int i=0; i<80000; i++){
            int top_x= ds.randInt(1, 50);
            int top_y= ds.randInt(1, 50);   
            int h= ds.randInt(1, 20);
            int w= ds.randInt(1, 5);
            String rectangle="R"+i+","+top_x+","+top_y+","+h+","+w;
            if (i<8000000) writer2.println(rectangle);
           
        }
        writer2.close();
        System.out.println("done");
    } catch (FileNotFoundException | UnsupportedEncodingException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }
    }
    private Random rn= new Random();
   
    public int randInt(int min, int max){
        int random_num=rn.nextInt(max-min+1)+  min;
        return random_num;
    }
   

}
