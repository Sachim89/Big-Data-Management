package Query;
import java.io.PrintWriter;
import java.util.Random;

public class Datasets {
public static void main(String args[])throws Exception
	{
		int i;
		PrintWriter writer = new PrintWriter("/user/hadoop/input/Customers.txt", "UTF-8");

		for (i=1; i<=50000; i++)
		{
			Random random = new Random();
			StringBuilder Name = new StringBuilder();
			
			int age = random.nextInt(70 - 10 + 1) + 10;
			int country_code = random.nextInt(10 - 1 + 1) + 1;
			float salary = random.nextFloat() * (10000.0f - 100.0f) + 100.0f;
			int length = random.nextInt(20-10) + 10;

			for(int j = 1; j<= length; j++)
			{
				char c = (char)(random.nextInt(26) + 'a');
				Name.append(c);
			}

			writer.println(i +","+Name+"," + age+","+country_code+","+ salary);
		}
		writer.close();
		
		PrintWriter writer1 = new PrintWriter("/user/hadoop/input/Transactions.txt", "UTF-8");
			
		for (i=1; i<=5000000;i++)
		{
			Random random = new Random();
			StringBuilder TransDesc = new StringBuilder();
			
			int CustId = random.nextInt(50000- 1 + 1) + 1;
			float TransTotal = random.nextFloat() * (1000.0f - 10.0f) + 10.0f;
			int TransNumItems = random.nextInt(10 - 1 + 1) + 1;
			int length = random.nextInt(50-20) + 20;

			for(int j = 1; j<= length; j++)
			{
				char c = (char)(random.nextInt(26) + 'a');
				TransDesc.append(c);
			}
			writer1.println(i +"," + CustId+","+TransTotal+","+ TransNumItems+","+TransDesc);
		}
		writer1.close();   
	}
}
