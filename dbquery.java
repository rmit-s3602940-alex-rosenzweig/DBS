import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class dbquery {
	
	public static void main(String[] args) {
		if(args.length != 2)
		{
			System.out.println("Incorrect Execution Arguments");
			return;
		}
		
		String name = args[0];
		int pageSize = Integer.parseInt(args[1]);
		
		if(name == null || pageSize <= 0)
		{
			System.out.println("Invalid Execution Arguments");
			return;
		}
		else
		{
			search(name, pageSize);
		}
	}
	
	public static void search(String name, int pageSize)
	{
		final int businessNamePosition = 0;
		String line;
		Boolean isFound = false;
		int pageCounter = 0;
		int loopCounter = 0;
		// based heavily on http://www.avajava.com/tutorials/lessons/how-do-i-read-a-string-from-a-file-line-by-line.html
		try 
		{
			File dataFile = new File("Heap."+pageSize);
			FileReader fileReader = new FileReader(dataFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			FileOutputStream outputStream = new FileOutputStream("Heap."+pageSize);
			while(!isFound)
			{
				while ((line = bufferedReader.readLine()) != null) 
				{
					String[] tokens = line.split(",");
					if(tokens[0].equals(name))
					{
						isFound = true;
						System.out.println(line);
					}
				}
				pageCounter++;
				fileReader.close();
				dataFile = new File("Heap."+pageSize+"."+pageCounter);
				fileReader = new FileReader(dataFile);
				bufferedReader = new BufferedReader(fileReader);
			}
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}