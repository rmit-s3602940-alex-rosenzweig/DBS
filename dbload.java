import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

public class dbload {

	public static void main(String[] args) {
		if(args.length != 3)
		{
			System.out.println("Incorrect Execution Arguments");
			return;
		}
		
		String dataFile = null;
		int pageSize = 0;
		
		for (int i = 0; i < args.length; i++)
		{
			if(args[i].equals("-p"))
			{
				i++;
				pageSize = Integer.parseInt(args[i]);
				continue;
			}
			dataFile = args[i];
		}
		if(dataFile == null || pageSize <= 0)
		{
			System.out.println("Invalid Execution Arguments");
			return;
		}
		else
		{
			read(dataFile, pageSize);
		}
	}
	public static void read(String fileName, int pageSize)
	{
		String line;
		String data = "";
		int pageCounter = 0;
		int loopCounter = 0;
		// based heavily on http://www.avajava.com/tutorials/lessons/how-do-i-read-a-string-from-a-file-line-by-line.html
		try 
		{
			File dataFile = new File(fileName);
			FileReader fileReader = new FileReader(dataFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			FileOutputStream outputStream = new FileOutputStream("Heap."+pageSize);
			while ((line = bufferedReader.readLine()) != null) {
				//Tokenize and serve up string to heap file
				if(loopCounter == 0)//Skip first iteration
				{
					loopCounter++;
					continue;					
				}
				
				if((data.getBytes().length) + (line.getBytes().length * 2) > pageSize)
				{
					byte[] writeData = data.getBytes();
					outputStream.write(writeData);
					outputStream.close();
					pageCounter++;
					outputStream = new FileOutputStream("Heap."+pageSize+"."+pageCounter);
					data = "";
				}
				
				String[] tokens = line.split("\t");
				
				for (int i = 1; i < tokens.length; i++)
				{
					if((i+1) == tokens.length)
					{
						data += tokens[i];
						continue;
					}
					data += tokens[i] + ",";
				}
				data += "\n";
				if(loopCounter == 500)
				{
					break;
				}
				loopCounter++;
			}
			fileReader.close();
			System.out.println("Contents of file:");
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}
}
