import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
			while(!isFound)
			{
				while ((line = bufferedReader.readLine()) != null) 
				{
					//First token is an int, the rest is text
					String[] binaryTokens = line.split(" ", 2);
					String actualText = "" + Integer.parseInt(binaryTokens[0], 2);
					actualText += convertBinaryStringToString(binaryTokens[1]);
					String[] tokens = actualText.split("\\|");
					if(tokens[1].equals(name))
					{
						isFound = true;
						System.out.println(actualText);
					}
				}
				pageCounter++;
				fileReader.close();
				//Moving to the next file
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
	
	/*
		Credit to https://codereview.stackexchange.com/questions/88451/converting-a-binary-string-to-an-ascii-string-the-longer-way#answer-88459
		For the design of these two functions which enable the program to convert binary (string) data to readable string data
	*/
	public static String convertBinaryStringToString(String string)
	{
		StringBuilder sb = new StringBuilder();
		String[] blocks = string.split(" ");

		for (int i = 0; i < blocks.length; i++)
		{
			int result = convertBlock(blocks[i]);
			sb.append(Character.toChars(result));
		}
		
		return sb.toString();
	}

	private static int convertBlock(String block) 
	{
		int [] mapping = {128,64,32,16,8,4,2,1};
		int sum = 0;
		int blockPosition = block.length() - 1;
		while(blockPosition >0 )
		{
			if(block.charAt(blockPosition) == '1')
			{
				sum+=mapping[blockPosition];
			}
			blockPosition--;
		}
		return sum;
	}
}