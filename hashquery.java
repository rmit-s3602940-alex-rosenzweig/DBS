import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;

/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public class hashquery implements dbimpl {
	// initialize
	public static void main(String args[]) {
		hashquery load = new hashquery();

		// calculate query time
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();

		System.out.println("Query time: " + (endTime - startTime) + "ms");
	}

	// reading command line arguments
	public void readArguments(String args[]) {
		if (args.length == 2) { //Validates correct arguments
			if (isInteger(args[1])) {
				try {
					findRecord(args[0], Integer.parseInt(args[1]));
				} catch (NumberFormatException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			System.out.println("Error: only pass in two arguments");
		}
	}

	// check if pagesize is a valid integer
	public boolean isInteger(String s) {
		boolean isValidInt = false;
		try {
			Integer.parseInt(s);
			isValidInt = true;
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return isValidInt;
	}

	public void findRecord(String term, int pagesize) throws IOException {
		//Keeps track of the amount of elements found in current bucket
		int counter = 0;
		boolean found = false;
		//String line = "";
		// Reader for the hash index
		BufferedReader br = new BufferedReader(new FileReader(hashIndexFile+pagesize));

		// Reader for heap file
		RandomAccessFile fis = new RandomAccessFile((HEAP_FNAME + pagesize), "rw");
		int hash = dbimpl.getHash(term);

		for(String line; (line = br.readLine()) != null; ) {
			if(line.startsWith("\0"))
			{
				continue;
			}
	        // process the line.
	    	// Stores data of hash index entry
			String[] temp = line.split(",");
			// Checks for a match for initial hash
			if (Integer.parseInt(temp[0]) == hash) 
			{
				// Skips to the relevant spot
				fis.seek(Integer.parseInt(temp[1]));
				byte[] record = new byte[RECORD_SIZE];
				fis.read(record, 0, RECORD_SIZE);
				found = printRecord(record, term);				
				if(found || counter == BUCKET_DEPTH)//Term doesn't match to this particular hash index
				{
					break;
				}
				counter++;
			}
			if(Integer.parseInt(temp[0]) > hash)
			{
				break;
			}

		}
		//Close the files we opened
		//Don't want memory leaks
		fis.close();
		br.close();
		if(!found)//If it doesn't find the term it begins the double hash search
		{
			found = search(term, pagesize);			
		}
	}
	
	public boolean search(String term, int pagesize) throws NumberFormatException, IOException
	{
		boolean found = false;
		//Holds the line data
		String line;
		//Keeps track of the amount of elements found in current bucket
		int counter = 0;
		int i = 1;
		//Getting base variables for double hash function
		int offset = dbimpl.hashFunction2(term);
		int hash = dbimpl.getHash(term);
		// Reader for the hash index
		BufferedReader br;

		// Reader for heap file
		RandomAccessFile fis = new RandomAccessFile((HEAP_FNAME + pagesize), "rw");
		while(!found)
		{
			//Initialises the buffered reader
			br = new BufferedReader(new FileReader(hashIndexFile + pagesize));
			
			//Gets the value of the double hash
			int hashCode = (hash + i * offset) % NUM_BUCKETS;
			if(hashCode == hash)//Value isn't in heap file
			{
				br.close();
				fis.close();
				return false;
			}
			//Reads Hash index line by line
			while ((line = br.readLine()) != null) {
				if(line.startsWith("\0"))
				{
					continue;
				}
				// Stores data of hash index entry
				String[] temp = line.split(",");
				// Checks for a match with the hash
				if (Integer.parseInt(temp[0]) == hashCode)
				{
					// Skips to the relevant spot
					fis.seek(Integer.parseInt(temp[1]));
					byte[] record = new byte[RECORD_SIZE];
					//reads the record into the byte array
					fis.read(record, 0, RECORD_SIZE);
					
					//Attempts to print the record
					found = printRecord(record, term);
					counter++;
					
					//Early termination optimisation
					if(found || counter == BUCKET_DEPTH)
					{
						//Closes the buffered reader if we terminate early
						br.close();
						break;
					}
				}
				//Skips because we have already read all we need
				//From the index (safety measure for non-full buckets)
				if(Integer.parseInt(temp[0]) > hashCode)
				{
					break;
				}
			}
			counter = 0;
			i++;
			//Close the buffered reader, we don't want memory leaks
			br.close();
		}
		//Close the heap file
		fis.close();
		return found;
	}

	// read heapfile by page (No longer used, Halil's code)
	public void readHeap(String name, int pagesize) {
		File heapfile = new File(HEAP_FNAME + pagesize);
		int intSize = 4;
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		try {
			FileInputStream fis = new FileInputStream(heapfile);
			// reading page by page
			while (isNextPage) {
				byte[] bPage = new byte[pagesize];
				byte[] bPageNum = new byte[intSize];
				fis.read(bPage, 0, pagesize);
				System.arraycopy(bPage, bPage.length - intSize, bPageNum, 0, intSize);

				// reading by record, return true to read the next record
				isNextRecord = true;
				while (isNextRecord) {
					byte[] bRecord = new byte[RECORD_SIZE];
					byte[] bRid = new byte[intSize];
					try {
						System.arraycopy(bPage, recordLen, bRecord, 0, RECORD_SIZE);
						System.arraycopy(bRecord, 0, bRid, 0, intSize);
						rid = ByteBuffer.wrap(bRid).getInt();
						if (rid != recCount) {
							isNextRecord = false;
						} else {
							printRecord(bRecord, name);
							recordLen += RECORD_SIZE;
						}
						recCount++;
						// if recordLen exceeds pagesize, catch this to reset to
						// next page
					} catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = 0;
						recCount = 0;
						rid = 0;
					}
				}
				// check to complete all pages
				if (ByteBuffer.wrap(bPageNum).getInt() != pageCount) {
					isNextPage = false;
				}
				pageCount++;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	// returns records containing the argument text from shell
	//Converted to boolean so we know if it printed or not
	public boolean printRecord(byte[] rec, String input) {
		String record = new String(rec);
		String BN_NAME = record.substring(RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE);
		//If the record matches print the record
		if (BN_NAME.toLowerCase().contains(input.toLowerCase())) {
			System.out.println(record);
			return true;
		}
		//Not a match
		return false;
	}
}