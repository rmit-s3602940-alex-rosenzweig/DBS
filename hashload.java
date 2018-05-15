import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;

/**
 *  Database Systems - HEAP IMPLEMENTATION
 */

public class hashload implements dbimpl
{
    // initialize
   public static void main(String args[])
   {
	   hashload load = new hashload();

      // calculate load time
      long startTime = System.currentTimeMillis();
      load.readArguments(args);
      long endTime = System.currentTimeMillis();

      System.out.println("Load time: " + (endTime - startTime) + "ms");
   }

   // reading command line arguments
   public void readArguments(String args[])
   {
	   //Validates correct number of arguments
      if (args.length == 1)
      {
         if (isInteger(args[0]))
         {
        	 try {
				readHeap(Integer.parseInt(args[0]));
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Error building hash index base");
				e.printStackTrace();
			}
         }
      }
      else //Tells user what to do if they did the wrong thing
      {
         System.out.println("Error: only pass in one argument");
      }
   }

   // check if pagesize is a valid integer
   public boolean isInteger(String s)
   {
      boolean isValidInt = false;
      try
      {
         Integer.parseInt(s);
         isValidInt = true;
      }
      catch (NumberFormatException e)
      {
         e.printStackTrace();
      }
      return isValidInt;
   }
   // create byte array for a field and append to record array at correct 
   // offset using array copy
   public void copy(String entry, int SIZE, int DATA_OFFSET, byte[] rec)
          throws UnsupportedEncodingException
   {
      byte[] DATA = new byte[SIZE];
      byte[] DATA_SRC = entry.trim().getBytes(ENCODING);
      if (entry != "")
      {
         System.arraycopy(DATA_SRC, 0,
                DATA, 0, DATA_SRC.length);
      }
      System.arraycopy(DATA, 0, rec, DATA_OFFSET, DATA.length);
   }
   
	// read heapfile by page (No longer used, Halil's code)
	public void readHeap(int pagesize) throws IOException {
		fillHashIndex(pagesize);
		RandomAccessFile hashIndex = new RandomAccessFile((hashIndexFile + pagesize), "rw");
		File heapfile = new File(HEAP_FNAME + pagesize);
		int intSize = 4;
		int pageCount = 0;
		int recCount = 0;
		int recordLen = 0;
		int rid = 0;
		boolean isNextPage = true;
		boolean isNextRecord = true;
		
		//Bucket Variables  
	    int[] bucketStatus = new int[NUM_BUCKETS];
	    for(int i = 0; i < NUM_BUCKETS; i++)
	    {
	    	bucketStatus[i] = 0;
	    }
		
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
							if(!writeIndex(bRecord, bucketStatus, pagesize, pageCount, recCount, hashIndex))
							{
			            		System.out.println("Error: To many records");
								return;
							}
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
			fis.close();
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Fills up the hash index file with null characters
	public void fillHashIndex(int pagesize) throws IOException
	{
		FileOutputStream hashIndex = new FileOutputStream(hashIndexFile + pagesize);
		String nullData = "";
		for(int i = 0; i < hashIndexLength; i++)
		{
			nullData += '\0';
		}
		nullData += "\r\n";
		for(int i = 0; i < NUM_BUCKETS * BUCKET_DEPTH; i++)
		{
			hashIndex.write(nullData.getBytes());
		}
		hashIndex.close();
	}
	

	// returns records containing the argument text from shell
	//Converted to boolean so we know if it printed or not
	public boolean writeIndex(byte[] rec, int[] bucketStatus, int pagesize,  int pageCount,
		int recCount, RandomAccessFile hashIndex) throws IOException {
		String record = new String(rec);
		String BN_NAME = record.substring(RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE);

		
		String realStr = "";
		char[] x = BN_NAME.toCharArray();
		//Removes null characters from string
		for(int i = 0; i < BN_NAME.length(); i++)
		{
			if(x[i] == '\0')
			{
				break;
			}
			realStr += x[i];
		}
		
		//Gets basic hash value
		int hashCode = dbimpl.getHash(realStr);
		
		//Check for space in current bucket
        //If bucket has space we write to it
        if(bucketStatus[hashCode] != BUCKET_DEPTH) 
        {
        	String s = hashCode+","+(((pageCount*pagesize) + (recCount * RECORD_SIZE))) + "\r\n";
        	//Write to hash index file
        	hashIndex.seek((hashIndexLength + lineBreakSize) * (bucketStatus[hashCode] + (hashCode * BUCKET_DEPTH)));
        	//Writes data to correct spot in hash index
        	hashIndex.write(s.getBytes());
        	//Updates our tracker of bucket space
        	bucketStatus[hashCode]++;
        }
        else //Otherwise we search for open buckets using double hashing
        {
        	//Second hash function
        	int offset = dbimpl.hashFunction2(realStr);
        	int i = 1;
        	while(true)
        	{            		
            	int newIndex = (hashCode + i * offset) % NUM_BUCKETS;
            	//Implement Double Hashing
            	if(bucketStatus[newIndex] != BUCKET_DEPTH)
            	{
            		//Writes data to hash index
                	String s = newIndex+","+(((pageCount*pagesize) + (recCount * RECORD_SIZE))) + "\r\n";
                	//Writes data to correct spot in hash index
                	hashIndex.seek((hashIndexLength + lineBreakSize) * (bucketStatus[newIndex]+ (newIndex * BUCKET_DEPTH)));
                	hashIndex.write(s.getBytes());
                	//Updates our tracker of bucket space
                	bucketStatus[newIndex]++;
            		break;
            	}
            	if(newIndex == hashCode)//Not space left in any bucket
            	{
            		return false;
            	}
            	i++;
        	}
        }
		//Not a match
		return true;
	}
	
   // converts ints to a byte array of allocated size using bytebuffer
   public byte[] intToByteArray(int i)
   {
      ByteBuffer bBuffer = ByteBuffer.allocate(4);
      bBuffer.putInt(i);
      return bBuffer.array();
   }
}