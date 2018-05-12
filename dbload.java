import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;

/**
 *  Database Systems - HEAP IMPLEMENTATION
 */

public class dbload implements dbimpl
{
    // initialize
   public static void main(String args[])
   {
      dbload load = new dbload();

      // calculate load time
      long startTime = System.currentTimeMillis();
      load.readArguments(args);
      long endTime = System.currentTimeMillis();

      System.out.println("Load time: " + (endTime - startTime) + "ms");
   }

   // reading command line arguments
   public void readArguments(String args[])
   {
      if (args.length == 3)
      {
         if (args[0].equals("-p") && isInteger(args[1]))
         {
            readFile(args[2], Integer.parseInt(args[1]));
         }
      }
      else
      {
         System.out.println("Error: only pass in three arguments");
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

   // read .csv file using buffered reader
   public void readFile(String filename, int pagesize)
   {
	  /*
	   * Variable Provided in startup code
	   */
      File heapfile = new File(HEAP_FNAME + pagesize);
      BufferedReader br = null;
      FileOutputStream fos = null, hashIndex = null;
      String line = "";
      String nextLine = "";
      String stringDelimeter = "\t";
      byte[] RECORD = new byte[RECORD_SIZE];
      int outCount, pageCount, recCount;
      outCount = pageCount = recCount = 0;
      
      
      //Bucket Variables  
      boolean[] bucketStatus = new boolean[NUM_BUCKETS];
      for(int i = 0; i < NUM_BUCKETS; i++)
      {
    	  bucketStatus[i] = false;
      }
      try
      {
         // create stream to write bytes to according page size
    	 hashIndex = new FileOutputStream(hashIndexFile);
         fos = new FileOutputStream(heapfile);
         br = new BufferedReader(new FileReader(filename));
         // read line by line
         while ((line = br.readLine()) != null)
         {
        	
            String[] entry = line.split(stringDelimeter, -1);
            int hashCode = dbimpl.getHash(entry[1]);
            if(!bucketStatus[hashCode])
            {
            	bucketStatus[hashCode] = true;
            	String s = hashCode+","+(((pageCount*pagesize) + (outCount * RECORD_SIZE))) + "\r\n";
            	hashIndex.write(s.getBytes());
            	hashIndex.flush();
            }
            else
            {
            	int offset = dbimpl.hashFunction2(entry[1]);
            	int i = 1;
            	while(true)
            	{
            		
	            	int newIndex = (hashCode + i * offset) % NUM_BUCKETS;
	            	//Implement Double Hashing
	            	if(!bucketStatus[newIndex])
	            	{
	            		if(entry[1].equals("NewGenTronics"))
	            		{
	            			System.out.println("i: "+i+" hashCode: "+newIndex);
	            		}
	            		bucketStatus[newIndex] = true;
	                	String s = newIndex+","+(((pageCount*pagesize) + (outCount * RECORD_SIZE))) + "\r\n";
	                	hashIndex.write(s.getBytes());
	            		hashIndex.flush();
	            		/*if(i == 1)
	            		{
	            			System.out.println(entry[1]);
	            		}*/
	            		//System.out.println("here");
	            		break;
	            	}
	            	if(newIndex == hashCode)
	            	{
	            		System.out.println("Error");
	            		return;
	            	}
	            	i++;
            	}
            }
            RECORD = createRecord(RECORD, entry, outCount);
            // outCount is to count record and reset everytime
            // the number of bytes has exceed the pagesize
            outCount++;
            fos.write(RECORD);
            if ((outCount+1)*RECORD_SIZE > pagesize)
            {
               eofByteAddOn(fos, pagesize, outCount, pageCount);
               //reset counter to start newpage
               outCount = 0;
               pageCount++;
            }
            recCount++;
         }
      }
      catch (FileNotFoundException e)
      {
         System.out.println("File: " + filename + " not found.");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (br != null)
         {
            try
            {
               // final add on at end of file
               if ((nextLine = br.readLine()) == null)
               {
                  eofByteAddOn(fos, pagesize, outCount, pageCount);
                  pageCount++;
               }
               fos.close();
               hashIndex.flush();
               hashIndex.close();
               br.close();
            }
            catch (IOException e)
            {
               e.printStackTrace();
            }
         }
      }
      System.out.println("Page total: " + pageCount);
      System.out.println("Record total: " + recCount);
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

   // creates record by appending using array copy and then applying offset
   // where neccessary
   public byte[] createRecord(byte[] rec, String[] entry, int out)
          throws UnsupportedEncodingException 
   {
      byte[] RID = intToByteArray(out);
      System.arraycopy(RID, 0, rec, 0, RID.length);

      copy(entry[0], REGISTER_NAME_SIZE, RID_SIZE, rec);

      copy(entry[1], BN_NAME_SIZE, BN_NAME_OFFSET, rec);

      copy(entry[2], BN_STATUS_SIZE, BN_STATUS_OFFSET, rec);

      copy(entry[3], BN_REG_DT_SIZE, BN_REG_DT_OFFSET, rec);

      copy(entry[4], BN_CANCEL_DT_SIZE, BN_CANCEL_DT_OFFSET, rec);

      copy(entry[5], BN_RENEW_DT_SIZE, BN_RENEW_DT_OFFSET, rec);

      copy(entry[6], BN_STATE_NUM_SIZE, BN_STATE_NUM_OFFSET, rec);

      copy(entry[7], BN_STATE_OF_REG_SIZE, BN_STATE_OF_REG_OFFSET, rec);

      copy(entry[8], BN_ABN_SIZE, BN_ABN_OFFSET, rec);

      return rec;
   }

   // EOF padding to fill up remaining pagesize
   // * minus 4 bytes to add page number at end of file
   public void eofByteAddOn(FileOutputStream fos, int pSize, int out, int pCount) 
          throws IOException
   {
      byte[] fPadding = new byte[pSize-(RECORD_SIZE*out)-4];
      byte[] bPageNum = intToByteArray(pCount);
      fos.write(fPadding);
      fos.write(bPageNum);
   }

   // converts ints to a byte array of allocated size using bytebuffer
   public byte[] intToByteArray(int i)
   {
      ByteBuffer bBuffer = ByteBuffer.allocate(4);
      bBuffer.putInt(i);
      return bBuffer.array();
   }
}