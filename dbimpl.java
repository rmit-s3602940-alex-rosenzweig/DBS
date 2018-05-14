/**
 *  Database Systems - HEAP IMPLEMENTATION
 */

public interface dbimpl
{

   public static final String HEAP_FNAME = "heap.";
   public static final String ENCODING = "utf-8";

   // fixed/variable lengths
   public static final int RECORD_SIZE = 297;
   public static final int RID_SIZE = 4;
   public static final int REGISTER_NAME_SIZE = 14;
   public static final int BN_NAME_SIZE = 200;
   public static final int BN_STATUS_SIZE = 12;
   public static final int BN_REG_DT_SIZE = 10;
   public static final int BN_CANCEL_DT_SIZE = 10;
   public static final int BN_RENEW_DT_SIZE = 10;
   public static final int BN_STATE_NUM_SIZE = 10;
   public static final int BN_STATE_OF_REG_SIZE = 3;
   public static final int BN_ABN_SIZE = 20;
   public static final int EOF_PAGENUM_SIZE = 4;
   public static final int BN_NAME_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE;

   public static final int BN_STATUS_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE;

   public static final int BN_REG_DT_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE;

   public static final int BN_CANCEL_DT_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE;

   public static final int BN_RENEW_DT_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE;

   public static final int BN_STATE_NUM_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE
                           + BN_RENEW_DT_SIZE;

   public static final int BN_STATE_OF_REG_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE
                           + BN_RENEW_DT_SIZE
                           + BN_STATE_NUM_SIZE;

   public static final int BN_ABN_OFFSET = RID_SIZE
                           + REGISTER_NAME_SIZE
                           + BN_NAME_SIZE
                           + BN_STATUS_SIZE
                           + BN_REG_DT_SIZE
                           + BN_CANCEL_DT_SIZE
                           + BN_RENEW_DT_SIZE
                           + BN_STATE_NUM_SIZE
                           + BN_STATE_OF_REG_SIZE;
   
   
   //name of the file that the hash index data is written too
   public static final String hashIndexFile = "hashIndex.dat";
   //Delimeter for all the data stored in the hash index file
   public static final String hashDelim = ",";

   //Determines the Number of Container Slots
   //I have set this to 64 but will change for information used in report
   public static final int NUM_BUCKETS = 600000;
   //Static depth of buckets (num record associations it can hold)
   public static final int BUCKET_DEPTH = 8;
   //Boolean that allows the user to see the
   //Intermediary files used to temporarily hold data
   public static final boolean DEBUG = false;
   
   public void readArguments(String args[]);

   public boolean isInteger(String s);
   
   //New Function to computer hash
   public static int getHash(String s)
   {
	   //Gets the hash code for each string
	   //Returns the absolute value
	   //Distribution should be even as the hash code is pseudo random
	   return Math.abs(((438439 * s.hashCode()) + 34723753) %  376307) % NUM_BUCKETS;
   }
   
   //New function to provide offset for double hashing
   public static int hashFunction2(String s)
   {
	   //Can never be 0
	   return 1 + (Math.abs((s.hashCode()/NUM_BUCKETS)) % (NUM_BUCKETS-1));
   }
   

}
