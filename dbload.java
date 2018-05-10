import java.io.*;
import java.nio.ByteBuffer;

/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public class dbload implements dbimpl {
	// initialize
	public static void main(String args[]) {
		dbload load = new dbload();

		// calculate load time
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();

		System.out.println("Load time: " + (endTime - startTime) + "ms");
	}

	// reading command line arguments
	public void readArguments(String args[]) {
		if (args.length == 3) {
			if (args[0].equals("-p") && isInteger(args[1])) {
				readFile(args[2], Integer.parseInt(args[1]));
			}
		} else {
			System.out.println("Error: only pass in three arguments");
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

	// read .csv file using buffered reader
	public void readFile(String filename, int pagesize) {
		/*
		 * Variable Provided in startup code
		 */
		// dbload load = new dbload();
		BufferedReader br = null;
		String line = "";
		String stringDelimeter = "\t";
		byte[] RECORD = new byte[RECORD_SIZE];
		int recCount, totalPages;
		totalPages = 0;
		recCount = 0;

		// Container Variables
		Container[] containers = new Container[NUM_CONTAINERS];
		int hashIndex = -1;

		// Writing to Containers
		try {
			// create stream to write bytes to according page size
			// Initialise all the containers
			for (int i = 0; i < containers.length; i++) {
				containers[i] = new Container("ContainerData" + i + ".dat", RECORD_SIZE);
				containers[i].openOutputStream();
			}

			br = new BufferedReader(new FileReader(filename));
			// read line by line
			while ((line = br.readLine()) != null) {
				String[] entry = line.split(stringDelimeter, -1);
				hashIndex = dbimpl.getHash(entry[1]);
				// System.out.println(hashIndex);
				RECORD = createRecord(RECORD, entry, containers[hashIndex].getCurPageNumRecords());
				// outCount is to count record and reset everytime
				// the number of bytes has exceed the pagesize
				containers[hashIndex].setCurPageNumRecords(containers[hashIndex].getCurPageNumRecords() + 1);
				containers[hashIndex].writeData(RECORD);

				if ((containers[hashIndex].getCurPageNumRecords() + 1) * RECORD_SIZE > pagesize) {
					containers[hashIndex].eofByteAddOn(pagesize);
					// reset counter to start newpage
					containers[hashIndex].setCurPageNumRecords(0);
					containers[hashIndex].setNumPages(containers[hashIndex].getNumPages() + 1);
				}
				recCount++;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + filename + " not found.");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null && hashIndex != -1) {
				try {
					// final add on at end of file
					for (int i = 0; i < containers.length; i++) {
						totalPages += containers[i].getNumPages();
						containers[i].eofByteAddOn(pagesize);
						// reset counter to start newpage
						containers[i].setCurPageNumRecords(0);
						containers[i].setNumPages(containers[i].getNumPages() + 1);
						containers[i].closeOutputStream();
					}
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		// Writing to heap file
		try {
			/*
			 * Here I take advantage of knowing I will be running on an AWS
			 * Linux System Using Cat as opposed to reading my sub files in
			 * saves a huge amount of time reading all the data in file by file
			 * and then building the heap from that data
			 */
			String command = "cat ";
			for (int i = NUM_CONTAINERS - 1; i > -1; i--) {
				command += "ContainerData" + i + ".dat ";
			}
			command += "> " + HEAP_FNAME + pagesize;
			executeCommand(command);

			// Writes the data for the hash file
			BufferedWriter writer = new BufferedWriter(new FileWriter(hashIndexFile));
			writer.write("HashCode" + hashDelim + "NumFullPages");
			writer.newLine();
			for (int i = NUM_CONTAINERS - 1; i >= 0; i--) {
				writer.write(
						i + hashDelim + containers[i].getNumPages() + hashDelim + containers[i].getCurPageNumRecords());
				writer.newLine();
			}
			writer.close();

			// Deletes intermediary files used by containers
			if (!DEBUG) {
				for (int i = 0; i < containers.length; i++) {
					File file = new File("ContainerData" + i + ".dat");
					file.delete();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Page total: " + totalPages);
		System.out.println("Record total: " + recCount);
	}

	// create byte array for a field and append to record array at correct
	// offset using array copy
	public void copy(String entry, int SIZE, int DATA_OFFSET, byte[] rec) throws UnsupportedEncodingException {
		byte[] DATA = new byte[SIZE];
		byte[] DATA_SRC = entry.trim().getBytes(ENCODING);
		if (entry != "") {
			System.arraycopy(DATA_SRC, 0, DATA, 0, DATA_SRC.length);
		}
		System.arraycopy(DATA, 0, rec, DATA_OFFSET, DATA.length);
	}

	// creates record by appending using array copy and then applying offset
	// where neccessary
	public byte[] createRecord(byte[] rec, String[] entry, int out) throws UnsupportedEncodingException {
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

	// converts ints to a byte array of allocated size using bytebuffer
	public byte[] intToByteArray(int i) {
		ByteBuffer bBuffer = ByteBuffer.allocate(4);
		bBuffer.putInt(i);
		return bBuffer.array();
	}

	public void executeCommand(String command) throws IOException, InterruptedException {
		Runtime r = Runtime.getRuntime();
		String[] commands = { "bash", "-c", command };
		Process p = r.exec(commands);
		p.waitFor();
	}
}
