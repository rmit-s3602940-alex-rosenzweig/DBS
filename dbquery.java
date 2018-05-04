import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;

/**
 * Database Systems - HEAP IMPLEMENTATION
 */

public class dbquery implements dbimpl {
	// initialize
	public static void main(String args[]) {
		dbquery load = new dbquery();

		// calculate query time
		long startTime = System.currentTimeMillis();
		load.readArguments(args);
		long endTime = System.currentTimeMillis();

		System.out.println("Query time: " + (endTime - startTime) + "ms");
	}

	// reading command line arguments
	public void readArguments(String args[]) {
		if (args.length == 2) {
			if (isInteger(args[1])) {
				linearSearch(args[0], Integer.parseInt(args[1]));
			}
		} else if (args.length == 4) {
			if (isInteger(args[1]) && args[2].equals("-i")) {
				System.out.println(args[3]);
				SearchIndex(args[0], Integer.parseInt(args[1]), args[3]);
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

	/*
	 * Code for a index to speed up search
	 */
	public void SearchIndex(String name, int pagesize, String index) {

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
			int[] offsets = new int[2];
			try {
				offsets = seekAndSize(index, pagesize, fis);
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Error with reading hash index file");
			}
			// reading page by page
			while (isNextPage) {
				int tempRec = 0;
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
						tempRec++;
						// if recordLen exceeds pagesize, catch this to reset to
						// next page
					} catch (ArrayIndexOutOfBoundsException e) {
						isNextRecord = false;
						recordLen = 0;
						recCount = 0;
						rid = 0;
					}
					/*if (pageCount > offsets[0] && tempRec >= offsets[1]) {
						isNextRecord = false;
						isNextPage = false;
					}*/
				}
				// check to complete all pages
				if (ByteBuffer.wrap(bPageNum).getInt() != pageCount) {
					isNextPage = false;
				}
				pageCount++;
				tempRec = 0;
			}
		} catch (FileNotFoundException e) {
			System.out.println("File: " + HEAP_FNAME + pagesize + " not found.");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public int[] seekAndSize(String index, int pagesize, FileInputStream fis) throws IOException, NumberFormatException {
		int hashIndex = dbimpl.getHash(index);
		String line;
		BufferedReader br = new BufferedReader(new FileReader(hashIndexFile));
		int[] offsets = new int[2];
		// Skip the first header line
		br.readLine();
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(hashDelim);
			if (Integer.parseInt(tokens[0]) == hashIndex) {
				System.out.println("Here");
				// Adds the number of complete pages in container
				offsets[0] = Integer.parseInt(tokens[1]);
				// Adds the number of records in any incomplete page in
				// container
				offsets[1] = Integer.parseInt(tokens[2]);
				break;
			} else {
				// Adds the number of complete pages needed as offset
				for (int i = 0; i < Integer.parseInt(tokens[1]); i++) {
					byte[] temp = new byte[pagesize];
					fis.read(temp);
				}
				// Adds the number of records in any incomplete page needed as offset
				for (int i = 0; i < Integer.parseInt(tokens[2]); i++) {
					byte[] temp = new byte[RECORD_SIZE];
					fis.read(temp);
				}				
			}
		}
		br.close();
		return offsets;
	}

	/*
	 * Reads the heap file to find the location of the data and the size of said
	 * data block The first index relates 2 sets of data 0 is related to the
	 * offset to seek the container 1 is related to the size of the container
	 * 
	 * The second index also relates to 2 sets of data 0 is the number of
	 * complete pages in the relative to the dataset 1 is the number of records
	 * in any incomplete page relative to the dataset
	 */
	public int[][] getOffsets(String index) throws IOException, NumberFormatException {
		int hashIndex = dbimpl.getHash(index);
		String line;
		BufferedReader br = new BufferedReader(new FileReader(hashIndexFile));
		int[][] offsets = new int[2][2];
		// Skip the first header line
		br.readLine();
		while ((line = br.readLine()) != null) {
			String[] tokens = line.split(hashDelim);
			if (Integer.parseInt(tokens[0]) == hashIndex) {
				// Adds the number of complete pages in container
				offsets[1][0] += Integer.parseInt(tokens[1]);
				// Adds the number of records in any incomplete page in
				// container
				offsets[1][1] += Integer.parseInt(tokens[2]);
				break;
			} else {
				// Adds the number of complete pages needed as offset
				offsets[0][0] += Integer.parseInt(tokens[1]);
				// Adds the number of records in any incomplete page needed as
				// offset
				offsets[0][1] += Integer.parseInt(tokens[2]);
			}
		}
		br.close();
		return offsets;
	}

	// read heapfile by page
	public void linearSearch(String name, int pagesize) {
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
	public void printRecord(byte[] rec, String input) {
		String record = new String(rec);
		String BN_NAME = record.substring(RID_SIZE + REGISTER_NAME_SIZE, RID_SIZE + REGISTER_NAME_SIZE + BN_NAME_SIZE);
		if (BN_NAME.toLowerCase().contains(input.toLowerCase())) {
			System.out.println(record);
		}
	}
}
