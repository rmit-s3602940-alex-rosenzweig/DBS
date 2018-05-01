import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Container {
	private String fileName;
	private int pages, curPageNumRecords, RECORD_SIZE;
	private FileOutputStream fos;
	public Container(String fileName, int RECORD_SIZE)
	{
		this.fileName = fileName;
		this.RECORD_SIZE = RECORD_SIZE;
		pages = 0;
		curPageNumRecords = 0;
	}
	
	//Getters
	public int getCurPageNumRecords()
	{
		return curPageNumRecords;
	}
	
	public int getNumPages()
	{
		return pages;
	}
	
	//Setters
	public void setCurPageNumRecords(int x)
	{
		curPageNumRecords = x;
	}
	
	public void setNumPages(int x)
	{
		pages = x;
	}
	
	//Open And Close Output Stream
	public void openOutputStream() throws FileNotFoundException
	{
		fos = new FileOutputStream(fileName);
	}
	
	public void closeOutputStream() throws IOException
	{
		fos.close();
	}
	
	//Writes data to temp file
	public void writeData(byte[] data) throws IOException
	{
		fos.write(data);
	}
	
	 // EOF padding to fill up remaining pagesize
	 // * minus 4 bytes to add page number at end of file
	 public void eofByteAddOn(int pSize) 
			 throws IOException
	 {
	    byte[] fPadding = new byte[pSize-(RECORD_SIZE*curPageNumRecords)-4];
	    byte[] bPageNum = intToByteArray(pages);
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
