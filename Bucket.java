public class Bucket {
	private int RECORD_SIZE;
	private boolean isFull;
	private int pageSize, depth, curDepth;
	private String[] data;
	public Bucket(int RECORD_SIZE, int pageSize, int depth)
	{
		this.RECORD_SIZE = RECORD_SIZE;
		this.pageSize = pageSize;
		isFull = false;
		this.depth = depth;
		curDepth = 0;
		data = new String[depth];
		for(int i = 0; i < data.length; i++)
		{
			data[i] = "";
		}
	}
	
	//Getters
	public String getData()
	{
		return data[curDepth-1];
	}
	
	public int getNumRecords()
	{
		return curDepth;
	}
	
	//Setters
	public void setNumRecords(int x)
	{
		curDepth = x;
	}
	
	public void write(int val, int Pages, int Records)
	{
		String s = "";
		s += val+","+(((Pages*pageSize) + (Records * RECORD_SIZE))) + "\r\n";
		data[curDepth] += s;
		curDepth++;
		updateFullStatus();
	}
	
	public void clearData()
	{
		data[curDepth -1] = null;
		data = null;
	}
	
	private void updateFullStatus()
	{
		if(curDepth == depth)
		{
			isFull = true;
		}
	}
	
	public boolean isFull()
	{
		return isFull;
	}
}
