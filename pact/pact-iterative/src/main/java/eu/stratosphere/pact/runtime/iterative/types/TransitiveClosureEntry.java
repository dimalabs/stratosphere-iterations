package eu.stratosphere.pact.runtime.iterative.types;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TransitiveClosureEntry
{
	private static final long[] EMPTY_NEIGHBORS = new long[0];
	
	private long vid;
	
	private long cid;
	
	private long[] neighbors;
	
	private int numNeighbors;
	
	
	public TransitiveClosureEntry()
	{
		this.neighbors = EMPTY_NEIGHBORS;
	}

	public TransitiveClosureEntry(long vid, long cid, long[] neighbors) {
		this.vid = vid;
		this.cid = cid;
		this.neighbors = neighbors;
		this.numNeighbors = this.neighbors.length;
	}
	
	public TransitiveClosureEntry(long vid, long cid, long[] neighbors, int numNeighbors) {
		this.vid = vid;
		this.cid = cid;
		this.neighbors = neighbors;
		this.numNeighbors = numNeighbors;
	}

	
	public long getVid() {
		return vid;
	}
	
	public void setVid(long vid) {
		this.vid = vid;
	}
	
	public long getCid() {
		return cid;
	}
	
	public void setCid(long cid) {
		this.cid = cid;
	}
	
	public long[] getNeighbors() {
		return neighbors;
	}
	
	public void setNeighbors(long[] neighbors) {
		this.neighbors = neighbors;
	}
	
	public int getNumNeighbors() {
		return this.numNeighbors;
	}
	
	public void setNumNeighbors(int num) {
		this.numNeighbors = num;
	}
}
