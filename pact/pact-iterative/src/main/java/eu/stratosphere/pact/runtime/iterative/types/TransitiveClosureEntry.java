package eu.stratosphere.pact.runtime.iterative.types;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class TransitiveClosureEntry
{
	private int vid;
	
	private int cid;
	
	private int[] neighbors;
	
	private int numNeighbors;
	
	
	public TransitiveClosureEntry()
	{}

	public TransitiveClosureEntry(int vid, int cid, int[] neighbors) {
		this.vid = vid;
		this.cid = cid;
		this.neighbors = neighbors;
		this.numNeighbors = this.neighbors.length;
	}
	
	public TransitiveClosureEntry(int vid, int cid, int[] neighbors, int numNeighbors) {
		this.vid = vid;
		this.cid = cid;
		this.neighbors = neighbors;
		this.numNeighbors = numNeighbors;
	}

	
	public int getVid() {
		return vid;
	}
	
	public void setVid(int vid) {
		this.vid = vid;
	}
	
	public int getCid() {
		return cid;
	}
	
	public void setCid(int cid) {
		this.cid = cid;
	}
	
	public int[] getNeighbors() {
		return neighbors;
	}
	
	public void setNeighbors(int[] neighbors) {
		this.neighbors = neighbors;
	}
	
	public int getNumNeighbors() {
		return this.numNeighbors;
	}
	
	public void setNumNeighbors(int num) {
		this.numNeighbors = num;
	}
}
