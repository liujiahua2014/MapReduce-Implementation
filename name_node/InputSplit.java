package name_node;

import java.io.Serializable;

public class InputSplit implements Serializable{

	private long start;
	private long end;
	
	public InputSplit() {
		super();
	}
	
	public InputSplit(long start, long end) {
		this.start = start;
		this.end = end;
	}
	
	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}
	

}