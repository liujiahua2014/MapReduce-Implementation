package client;

public class Pair {
	
	private String first;
	private String second;
	
	public Pair(String key, String key2) {
		this.first = new String(key);
		this.second = new String(key2);
	}
	
	public String getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first = first;
	}
	public String getSecond() {
		return second;
	}
	public void setSecond(String second) {
		this.second = second;
	}
	
	public void print() {
		System.out.println(first + " : " + second);
	}
}