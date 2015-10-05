package client;

public class Mapper{
	
	protected Pair map(String line) {
		return new Pair(line, line);
	}
}