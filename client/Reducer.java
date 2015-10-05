package client;

import java.util.ArrayList;

public class Reducer<VALUEIN> {
	
	protected Pair reduce(String key, ArrayList<VALUEIN> values) {
		return new Pair(key, key);
	}
}