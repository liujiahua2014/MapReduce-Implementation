package test;

import java.util.ArrayList;

import client.Pair;
import client.Reducer;

public class TestReducer extends Reducer<String>{
	
	public Pair reduce(String key, ArrayList<String> values) {
		return new Pair(key, Integer.toString(values.size()));
	}
}