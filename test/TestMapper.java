package test;

import client.Mapper;
import client.Pair;

public class TestMapper extends Mapper{

		public Pair map(String line) {
			
			String key = "", value = "";
			try {
				String[] splits = line.split("\t");
				key = splits[3];
				value = splits[4];
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
			
			return new Pair(key, value);
		}
	}