package test;


import java.io.IOException;

import client.Job;

public class Test{
	
	public static void main(String[] args) throws IOException
	{
		Job job = new Job();
		job.setMapperName("test.TestMapper");
		job.setReducerName("test.TestReducer");
		job.setInputPath("purchases4_39M.txt");
		job.setOutputPath("output");
		job.setJarPath("test.jar");
		job.waitForCompletion();
	}
}