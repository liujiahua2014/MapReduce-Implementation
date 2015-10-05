package context;

import java.io.Serializable;

import client.Mapper;

public class JobContext implements Serializable{
	
	private String mapperName;
	private String reducerName;
	private String inputPath;
	private String jarPath;
	private String outputPath;
	
	public String getJarPath() {
		return jarPath;
	}

	public void setJarPath(String jarPath) {
		this.jarPath = jarPath;
	}

	public String getOutputPath() {
		return outputPath;
	}

	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
	}

	public String getMapperName() {
		return mapperName;
	}

	public String getReducerName() {
		return reducerName;
	}

	public String getInputPath() {
		return inputPath;
	}

	public void setMapperName(String mapperName) {
		this.mapperName = mapperName;
	}

	public void setReducerName(String reducerName) {
		this.reducerName = reducerName;
	}
	
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	
}