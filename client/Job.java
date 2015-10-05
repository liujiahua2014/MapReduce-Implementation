package client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import org.apache.commons.io.IOUtils;

import sss.SSS;
import client.Mapper;
import context.JobContext;

public class Job{

	public Job() {
		super();
		this.jobContext = new JobContext();
	}

	private JobContext jobContext;
	
	
	public void setMapperName(String mapperName) {
		jobContext.setMapperName(mapperName);
	}

	public void setReducerName(String reducerName) {
		jobContext.setReducerName(reducerName);
	}
	
	public void setInputPath(String inputPath) {
		jobContext.setInputPath(inputPath);
	}
	
	public void setOutputPath(String outputPath) {
		jobContext.setOutputPath(outputPath);
	}
	
	public void setJarPath(String jarPath) {
		jobContext.setJarPath(jarPath);
	}
	
	public void waitForCompletion() throws IOException {
		//Get masterName and masterPort from S3
		SSS sss = new SSS();
		InputStream is = sss.downloadFile(sss.getMasterAddrFileName());
		String IPPort = IOUtils.toString(is, "UTF-8");
		String masterName = IPPort.split("\n")[0];
		int masterPort = Integer.parseInt(IPPort.split("\n")[1]);
		
		Socket socket = new Socket(masterName, masterPort);
		
		DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		out.writeUTF("Client");
		
		ObjectOutputStream outObj = new ObjectOutputStream(socket.getOutputStream());
		outObj.writeObject(this.jobContext);
		
		DataInputStream in = new DataInputStream(socket.getInputStream());
		String end = in.readUTF();
	}
}
