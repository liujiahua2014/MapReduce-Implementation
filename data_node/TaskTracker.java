package data_node;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;

import sss.SSS;
import context.JobContext;
import name_node.InputSplit;
import client.Pair;

public class TaskTracker {

	private Socket socket;
	private ObjectInputStream inObj;
	private DataOutputStream outData; 
	private DataInputStream inData;
	private SSS sss;
	
	private JobContext jobContext;
	private InputSplit inputSplit;
	private HashMap<String, StringBuffer> mapOutKV;
	private String redOutStr;
	private String jarURL;

	public TaskTracker() {
		sss = new SSS();
		mapOutKV = new HashMap<String, StringBuffer>();
		redOutStr = "";
	}

	public void run() {

		try {
			//Get masterName and masterPort from S3
			InputStream is = sss.downloadFile(sss.getMasterAddrFileName());
			String IPPort = IOUtils.toString(is, "UTF-8");
			String masterName = IPPort.split("\n")[0];
			int masterPort = Integer.parseInt(IPPort.split("\n")[1]);

			//Initialize
			socket = new Socket(masterName, masterPort);
			outData = new DataOutputStream(socket.getOutputStream());
			inData = new DataInputStream(socket.getInputStream());

			//Notify Master of its identity
			outData.writeUTF("TaskTracker");

			//Get JobContext from Master and set jarURL
			inObj = new ObjectInputStream(socket.getInputStream());
			this.jobContext = (JobContext)inObj.readObject();
			jarURL = "https://s3.amazonaws.com/" + sss.getBucketName() + "/" + jobContext.getJarPath();

			//Execute Map
			exeMap();
			
			//Execute Reduce
			exeReduce();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void exeMap() {
		
		try {
			//Mapper Stage
			while(true) {
				if(inData.readUTF().equals("Map Done"))
					break;
				
				//Download a block
				this.inputSplit = (InputSplit)inObj.readObject();
				InputStream inputStream = sss.downloadRange(inputSplit.getStart(), inputSplit.getEnd(), jobContext.getInputPath());
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));

				//Call map function
				URL[] urls = new URL[1];
				urls[0] = new URL(jarURL);
				URLClassLoader uRLClassLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
				Class classToLoad = Class.forName(jobContext.getMapperName(), true, uRLClassLoader);
				Object instancer = classToLoad.newInstance();
				Method method = classToLoad.getMethod("map", String.class);
				
				String line = br.readLine();
				while ((line = br.readLine()) != null) {
					Pair pair = (Pair)method.invoke(instancer, line);
					addPairToHM(pair);
				}

				br.close();
				outData.writeUTF("Block Done");
			}

			//Construct a large string for uploading
			Iterator it = mapOutKV.keySet().iterator();  
			StringBuffer mapOutSB = new StringBuffer("");
			while(it.hasNext()) {  
				String key = (String)it.next();
				String value = mapOutKV.get(key).toString();
				mapOutSB.append(key + "\t" + value + "\n");
			}

			//Upload map output file onto S3
			String file = "MapOut/" + this.toString();
			sss.uploadFile(mapOutSB.toString(), file);
			outData.writeUTF(file);
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void exeReduce() {
		try {
			//Reducer Stage
			while(true) {
				String key = inData.readUTF();
				if(key.equals("Reduce Done"))
					break;
				
				InputStream inputStream = sss.downloadFile(key);
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
				String[] splits = br.readLine().split(", ");
				
				//Call reduce function
				URL[] urls = new URL[1];
				urls[0] = new URL(jarURL);
				URLClassLoader uRLClassLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
				Class classToLoad = Class.forName(jobContext.getReducerName(), true, uRLClassLoader);
				Object instancer = classToLoad.newInstance();
				Method method = classToLoad.getMethod("reduce", String.class, ArrayList.class);
				Pair pair = (Pair)method.invoke(instancer, key.split("/")[1], new ArrayList<String>(Arrays.asList(splits)));
				redOutStr += pair.getFirst() + "\t" + pair.getSecond() + "\n";

				br.close();
				outData.writeUTF("Key Done");
			}
			
			sss.uploadFile(redOutStr, jobContext.getOutputPath() + "/" + this.toString());
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	private void addPairToHM(Pair pair) {
		String key = pair.getFirst();
		String value = pair.getSecond();

		if (mapOutKV.containsKey(key)) {
			mapOutKV.get(key).append(", " + value);
		} else {
			mapOutKV.put(key, new StringBuffer(value));
		}

	}

	public static void main(String[] args)
	{
		TaskTracker taskTracker = new TaskTracker();
		taskTracker.run();
	}
}


//InputStream and OutputStream
//			while(true) {
//				OutputStream a;
//				ByteArrayOutputStream b = (ByteArrayOutputStream)a;
//				byte[] c = b.toByteArray();
//				InputStream inStream = new ByteArrayInputStream(c);
//				
//				String aa = "";
//				c = aa.getBytes();
//				byte[] dd = c.clone();
//			}

//HeartBeat Handling
//			while(true) {
//				String outStr = "heartbeat from client: " + socket.getLocalSocketAddress();
//	        	out.writeUTF(outStr);
//	        	Thread.sleep(1000);
//			}