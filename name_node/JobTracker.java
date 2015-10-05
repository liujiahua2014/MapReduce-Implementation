package name_node;
import java.util.List;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import context.JobContext;
import sss.SSS;

public class JobTracker extends Thread {
	
	private ArrayList<MyThread> myThreads;
	private ArrayList<InputSplit> inputSplits;
	private ArrayList<String> mapOutFileNames;
	private ArrayList<String> shuffleOutFileNames;
	private ServerSocket serverSocket;
	private JobContext jobContext;
	private static final int BLOCKSIZE = 10777222;
	private static final int MAXLINESIZE = 100;

	public JobTracker(int port) throws IOException {
		super(); 
		this.myThreads = new ArrayList<MyThread>();
		this.inputSplits = new ArrayList<InputSplit>();
		this.mapOutFileNames = new ArrayList<String>();
		this.shuffleOutFileNames = new ArrayList<String>();
		this.serverSocket = new ServerSocket(port);
		this.jobContext = new JobContext();
	}
	
	public void run() {
		
		//Upload IP and Port onto S3
		SSS sss = new SSS();
		try {
			String IPPort = InetAddress.getLocalHost().getHostAddress() + "\n" + serverSocket.getLocalPort();
			sss.uploadFile(IPPort, sss.getMasterAddrFileName());
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		//Delete several folders
		//sss.deleteAllFiles("MapOut");
		//sss.deleteAllFiles("ShuffleOut");
		//sss.deleteAllFiles(jobContext.getOutputPath());

		while(true) {
			try {	        	 
				Socket socket = serverSocket.accept();

				//Identify client or tasktracker
				DataInputStream in = new DataInputStream(socket.getInputStream());
				String msg = in.readUTF();

				if(msg.equals("TaskTracker")) {
					//Start a thread for a tasktracker
					MyThread clientThread = new MyThread(socket);
					clientThread.start();
					myThreads.add(clientThread);
				} else if(msg.equals("Client")) {
					//Get jobContext
					ObjectInputStream inObj = new ObjectInputStream(socket.getInputStream());
					this.jobContext = (JobContext)inObj.readObject();
					
					long time = System.currentTimeMillis();
					
					//Input Split
					confIS();
					
					System.out.println("InputSplit time: " + Long.toString(System.currentTimeMillis() - time));
					time = System.currentTimeMillis();

					//Sends out JobContext
					for (int i = 0; i < myThreads.size(); i++) {
						myThreads.get(i).setJobContext(jobContext);
					}

					//Map
					manageMap();

					System.out.println("Map time: " + Long.toString(System.currentTimeMillis() - time));
					time = System.currentTimeMillis();
					
					//Shuffle
					manageShuffle();
					
					System.out.println("Shuffle time: " + Long.toString(System.currentTimeMillis() - time));
					time = System.currentTimeMillis();

					//Reduce
					sss.deleteAllFiles(jobContext.getOutputPath());
					manageReduce();
					
					System.out.println("Reduce time: " + Long.toString(System.currentTimeMillis() - time));
					time = System.currentTimeMillis();
					
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());
					out.writeUTF("Job Done");
					break;
				}
	         } catch(SocketTimeoutException s) {
	            System.out.println("Socket timed out!");
	            break;
	         } catch(IOException e) {
	            e.printStackTrace();
	            break;
	         }catch(ClassNotFoundException e) {
		        e.printStackTrace();
		        break;
		     } catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }		
	}
	
	private void confIS() throws IOException {
		SSS s3 = new SSS();
		s3.setInputFileName(this.jobContext.getInputPath());

		long fileSize = s3.getInputFileSize();
		long start = 0, end = 0;

		while(start + BLOCKSIZE + MAXLINESIZE <= fileSize) {
			
			end = s3.getActualEnd(start, BLOCKSIZE, MAXLINESIZE);			
			InputSplit inputSplit = new InputSplit(start, end);
			inputSplits.add(inputSplit);			
			start = end + 1;
		}
		
		InputSplit inputSplit = new InputSplit(start, fileSize);
		inputSplits.add(inputSplit);
	}
	
	private void manageMap() throws InterruptedException, IOException {
		int lastAssignedIS = 0;
		while (true) {
			for (int i = 0; i < myThreads.size(); i++) {
				if(!myThreads.get(i).getIsAssigned()) {
					System.out.println("Thread " + i + " starts IS " + lastAssignedIS);
					myThreads.get(i).sendIS(inputSplits.get(lastAssignedIS++));
				}
				if(lastAssignedIS == inputSplits.size())
					break;
			}
			if(lastAssignedIS == inputSplits.size())
				break;
		}

		for (int i = 0; i < myThreads.size(); i++) {
			while(true) {
				if(!myThreads.get(i).isIdle())
					Thread.sleep(100);
				else
					break;
			}
			
			myThreads.get(i).notifyMapDone();
			
			while(true) {
				if(myThreads.get(i).getMapOutFileName().equals(""))
					Thread.sleep(100);
				else {
					this.mapOutFileNames.add(myThreads.get(i).getMapOutFileName());
					break;
				}
			}
			
		}
	}
	
	public void manageShuffle() throws UnsupportedEncodingException {
		SSS s3 = new SSS();
		HashMap<String, StringBuffer> KV = new HashMap<String, StringBuffer>();
		
		for(String fileName : this.mapOutFileNames) {
			InputStream inputStream = s3.downloadFile(fileName);
			
			try {
				String content = IOUtils.toString(inputStream, "UTF-8");
				String[] splits = content.split("\n");
				for(int i = 0; i < splits.length; ++i) {
					String key = splits[i].split("\t")[0];
					String value = splits[i].split("\t")[1];
					if(KV.containsKey(key)) {
						KV.get(key).append(", " + value);
					} else {
						KV.put(key, new StringBuffer(value));
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		//Upload files
		Iterator it = KV.keySet().iterator();
	    while(it.hasNext()) {
	    	String key = (String)it.next();
	    	s3.uploadFile(KV.get(key).toString(), "ShuffleOut/" + key);
	    	this.shuffleOutFileNames.add("ShuffleOut/" + key);
	    }
	}

	private void manageReduce() throws IOException, InterruptedException {
		int lastAssignedKey = 0;
		while (true) {
			for (int i = 0; i < myThreads.size(); i++) {
				if(!myThreads.get(i).getIsAssigned()) {
					System.out.println("Thread " + i + " starts Key " + lastAssignedKey);
					myThreads.get(i).sendKey(shuffleOutFileNames.get(lastAssignedKey++));
				}
				if(lastAssignedKey == shuffleOutFileNames.size())
					break;
			}
			if(lastAssignedKey == shuffleOutFileNames.size())
				break;
		}
		
		for (int i = 0; i < myThreads.size(); i++) {
			myThreads.get(i).notifyReduceDone();
		}
	}
	
	public static void main(String[] args)
	{
		int port = Integer.parseInt(args[0]);
	    try {
	    	JobTracker jobtracker = new JobTracker(port);
	    	jobtracker.start();
	    } catch(IOException e) {
	        e.printStackTrace();
	    }
	}
}