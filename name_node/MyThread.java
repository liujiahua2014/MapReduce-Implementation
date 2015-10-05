package name_node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import context.JobContext;

public class MyThread extends Thread {
    
	protected Socket socket;
	//private long lastUpdateTime;
	//private long timeout;
	private JobContext jobContext;
	private InputSplit inputSplit;
	private boolean isAssigned;
	private ObjectOutputStream outObj;
	private DataOutputStream outData; 
	private DataInputStream inData;
	private String mapOutFileName;
	private STATUS status;
	private String key;
	
	public enum STATUS {
		Idle, MapReady, MapDone, ReduceReady, ReduceDone;
    }

	public String getMapOutFileName() {
		return mapOutFileName;
	}

	public MyThread(Socket clientSocket) throws IOException {
        this.socket = clientSocket;
        this.outObj = new ObjectOutputStream(socket.getOutputStream());
        this.outData = new DataOutputStream(socket.getOutputStream());
        this.inData = new DataInputStream(socket.getInputStream());
        this.status = STATUS.Idle;
        this.mapOutFileName = "";
    }
	
	public void setJobContext(JobContext jobContext) throws IOException {
		this.jobContext = jobContext;
		outObj.writeObject(this.jobContext);
	}

    public boolean getIsAssigned() {
		return isAssigned;
	}
    
    @Override
    public void run() {
    	while(true) {
    		switch(status){
    			case Idle:
    				break;
    			case MapReady:
					try {
						notifyMapStart();
						System.out.println("MapReady Done");
						this.status = STATUS.Idle;
					} catch (IOException e) {
						e.printStackTrace();
					}
    				break;
    			case MapDone:
					try {				
						System.out.println("MapDone Starts");
						outData.writeUTF("Map Done");
	    				this.mapOutFileName = inData.readUTF();
	    				this.status = STATUS.Idle;
					} catch (IOException e) {
						e.printStackTrace();
					}
    				break;
    			case ReduceReady:
					try {
						notifyReduceStart();
					} catch (IOException e) {
						e.printStackTrace();
					}
    				break;
    			case ReduceDone:
					try {
						this.status = STATUS.Idle;
						outData.writeUTF("Reduce Done");
					} catch (IOException e) {
						e.printStackTrace();
					}
    				break;
    		}
    	}
    }
    
//    public boolean isHBTimeout() {
//		long now = System.currentTimeMillis();
//		return (now > this.lastUpdateTime + this.timeout);
//	}
    
    public boolean isIdle() {
    	return status==STATUS.Idle;
    }
    
    /*** Mapper Stage ***/
    public void sendIS(InputSplit inputSplit) {
		this.inputSplit = inputSplit;
		this.status = STATUS.MapReady;
		this.isAssigned = true;
	}
    
    public void notifyMapStart() throws IOException {
    	
    	outData.writeUTF("Start Map");
		outObj.writeObject(this.inputSplit);
		
	 	if(inData.readUTF().equals("Block Done"))
			isAssigned = false;
	 	
    }

	public void notifyMapDone() throws IOException {
		this.status = STATUS.MapDone;
	}
	
	/*** Reducer Stage ***/
	public void sendKey(String key) {
		this.key = key;
		this.status = STATUS.ReduceReady;
		this.isAssigned = true;
	}	
	
	public void notifyReduceStart() throws IOException {
		this.status = STATUS.Idle;
		
		outData.writeUTF(key);
		
	 	if(inData.readUTF().equals("Key Done"))
			isAssigned = false;
	}
	
	public void notifyReduceDone() throws IOException {
		this.status = STATUS.ReduceDone;
	}
}