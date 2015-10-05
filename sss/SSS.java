package sss;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class SSS{
	
	private AmazonS3 s3client;
	private String bucketName;
	private String MasterAddrFileName;
	private String inputFileName;

	public SSS() {
		AWSCredentials credentials = new BasicAWSCredentials("AKIAJDSEM7P344JDBAEQ", "79AOuVMffkVI4ALF5yVoNp1bplgUx/0bzcLRy28k");
		this.s3client = new AmazonS3Client(credentials);
		this.bucketName = "mr-game-of-nodes";
		this.MasterAddrFileName = "MasterIPPort.txt";
	}
	
	public String getMasterAddrFileName() {
		return MasterAddrFileName;
	}
	
	public String getBucketName() {
		return bucketName;
	}
	
	public void uploadFile(String s, String file) throws UnsupportedEncodingException {
		InputStream inStream = new ByteArrayInputStream(s.getBytes("UTF-8"));
		s3client.deleteObject(bucketName, MasterAddrFileName);
		s3client.putObject(new PutObjectRequest(bucketName, file, inStream, new ObjectMetadata()));
	}
	
	public InputStream downloadFile(String fileName) {     
		S3Object object = s3client.getObject(new GetObjectRequest(bucketName, fileName));
		return object.getObjectContent();
	}	
	
	public void deleteAllFiles(String folderName) {
		List<S3ObjectSummary> fileList = s3client.listObjects(bucketName, folderName).getObjectSummaries();
		for (S3ObjectSummary file : fileList) {
			s3client.deleteObject(bucketName, file.getKey());
		}
	}
	
	/*** InputSplit ***/
	public void setInputFileName(String inputFileName) {
		this.inputFileName = inputFileName;
	}
	
	public long getInputFileSize() {
		S3Object object = s3client.getObject(new GetObjectRequest(bucketName, inputFileName));
		return object.getObjectMetadata().getContentLength();
	}
	
	public long getActualEnd(long start, int BLOCKSIZE, int MAXLINESIZE) throws IOException {
		InputStream inputStream = downloadRange(start + BLOCKSIZE, start + BLOCKSIZE + MAXLINESIZE, inputFileName);
		String beyond = IOUtils.toString(inputStream, "UTF-8");
		String[] splits = beyond.split("\n");		
		return start + BLOCKSIZE + splits[0].length();
	}
	
	public InputStream downloadRange(long start, long end, String file) {
		GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, file);
		rangeObjectRequest.setRange(start, end); // retrieve 1st 10 bytes.
		S3Object objectPortion = s3client.getObject(rangeObjectRequest);
		InputStream objectData = objectPortion.getObjectContent();
		return objectData;
	}
}