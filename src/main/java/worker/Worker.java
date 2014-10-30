package worker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import communication.ClusterMessage;
import communication.Communicator;
import communication.IMessageHandler;
import lombok.extern.slf4j.Slf4j;
import main.Main;
import task.EncodingTask;
import task.Result;
import task.ResultType;

/**
 * Created by Rogier on 31-10-14.
 */
@Slf4j
public class Worker implements IMessageHandler {

	private final Communicator comm;
	private String instanceId;

	public Worker() {
		try {
			Properties p = new Properties();
			p.load(this.getClass().getResourceAsStream("/worker.properties"));
			instanceId = p.getProperty("instanceId");
		}
		catch (IOException e) {
			log.error("Could not read property file");
			System.exit(1);
		}
		log.info("Using an identifier of {}",instanceId);
		comm = new Communicator(this, Communicator.type.WORKER, instanceId);

	}

	public void sendMessage(ClusterMessage m) {
		this.comm.send(m);
	}

	public void handleMessage(ClusterMessage m) {
		if (m.getReceiverType()== Communicator.type.WORKER && m.getReceiverIdentifier().equals(instanceId)) {
			// Meant for me!
			if ( m.getMessageType().equals("assignment")) {
				log.info("I'm going to work on {}",m.getData().get("inputFile"));
				workOn((String)m.getData().get("inputFile"),(String)m.getData().get("outputFile"));
			}
		}
	}

	private void workOn(final String inputFile, final String outputFile) {
		Runnable task = new Runnable() {
			public void run() {
				Main main = new Main();
				AmazonS3Client s3Client = new AmazonS3Client(main.getCredentials());

				log.info("Copying the file to myself");
				S3Object rawInput = s3Client.getObject(main.getProperties().getProperty("aws.s3.input"),inputFile);
				InputStream reader = new BufferedInputStream(
						rawInput.getObjectContent());
				File file = new File("/tmp/currentInputFile");
				try {
					OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
					int read = -1;

					while ((read = reader.read()) != -1) {
						writer.write(read);
					}

					writer.flush();
					writer.close();
					reader.close();

					log.info("Got the file locally now!");

					log.info("Started conversion");
					EncodingTask et = new EncodingTask("/tmp/currentInputFile","/tmp/currentOutputFile.mp4");
					Result r = et.executeTask();
					log.info("Result was {}",r.type);
					if ( r.type == ResultType.Success) {
						s3Client.putObject(main.getProperties().getProperty("aws.s3.output"),outputFile,new File("/tmp/currentOutputFile.mp4"));
						// Communicate back, delete from from input bucket
					}
					log.info("conversion was done and file wsa sent back to S3, unless you saw a stack trace");
					// s3Client.deleteObject(main.getProperties().getProperty("aws.s3.input"),inputFile);

					ClusterMessage m = new ClusterMessage();
					m.setReceiverIdentifier("scheduler");
					m.setReceiverType(Communicator.type.SCHEDULER);
					m.setMessageType("assignment-done");
					Map<String,Object> map = new HashMap<String,Object>();
					map.put("inputFile",inputFile);
					map.put("outputFile",outputFile);
					m.setData(map);
					sendMessage(m);
				} catch ( Exception e) {
					log.error("Something went terribly wrong",e);
				}

			}
		};
		// Should be done in a separate thread since it otherwise will block the messaging thread
		new Thread(task).start();
	}

	public static void main(String[] args) {
		new Worker();
	}
}
