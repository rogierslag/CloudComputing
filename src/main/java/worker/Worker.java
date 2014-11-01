package worker;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
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

	/**
	 * Loads the properties files, sets its identifier and starts participating in the cluster
	 */
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
		log.info("Using an identifier of {}", instanceId);
		comm = new Communicator(this, Communicator.type.WORKER, instanceId);

	}

	/**
	 * Sends a message to the entire cluster
	 *
	 * @param m the message to send
	 */
	public void sendMessage(ClusterMessage m) {
		this.comm.send(m);
	}

	/**
	 * Handle incoming messages
	 *
	 * Since I'm a worker, I only listen to assignments
	 * @param m
	 */
	public void handleMessage(ClusterMessage m) {
		if (m.getMessageType().equals("assignment")) {
			log.info("I'm going to work on {}", m.getData().get("inputFile"));
			workOn((String) m.getData().get("inputFile"), (String) m.getData().get("outputFile"));
		}
	}

	/**
	 * Start working on some assignment.
	 *
	 * The actual work is done in a separate thread
	 * @param inputFile the S3 input file
	 * @param outputFile the S3 output file
	 */
	private void workOn(final String inputFile, final String outputFile) {
		Runnable task = new Runnable() {
			public void run() {
				Main main = new Main();

				// This is a better and faster way to copy to S3 (otherwise timeouts)
				TransferManager tx = new TransferManager(main.getCredentials());

				for ( int i = 0; i < 3; i++) {
					try {
						log.info("Copying the file to myself");
						File file = new File("/tmp/currentInputFile");
						Download dl = tx.download(main.getProperties().getProperty("aws.s3.input"), inputFile,
								file);
						dl.waitForCompletion();

						log.info("Got the file locally now!");

						log.info("Started conversion");
						EncodingTask et = new EncodingTask("/tmp/currentInputFile", "/tmp/currentOutputFile.mp4");
						Result r = et.executeTask();
						log.info("Result was {}", r.type);
						if (r.type == ResultType.Success) {
							Upload ul = tx.upload(main.getProperties().getProperty("aws.s3.output"), inputFile,
									new File("/tmp/currentOutputFile.mp4"));
							ul.waitForCompletion();

							// Communicate back, delete from from input bucket
							log.info("conversion was done and file waz sent back to S3");
							tx.shutdownNow();
							// s3Client.deleteObject(main.getProperties().getProperty("aws.s3.input"),inputFile);

							ClusterMessage m = new ClusterMessage();
							m.setReceiverIdentifier("scheduler");
							m.setReceiverType(Communicator.type.SCHEDULER);
							m.setMessageType("assignment-done");
							Map<String, Object> map = new HashMap<String, Object>();
							map.put("inputFile", inputFile);
							map.put("outputFile", outputFile);
							m.setData(map);
							sendMessage(m);
							return;
						}
					}
					catch (Exception e) {
						log.error("Something went terribly wrong", e);
					}
					log.warn("Iteration {} of 3 failed.",i);
				}
				tx.shutdownNow();

				// We only get here if we did not have any success
				ClusterMessage m = new ClusterMessage();
				m.setReceiverIdentifier("scheduler");
				m.setReceiverType(Communicator.type.SCHEDULER);
				m.setMessageType("assignment-failed");
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("inputFile", inputFile);
				map.put("outputFile", outputFile);
				m.setData(map);
				sendMessage(m);
			}
		};
		// Should be done in a separate thread since it otherwise will block the messaging thread
		new Thread(task).start();
	}

	/**
	 * Start the thing
	 * @param args _unused_
	 */
	public static void main(String[] args) {
		new Worker();
	}
}
