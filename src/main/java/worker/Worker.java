package worker;

import java.io.IOException;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import task.EncodingTask;

import communication.ClusterMessage;
import communication.Communicator;
import communication.IMessageHandler;

/**
 * Created by Rogier on 31-10-14.
 */
@Slf4j
public class Worker implements IMessageHandler {

	private final Communicator comm;
	private String instanceId;

	/**
	 * Loads the properties files, sets its identifier and starts participating
	 * in the cluster
	 */
	public Worker() {
		try {
			Properties p = new Properties();
			p.load(this.getClass().getResourceAsStream("/worker.properties"));
			instanceId = p.getProperty("instanceId");
		} catch (IOException e) {
			log.error("Could not read property file");
			System.exit(1);
		}
		log.trace("Using an identifier of {}", instanceId);
		comm = new Communicator(this, Communicator.type.WORKER, instanceId);

	}

	/**
	 * Sends a message to the entire cluster
	 * 
	 * @param m
	 *            the message to send
	 */
	public void sendMessage(ClusterMessage m) {
		this.comm.send(m);
	}

	/**
	 * Handle incoming messages Since I'm a worker, I only listen to assignments
	 * 
	 * @param m
	 */
	public void handleMessage(ClusterMessage m) {
		if (m.getMessageType().equals("assignment")) {
			log.trace("I'm going to work on {}", m.getData().get("inputFile"));
			EncodingTask et = new EncodingTask(m.getData().get("inputFile").toString(), m.getData().get("outputFile").toString());
			new Thread(et.createWorkTask(this.comm)).start();
		}
	}

	/**
	 * Start the thing h
	 * 
	 * @param args
	 *            _unused_
	 */
	public static void main(String[] args) {
		new Worker();
	}
}
