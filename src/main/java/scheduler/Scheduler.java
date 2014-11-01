package scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import amazon.Credentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import communication.ClusterMessage;
import communication.Communicator;
import communication.IMessageHandler;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import main.Main;
import org.joda.time.DateTime;

/**
 * Created by Rogier on 17-10-14.
 */
@Slf4j
public class Scheduler implements IMessageHandler {

	private final Properties properties;

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

	private final AmazonS3Client s3Client;

	@Getter
	private final List<Task> taskQueue = new ArrayList<Task>();

	@Getter
	private final List<Node> nodes = new ArrayList<Node>();

	private Communicator comm;

	@Getter
	private final Provisioner provisioner;

	/**
	 * Create the scheduler. Reads the properties, starts and S3 client and starts scheduling the poll tasks
	 */
	public Scheduler(Credentials awsCredentials, Properties properties) {
		this.properties = properties;

		s3Client = new AmazonS3Client(awsCredentials);

		comm = new Communicator(this);
		provisioner = new Provisioner(executorService, nodes, taskQueue, awsCredentials, properties);
		new ClusterHealth(this, executorService, nodes, properties);

		// Every task will start another one in succession. This ensures in case
		// of a high load not all tasks are
		// interfering and makes it easier to reason about stuff and things
		executorService.scheduleWithFixedDelay(this.checkForTasks(), 5,
				Integer.parseInt(properties.getProperty("aws.s3.check_interval", "10")), TimeUnit.SECONDS);
		executorService.scheduleWithFixedDelay(this.assignTaskToNode(), 5,
				Integer.parseInt(properties.getProperty("scheduler.assign_interval", "10")), TimeUnit.SECONDS);
	}

	/**
	 * Checks if there are any new tasks and adds them to the queue if required
	 *
	 * @return A Runnable object for the executorservice
	 */
	@Synchronized
	private Runnable checkForTasks() {
		return new Runnable() {

			@Override
			public void run() {
				String inputBucket = properties.getProperty("aws.s3.input", "input");
				String outputBucket = properties.getProperty("aws.s3.output", "output");

				// List the object and iterate through them
				ObjectListing list = s3Client.listObjects(inputBucket);
				List<S3ObjectSummary> objects = list.getObjectSummaries();
				// Shuffling is to make the testing a bit more non-deterministic
				Collections.shuffle(objects);
				for (S3ObjectSummary object : objects) {
					Task tmp = new Task(object.getKey());
					/* The equals only checks on the `inputFile` so we can
					 * safely do this
					 */
					if (!taskQueue.contains(tmp)) {
						/* If we do not yet have the task, add the other data
						 * and schedule it
						 */
						tmp.setCreated_at(DateTime.now());
						tmp.setOutputBucket(outputBucket);
						tmp.setOutputFile(tmp.getInputFile() + ".mp4");
						tmp.setStatus(Task.Status.QUEUED);
						// the assignment to a node is done by another method
						taskQueue.add(tmp);
						log.info("Added task to scheduler queue: {}", tmp);
					}
					else {
						// We have a precomputed one!
						// So we should definitely do something here
						// TODO
					}
				}
			}
		};
	}

	/**
	 * Assigns waiting tasks to nodes. It therefor picks a free node and the longest-waiting task, and matches them. In
	 * case there are no waiting tasks or free nodes, it does nothing
	 *
	 * @return
	 */
	@Synchronized
	private Runnable assignTaskToNode() {
		return new Runnable() {
			public void run() {
				Node assignTo = getIdleNode();
				Task assignTask = getFirstTask();
				if (assignTo == null || assignTask == null) {
					return;
				}

				assignTo.getAssignedTasks().add(assignTask);
				assignTask.setAssignedNode(assignTo);
				assignTask.setStatus(Task.Status.STARTED);

				ClusterMessage m = new ClusterMessage();
				m.setMessageType("assignment");
				m.setReceiverType(Communicator.type.WORKER);
				m.setReceiverIdentifier(assignTo.getInstanceId());
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("inputFile", assignTask.getInputFile());
				map.put("outputFile", assignTask.getOutputFile());
				m.setData(map);
				sendMessage(m);
				log.info("Assigned task {} to {}", assignTask, assignTo);
			}
		};
	}

	/**
	 * Get the first waiting task
	 *
	 * @return The task which was waiting for the longest time or null if no waiting task was found
	 */
	private Task getFirstTask() {
		if (taskQueue.size() > 0) {
			for (Task t : taskQueue) {
				if (t.getStatus() == Task.Status.QUEUED) {
					return t;
				}
			}
		}
		return null;
	}

	/**
	 * Get an idle node
	 *
	 * @return An idle node or null if no idle node was present
	 */
	private Node getIdleNode() {
		for (Node n : nodes) {
			if (n.isIdle()) {
				return n;
			}
		}
		return null;
	}

	/**
	 * Stops the remaining threads which the scheduler has spawned
	 */
	public void stop() {
		log.info("Stopping the scheduler");
		executorService.shutdownNow();
	}

	/**
	 * Set a task in the QUEUE to finished
	 *
	 * @param taskRepresentation A task object with the same inputfile field, so we can find the actual task in the
	 *                           queue
	 */
	public void removeTaskFromQueue(Task taskRepresentation, Task.Status status) {
		Task task = null;
		for (Task t : taskQueue) {
			if (t.equals(taskRepresentation)) {
				task = t;
				break;
			}
		}
		log.info("Done with task: {}", task);
		task.setStatus(status);
	}

	/**
	 * Handle an incoming message
	 *
	 * @param m the message which was received
	 */
	public void handleMessage(ClusterMessage m) {
		// Additionally, if a node reports success I should update my status
		if (m.getMessageType().equals("assignment-done")) {
			Task t = new Task((String) m.getData().get("inputFile"));
			removeTaskFromQueue(t, Task.Status.FINISHED);
			for (Node n : nodes) {
				if (n.getInstanceId().equals(m.getSenderIdentifier())) {
					n.getAssignedTasks().remove(t);
				}
			}
		}
		// And if a node reports a failure, I should be sad and update my status
		else if (m.getMessageType().equals("assignment-failed")) {
			Task t = new Task((String) m.getData().get("inputFile"));
			removeTaskFromQueue(t, Task.Status.FAILED);
			for (Node n : nodes) {
				if (n.getInstanceId().equals(m.getSenderIdentifier())) {
					n.getAssignedTasks().remove(t);
				}
			}
		}
	}

	/**
	 * Send a message to the cluster
	 *
	 * @param m the message to send
	 */
	public void sendMessage(ClusterMessage m) {
		this.comm.send(m);
	}

	/**
	 * This is just how we start the thing
	 *
	 * @param args _unused_
	 */
	public static void main(String[] args) {
		Main main = new Main();
		final Scheduler scheduler = new Scheduler(main.getCredentials(), main.getProperties());
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				scheduler.stop();
			}
		});
	}

}
