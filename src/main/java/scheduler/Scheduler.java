package scheduler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;

import amazon.Credentials;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Created by Rogier on 17-10-14.
 */
@Slf4j
public class Scheduler {

	private final Properties properties;

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);

	private final AmazonEC2Client ec2Client;
	private final AmazonS3Client s3Client;

	@Getter
	private final List<Task> taskQueue = new ArrayList<Task>();

	@Getter
	private final List<Node> nodes = new ArrayList<Node>();
	private boolean isDestroying = false;
	private boolean isCreating = false;

	/**
	 * Create the scheduler. Reads the properties, starts and S3 client and
	 * starts scheduling the poll tasks
	 */
	public Scheduler(Credentials awsCredentials, Properties properties) {
		this.properties = properties;
		int checkInterval = 15;
		try {
			checkInterval = Integer.parseInt(this.properties.getProperty("scheduler.check_every_x_seconds", "15"));
		} catch (NumberFormatException e) {
			log.warn("Could not read properties value '{}'", "scheduler.check_every_x_seconds");
		}

		s3Client = new AmazonS3Client(awsCredentials);
		ec2Client = new AmazonEC2Client(awsCredentials);
		ec2Client.setEndpoint("ec2.eu-west-1.amazonaws.com");

		log.info("Start polling buckets in {} seconds and redo every {} seconds", checkInterval, checkInterval);

		// Every task will start another one in succession. This ensures in case
		// of a high load not all tasks are
		// interfering and makes it easier to reason about stuff and things
		executorService.schedule(this.checkForTasks(checkInterval), checkInterval, TimeUnit.SECONDS);
		executorService.schedule(this.checkForNodeAdjustments(checkInterval), checkInterval, TimeUnit.SECONDS);
	}

	/**
	 * Reads the properties file and load them into a Properties object
	 * 
	 * @return the created Properties object
	 */
	private Properties readProperties() {
		try {
			FileInputStream propertiesFile = new FileInputStream("scheduler.properties");
			Properties props = new Properties();
			props.load(propertiesFile);
			return props;
		} catch (FileNotFoundException e) {
			// A problem was found, so returning to sane defaults happens in the
			// app
			log.error("Triggered an FileNotFound error when trying to read the properties file: {}", e);
			return new Properties();
		} catch (IOException e) {
			log.error("Triggered an IO error when trying to read the properties file: {}", e);
			return new Properties();
		}
	}

	/**
	 * Checks if there are any new tasks and adds them to the queue if required
	 * 
	 * @param rescheduleInterval
	 *            how many seconds after finishing the next poll should be done
	 * @return A Runnable object for the executorservice
	 */
	@Synchronized
	private Runnable checkForTasks(int rescheduleInterval) {
		final int interval = rescheduleInterval;
		return new Runnable() {

			@Override
			public void run() {
				String inputBucket = properties.getProperty("aws.s3.input", "input");
				String outputBucket = "output";
				if (properties.containsKey("aws.s3.output")) {
					outputBucket = properties.getProperty("aws.s3.output", "output");

				} else {
					log.warn("Output bucket was not defined in properties file: {}", properties);
				}
				log.info("About to do a run of the input bucket: {}", inputBucket);

				// List the object and iterate through them
				ObjectListing list = s3Client.listObjects(inputBucket);
				for (S3ObjectSummary object : list.getObjectSummaries()) {
					Task tmp = new Task(object.getKey());
					// The equals only checks on the `inputFile` so we can
					// safely do this
					if (!taskQueue.contains(tmp)) {
						// If we do not yet have the task, add the other data
						// and schedule it
						tmp.setCreated_at(DateTime.now());
						tmp.setOutputBucket(outputBucket);
						tmp.setOutputFile(UUID.randomUUID() + ".some_extension");
						tmp.setStatus(Task.Status.QUEUED);
						// Should also assign to a node, but will do that later
						// (separate method etc)
						taskQueue.add(tmp);
						log.info("Added task to scheduler queue: {}", tmp);
					} else {
						// We have a precomputed one!
						// So we should definitely do something here
					}
				}
				log.info("Done with a run of the input bucket");
				executorService.schedule(checkForTasks(interval), interval, TimeUnit.SECONDS);
			}
		};
	}

	/**
	 * Checks if we need to adjust our node count and does do if required
	 * 
	 * @param rescheduleInterval
	 *            after how many seconds a recheck should be done
	 * @return Runnable for the scheduler
	 */
	private Runnable checkForNodeAdjustments(int rescheduleInterval) {
		final int interval = rescheduleInterval;
		return new Runnable() {
			@Override
			public void run() {
				log.info("Going to check how busy we are");
				List<Task> waitingTasks = new ArrayList<Task>();
				for (Task t : taskQueue) {
					if (t.getStatus() == Task.Status.QUEUED) {
						waitingTasks.add(t);
					}
				}
				log.info("We have exactly {} waiting tasks", waitingTasks.size());
				if (waitingTasks.size() > 0 && nodes.isEmpty()) {
					log.info("We have tasks, but no node. Always provision one");
					provisionNewNode();
					return;
				}
				if (waitingTasks.size() == 0 && !nodes.isEmpty()) {
					Node toRemove = null;
					for (Node toCheck : nodes) {
						if (toCheck.getAssignedTasks().isEmpty()) {
							toRemove = toCheck;
							break;
						}
					}
					if (toRemove != null) {
						// Destroy the node without tasks
						log.info("We have no tasks, but we do have nodes. Delete node '{}'", toRemove);
						destroyExistingNode(toRemove);
					}
					return;
				}
				Task longestWaitingTask = waitingTasks.get(0);
				if (longestWaitingTask.getCreated_at().isBefore(new DateTime().minusMinutes(10))) {
					log.info("A task was waiting for more than 10 minutes, which is long, so we need a new node");
					provisionNewNode();
				}
			}
		};
	}

	/**
	 * Destroys a node and removes it from the list of nodes if it was in there
	 * 
	 * @param remove
	 *            The node to remove
	 */
	public void destroyExistingNode(Node remove) {
		if (isDestroying) {
			// If we are already destroying a node, don't destroy another
			return;
		}
		isDestroying = true;

		if (!remove.getAssignedTasks().isEmpty()) {
			return;
		}

		log.info("Node {} is about to be removed", remove);
		nodes.remove(remove);
		List<String> list = new ArrayList<String>(1);
		list.add(remove.getInstanceId());

		TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(list);
		ec2Client.terminateInstances(terminateRequest);

		log.info("Node {} is removed from the EC2 pool", remove);
		isDestroying = false;
	}

	/**
	 * Creates a new node and adds it to the list of nodes
	 */
	public void provisionNewNode() {
		if (isCreating) {
			// If we are already creating a node, dont create another
			return;
		}
		isCreating = true;

		log.info("About to spin up an extra instance");

		RunInstancesRequest runInstancesRequest = new RunInstancesRequest().withInstanceType(properties.getProperty("aws.ec2.type", "t2.micro"))
				.withImageId(properties.getProperty("aws.ec2.image", "ami-b0b51cc7")).withMinCount(1).withMaxCount(1)
				.withSecurityGroupIds(properties.getProperty("aws.ec2.security", "default")).withKeyName(properties.getProperty("aws.ec2.key", "scheduler"))
				.withUserData(Base64.encodeBase64String("echo \"instance running\" | mail rogier.slag@gmail.com".getBytes()));
		RunInstancesResult runInstances = ec2Client.runInstances(runInstancesRequest);

		String instanceId = runInstances.getReservation().getInstances().get(0).getInstanceId();
		log.info("New instance has instanceId '{}'", instanceId);
		String publicIp = null;

		for (int i = 0; i < 20; i++) {
			try {
				DescribeInstancesRequest describeInstance = new DescribeInstancesRequest().withInstanceIds(instanceId);
				DescribeInstancesResult instance = ec2Client.describeInstances(describeInstance);
				String tmp = instance.getReservations().get(0).getInstances().get(0).getPublicIpAddress();
				if (tmp != null) {
					publicIp = tmp;
					break;
				}
			} catch (IndexOutOfBoundsException e) {
				// This exception is triggered if one of the get(0) calls does
				// not work.
				// And it is quite a lot of boilerplate to handle that
			}
			try {
				// Sleep 6 seconds. With a total of 20 runs this gives AWS 120
				// seconds to spinup
				Thread.sleep(6000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		try {
			Node node = new Node(InetAddress.getByName(publicIp), instanceId);
			nodes.add(node);
			log.info("Provisioned new node '{}'", node);
		} catch (UnknownHostException e) {
			// Wait whut
			// When shit hits the fan, just cancel the thing
			// This one triggers if Amazon gives us a non-existing IP
			// Or if we did not get a final IP in 120 seconds
			log.error("Something went wrong when getting a new node, destroying instanceId {}", instanceId);
			List<String> list = new ArrayList<String>(1);
			list.add(instanceId);

			TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(list);
			ec2Client.terminateInstances(terminateRequest);
		}

		isCreating = false;
	}

	/**
	 * Stops the remaining threads which the scheduler has spawned
	 */
	public void stop() {
		log.info("Stopping the scheduler");
		executorService.shutdownNow();
	}

	public void removeTaskFromQueue(Task task) {
		log.info("Done with task: {}", task);
		taskQueue.remove(task);
	}

}
