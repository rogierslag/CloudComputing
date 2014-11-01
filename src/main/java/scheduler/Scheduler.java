package scheduler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import communication.ClusterMessage;
import communication.Communicator;
import communication.IMessageHandler;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import main.Main;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;

/**
 * Created by Rogier on 17-10-14.
 */
@Slf4j
public class Scheduler implements IMessageHandler {

	private final Properties properties;

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);

	private final AmazonEC2Client ec2Client;
	private final AmazonS3Client s3Client;

	@Getter
	private final List<Task> taskQueue = new ArrayList<Task>();

	@Getter
	private final List<Node> nodes = new ArrayList<Node>();
	private boolean isDestroying = false;
	private boolean isCreating = false;

	private Communicator comm;
	private ArrayList<String> healthcheckReplies;
	private HashMap<String, Object> healthcheckMap;
	private HashMap<String, Integer> missedHealthchecks = new HashMap<String, Integer>();

	/**
	 * Create the scheduler. Reads the properties, starts and S3 client and starts scheduling the poll tasks
	 */
	public Scheduler(Credentials awsCredentials, Properties properties) {
		this.properties = properties;
		int checkInterval = 15;
		try {
			checkInterval = Integer.parseInt(this.properties.getProperty("scheduler.check_every_x_seconds", "15"));
		}
		catch (NumberFormatException e) {
			log.warn("Could not read properties value '{}'", "scheduler.check_every_x_seconds");
		}

		s3Client = new AmazonS3Client(awsCredentials);
		ec2Client = new AmazonEC2Client(awsCredentials);
		ec2Client.setEndpoint("ec2.eu-west-1.amazonaws.com");

		log.info("Start polling buckets in {} seconds and redo every {} seconds", checkInterval, checkInterval);

		// Every task will start another one in succession. This ensures in case
		// of a high load not all tasks are
		// interfering and makes it easier to reason about stuff and things
		executorService.schedule(this.checkForTasks(checkInterval), 5, TimeUnit.SECONDS);
		executorService.schedule(this.checkForNodeAdjustments(checkInterval), checkInterval, TimeUnit.SECONDS);

		comm = new Communicator(this);
		executorService.scheduleWithFixedDelay(this.checkForClusterLiveness(), 10, 10, TimeUnit.SECONDS);

		executorService.scheduleWithFixedDelay(this.assignTaskToNode(), 15, 10, TimeUnit.SECONDS);
	}

	/**
	 * Checks all the nodes in the cluster to determine if they are still alive. In case a host does not respond to
	 * three healthchecks in a row, it is removed from the cluster and also terminated by EC2.
	 *
	 * @return Runnable for the executorservice
	 */
	@Synchronized
	private Runnable checkForClusterLiveness() {
		return new Runnable() {
			public void run() {
				log.info("The following nodes were alive during the last healthcheck: {}",
						healthcheckReplies);

				if (healthcheckReplies != null) {

					/*
					 * We check every node in the `nodes` arrayList. If it did not reply to the healthcheck,
					 * we increase
					 * the missedhealthcheck by one 1 for that host. Once it goes over 3, the node is terminated.
					 *
					 * If it does respond an entry of the missedhealthcheck is removed (if applicable)
					 */
					List<Node> toTerminate = new ArrayList<Node>();
					for (Node n : nodes) {
						if (!healthcheckReplies.contains(n.getInstanceId())) {
							if (missedHealthchecks.containsKey(n.getInstanceId())) {
								int missedCount = missedHealthchecks.get(n.getInstanceId()) + 1;
								if (missedCount > 3) {
									log.warn("{} missed over 3 healthchecks, terminating", n);
									toTerminate.add(n);
									for (Task t : n.getAssignedTasks()) {
										t.setStatus(Task.Status.QUEUED);
										t.setAssignedNode(null);
									}
								}
								else {
									log.warn("{} missed {} healthchecks", n,
											missedCount);
									missedHealthchecks.put(n.getInstanceId(),
											missedCount);
								}
							}
							else {
								missedHealthchecks.put(n.getInstanceId(), 1);
							}

						}
						else {
							missedHealthchecks.remove(n.getInstanceId());
						}
					}
					for (Node n : toTerminate) {
						log.info("Destroying node {}", n);
						destroyExistingNode(n, true);
					}

				}

				/*
				 * Send a new round of healthcheck messages.
				 * The map we send with random data ensures we know the replies are recent,
				 * hence nodes are not a few minutes behind with replying
				 */
				healthcheckReplies = new ArrayList<String>();
				healthcheckMap = new HashMap<String, Object>();
				healthcheckMap.put("hash", UUID.randomUUID());
				ClusterMessage m = new ClusterMessage();
				m.setMessageType("healthcheck");
				m.setData(healthcheckMap);
				sendMessage(m);
			}
		};
	}

	/**
	 * Checks if there are any new tasks and adds them to the queue if required
	 *
	 * @param rescheduleInterval how many seconds after finishing the next poll should be done
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

				}
				else {
					log.warn("Output bucket was not defined in properties file: {}", properties);
				}

				// List the object and iterate through them
				ObjectListing list = s3Client.listObjects(inputBucket);
				for (S3ObjectSummary object : list.getObjectSummaries()) {
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
				executorService.schedule(checkForTasks(interval), interval, TimeUnit.SECONDS);
			}
		};
	}

	/**
	 * Checks if we need to adjust our node count and does do if required.
	 * The log messages make this methods relatively self-explanatory
	 *
	 * @param rescheduleInterval after how many seconds a recheck should be done
	 * @return Runnable for the scheduler
	 */
	private Runnable checkForNodeAdjustments(final int rescheduleInterval) {
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
				}
				else if (waitingTasks.size() == 0 && !nodes.isEmpty()) {
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
				}
				else if (waitingTasks.size() > 0 &&
						waitingTasks.get(0).getCreated_at().isBefore(new DateTime().minusMinutes(10))) {
					log.info("A task was waiting for more than 10 minutes, which is long, so we need a new node");
					provisionNewNode();
				}
				else if (waitingTasks.size() >= 3 * nodes.size()) {
					log.info("There are currently 3 times more tasks than nodes, so we should add capacity");
					provisionNewNode();
				}
				else {
					log.info("Apparantly we do not need more nodes than we already have: {}", nodes);
				}

				executorService.schedule(checkForNodeAdjustments(rescheduleInterval), rescheduleInterval,
						TimeUnit.SECONDS);
			}
		};
	}

	/**
	 * Assigns waiting tasks to nodes.
	 * It therefor picks a free node and the longest-waiting task, and matches them. In case there are no waiting tasks
	 * or free nodes, it does nothing
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
	 * Destroy a node it possible (no force)
	 * @param remove The node to be removed
	 */
	public void destroyExistingNode(Node remove) {
		destroyExistingNode(remove, false);
	}

	/**
	 * Destroys a node and removes it from the list of nodes if it was in there
	 *
	 * @param remove The node to remove
	 * @param force  Whether or not the node should forcibly be removed (hence even with running tasks)
	 */
	public void destroyExistingNode(Node remove, boolean force) {
		if (isDestroying) {
			// If we are already destroying a node, don't destroy another
			return;
		}
		isDestroying = true;

		if (!force && !remove.getAssignedTasks().isEmpty()) {
			// In case the node has tasks and we are not forcing destroying, return and do nothing
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

		RunInstancesRequest runInstancesRequest =
				new RunInstancesRequest().withInstanceType(properties.getProperty("aws.ec2.type", "t2.micro"))
						.withImageId(properties.getProperty("aws.ec2.image", "ami-b0b51cc7"))
						.withMinCount(1)
						.withMaxCount(1)
						.withSecurityGroupIds(properties.getProperty("aws.ec2.security", "default"))
						.withKeyName(properties.getProperty("aws.ec2.key", "scheduler"))
						.withUserData(Base64.encodeBase64String(getScript("install_worker.sh").getBytes()));
		RunInstancesResult runInstances = ec2Client.runInstances(runInstancesRequest);

		String instanceId = runInstances.getReservation().getInstances().get(0).getInstanceId();
		log.info("New instance has instanceId '{}'", instanceId);
		String privateIp = null;

		/* We dont get the IP from amazon directly, so we have to poll for it. Using a loop is the easiest way, since
		 * there is no push possible of the information
		 */
		for (int i = 0; i < 20; i++) {
			try {
				DescribeInstancesRequest describeInstance = new DescribeInstancesRequest().withInstanceIds(instanceId);
				DescribeInstancesResult instance = ec2Client.describeInstances(describeInstance);
				String tmp = instance.getReservations().get(0).getInstances().get(0).getPrivateIpAddress();
				if (tmp != null) {
					privateIp = tmp;
					break;
				}
			}
			catch (IndexOutOfBoundsException e) {
				// This exception is triggered if one of the get(0) calls does
				// not work.
				// And it is quite a lot of boilerplate to handle that
			}
			try {
				// Sleep 6 seconds. With a total of 20 runs this gives AWS 120
				// seconds to spinup
				Thread.sleep(6000);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		/*
		 * Using the `withUserData` method we install software on the node, however we also need to start it
		 * The method `startNode` handles exactly that and does the maven magic. Once it returns the node is ready for
		 * production
		 */
		try {
			log.info("Starting the software on the new node");
			startNode(privateIp, instanceId, properties);

			Node node = new Node(InetAddress.getByName(privateIp), instanceId);
			nodes.add(node);
			log.info("Provisioned new node '{}'", node);
			log.info("Now we have the following nodes: {}", nodes);
		}
		catch (UnknownHostException e) {
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
		catch (JSchException e) {
			// Wait whut
			// When shit hits the fan, just cancel the thing
			// This one triggers if SSH connection fail
			log.error("Something went wrong when getting a new node, destroying instanceId {}", instanceId);
			List<String> list = new ArrayList<String>(1);
			list.add(instanceId);

			TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(list);
			ec2Client.terminateInstances(terminateRequest);
		}

		// Other may start to create nodes again
		isCreating = false;
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
	 * @param taskRepresentation A task object with the same inputfile field, so we can find the actual task in the queue
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
	 * @param m the message which was received
	 */
	public void handleMessage(ClusterMessage m) {
		// Being the scheduler, I should handle healthcheck response
		if (m.getMessageType().equals("healthcheck-response")) {
			receivedHealthCheckReply(m);
		}
		// Additionally, if a node reports success I should update my status
		else if (m.getMessageType().equals("assignment-done")) {
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
	 * @param m the message to send
	 */
	public void sendMessage(ClusterMessage m) {
		this.comm.send(m);
	}

	/**
	 * Process a healthcheck response message
	 * @param m the response
	 */
	private void receivedHealthCheckReply(ClusterMessage m) {
		if (m.getData().equals(this.healthcheckMap) && !this.healthcheckReplies.contains(m.getSenderIdentifier())) {
			this.healthcheckReplies.add(m.getSenderIdentifier());
		}
	}

	/**
	 * Reads a script from disk and returns it as String
	 * @param file The script to read
	 * @return The resulting string
	 */
	public static String getScript(String file) {
		String result = "";
		try {
			BufferedReader br = new BufferedReader(new FileReader(file));

			String line;
			while ((line = br.readLine()) != null) {
				result += line + "\n";
			}
			br.close();
		}
		catch (Exception e) {
			log.error("Could not read {} script",file, e);
		}
		return result;
	}

	/**
	 * Actually start the node with the software of a worker
	 * @param privateIp the private IP addres to listen on
	 * @param instanceId The instance ID as specified by AWS
	 * @param settings The properties to initialize with
	 * @throws JSchException In case we cannot connect
	 */
	private static void startNode(String privateIp, String instanceId, Properties settings) throws JSchException {
		for (int i = 0; i < 20; i++) {
			try {
				/*
				 * Connect by SSH (private key) as the ubuntu user without StrictHostKeyChecking
				 */
				JSch ssh = new JSch();
				ssh.addIdentity("scheduler.priv");

				Session sshSession = ssh.getSession("ubuntu", privateIp, 22);
				java.util.Properties config = new java.util.Properties();
				config.put("StrictHostKeyChecking", "no");
				sshSession.setConfig(config);

				/*
				 * Get the `run_worker.sh` script, add the "secret" values and execute it remotely
				 */
				sshSession.connect();
				ChannelExec channel = (ChannelExec) sshSession.openChannel("exec");

				channel.setInputStream(null);
				channel.setCommand(
						getScript("run_worker.sh")
								.replace("((access_key))", settings.getProperty("aws.s3.access_key"))
								.replace("((secret_key))", settings.getProperty("aws.s3.secret_key"))
								.replace("((private_ip))", privateIp)
								.replace("((instance_id))", instanceId)
				);

				log.info("Executing the commands on the new node");
				channel.connect();

				/*
				 * Wait for the command to finish executing
				 */
				while (true) {
					if (channel.isClosed()) {
						log.info("New node start had exit code: {}", channel.getExitStatus());
						break;
					}
					try {
						Thread.sleep(1000);
					}
					catch (Exception ee) {
					}
				}
				channel.disconnect();
				sshSession.disconnect();

				log.info("Give the node two minutes to come online");
				Thread.sleep(120000);
				log.info("Node should be online!");
				return;
			}
			catch (Exception e) {
				// This may happen in case we try to connect before the node is ready. Not a big deal
				log.warn("We hit an exception, but we will retry in a few seconds");
			}
			try {
				// Sleep 6 seconds. With a total of 20 runs this gives AWS 120
				// seconds to become accessible by SSH
				Thread.sleep(6000);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This is just how we start the thing
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
