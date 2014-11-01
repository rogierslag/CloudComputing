package scheduler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.joda.time.DateTime;

/**
 * Created by Rogier on 01-11-14.
 */
@Slf4j
public class Provisioner {

	private final List<Node> nodes;
	private final Properties properties;
	private final AmazonEC2Client ec2Client;
	private final List<Task> taskQueue;
	private final int maxWaitingTime;
	private final int maxTasksNodesRatio;
	private final Scheduler parent;

	private boolean isDestroying = false;
	private boolean isCreating = false;
	private int maxNodes;


	public Provisioner(Scheduler parent, ScheduledExecutorService executorService, List<Node> nodes,
			List<Task> taskQueue,
			AWSCredentials awsCredentials,
			Properties properties) {
		this.parent = parent;
		this.nodes = nodes;
		this.properties = properties;
		this.taskQueue = taskQueue;

		this.maxWaitingTime = Integer.parseInt(properties.getProperty("scheduler.max_waiting_time", "600"));
		this.maxTasksNodesRatio = Integer.parseInt(properties.getProperty("scheduler.max_ratio", "3"));
		this.maxNodes = Integer.parseInt(properties.getProperty("provision.max_nodes", "2"));

		ec2Client = new AmazonEC2Client(awsCredentials);
		ec2Client.setEndpoint("ec2.eu-west-1.amazonaws.com");

		executorService.scheduleWithFixedDelay(checkForNodeAdjustments(), 5,
				Integer.parseInt(properties.getProperty("provision.check_load_interval", "15")), TimeUnit.SECONDS);
	}

	/**
	 * Checks if we need to adjust our node count and does do if required. The log messages make this methods
	 * relatively
	 * self-explanatory
	 *
	 * @return Runnable for the scheduler
	 */
	private Runnable checkForNodeAdjustments() {
		return new Runnable() {
			@Override
			public void run() {
				log.info("Going to check how busy we are");
				List<Task> waitingTasks = parent.waitingTasks();

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
				else if (!isCreating && nodes.size() < maxNodes && waitingTasks.size() > 0 &&
						waitingTasks.get(0).getCreated_at().isBefore(new DateTime().minusSeconds(maxWaitingTime))) {
					log.info("A task was waiting for more than {} seconds, which is long, so we need a new node",
							maxWaitingTime);
					provisionNewNode();
				}
				else if (!isCreating && nodes.size() < maxNodes
						&& waitingTasks.size() > maxTasksNodesRatio * nodes.size()) {
					log.info("There are currently {} times more tasks than nodes, so we should add capacity",
							maxTasksNodesRatio);
					provisionNewNode();
				}
				else {
					log.info("Apparantly we do not need more nodes than we already have: {}", nodes);
				}
			}
		};
	}

	/**
	 * Destroy a node it possible (no force)
	 *
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

		for (Task t : remove.getAssignedTasks()) {
			t.setStatus(Task.Status.QUEUED);
			t.setAssignedNode(null);
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
			new Thread(startNode(privateIp, instanceId, properties)).start();
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
	}

	/**
	 * Reads a script from disk and returns it as String
	 *
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
			log.error("Could not read {} script", file, e);
		}
		return result;
	}

	/**
	 * Actually start the node with the software of a worker
	 *
	 * @param privateIp  the private IP addres to listen on
	 * @param instanceId The instance ID as specified by AWS
	 * @param settings   The properties to initialize with
	 * @throws JSchException In case we cannot connect
	 */
	private Runnable startNode(final String privateIp, final String instanceId,
			final Properties settings) throws JSchException {
		return new Runnable() {
			public void run() {
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

						log.info("Give the node five minutes to come online");
						for (int j = 0; j < 30; j++) {
							if (parent.getClusterHealth().isAlive(instanceId)) {
								log.info("We heard from the healthcheck this node is online, so lets stop waiting");
								break;
							}
							try {
								Thread.sleep(10000);
							}
							catch (InterruptedException e) {
							}
						}
						try {
							Node node = new Node(InetAddress.getByName(privateIp), instanceId);
							nodes.add(node);

							// Other may start to create nodes again
							isCreating = false;

							log.info("Provisioned new node '{}'", node);
							log.info("Now we have the following nodes: {}", nodes);
							log.info("Node should be online!");
							return;
						}
						catch (UnknownHostException e) {
							// Wait whut
							// When shit hits the fan, just cancel the thing
							// This one triggers if Amazon gives us a non-existing IP
							// Or if we did not get a final IP in 120 seconds
							log.error("Something went wrong when getting a new node, destroying instanceId {}",
									instanceId);
							List<String> list = new ArrayList<String>(1);
							list.add(instanceId);

							TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(list);
							ec2Client.terminateInstances(terminateRequest);
						}
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
		};
	}
}
