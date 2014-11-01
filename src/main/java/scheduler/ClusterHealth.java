package scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import communication.ClusterMessage;
import communication.Communicator;
import communication.IMessageHandler;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by Rogier on 01-11-14.
 */
@Slf4j
public class ClusterHealth implements IMessageHandler {

	private final List<Node> nodes;
	private final Scheduler parent;
	private final Communicator communicator;
	private final int chaosProbability;

	private ArrayList<String> healthcheckReplies;
	private HashMap<String, Object> healthcheckMap;
	private HashMap<String, Integer> missedHealthchecks = new HashMap<String, Integer>();
	private int maxMissedHealthchecks;

	public ClusterHealth(Scheduler parent, ScheduledExecutorService executorService, List<Node> nodes,
			Properties properties) {
		this.parent = parent;
		this.nodes = nodes;

		this.maxMissedHealthchecks = Integer.parseInt(properties.getProperty("cluster_health.max_missed_checks", "5"));
		this.chaosProbability = Integer.parseInt(properties.getProperty("cluster_health.chaos_probability", "0"));

		this.communicator = new Communicator(this, Communicator.type.HEALTH_CHECK, "healthcheck");
		executorService.scheduleWithFixedDelay(checkForClusterLiveness(), 5, Integer.parseInt(
				properties.getProperty("cluster_health.interval", "5")), TimeUnit.SECONDS);
		executorService.scheduleWithFixedDelay(causeChaos(), 30, Integer.parseInt(
				properties.getProperty("cluster_health.chaos_interval", "60")), TimeUnit.SECONDS);
	}

	private Runnable causeChaos() {
		return new Runnable() {
			public void run() {
				// Determine whether we should actually do something now

				Random r = new Random();
				if (r.nextInt(100) < chaosProbability) {
					log.warn("Here comes chaos!");
					synchronized (nodes) {
						int elementNumber = r.nextInt(nodes.size());
						Node byebye = nodes.get(elementNumber);
						log.warn("ChoasMonkey killed node {}. Mwahaha", byebye);
						parent.getProvisioner().destroyExistingNode(byebye, true);
					}
				}

			}
		};
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
							nodeMissedHealthcheck(toTerminate, n);
						}
						else {
							missedHealthchecks.remove(n.getInstanceId());
						}
					}
					for (Node n : toTerminate) {
						log.info("Destroying node {}", n);
						parent.getProvisioner().destroyExistingNode(n, true);
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

	private void nodeMissedHealthcheck(List<Node> toTerminate, Node n) {
		if (missedHealthchecks.containsKey(n.getInstanceId())) {
			int missedCount = missedHealthchecks.get(n.getInstanceId()) + 1;
			if (missedCount > maxMissedHealthchecks) {
				log.warn("{} missed over {} healthchecks, terminating", n, maxMissedHealthchecks);
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

	@Override
	public void handleMessage(ClusterMessage m) {
		if (m.getMessageType().equals("healthcheck-response")) {
			//			log.info("Received HC: {}",m);
			receivedHealthCheckReply(m);
		}
	}

	@Override
	public void sendMessage(ClusterMessage m) {
		this.communicator.send(m);
	}

	/**
	 * Process a healthcheck response message
	 *
	 * @param m the response
	 */
	private void receivedHealthCheckReply(ClusterMessage m) {
		if (m.getData().equals(this.healthcheckMap) && !this.healthcheckReplies.contains(m.getSenderIdentifier())) {
			this.healthcheckReplies.add(m.getSenderIdentifier());
		}
	}
}
