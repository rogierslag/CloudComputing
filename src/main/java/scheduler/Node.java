package scheduler;

import java.net.InetAddress;
import java.util.LinkedList;
import java.util.Queue;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by Rogier on 17-10-14.
 */
@AllArgsConstructor
@Data
public class Node {

	private InetAddress ip;
	private Queue<Task> assignedTasks;
	private String instanceId;

	public Node(InetAddress ip, String instanceId) {
		this.ip = ip;
		this.instanceId = instanceId;
		this.assignedTasks = new LinkedList<Task>();
	}

	public boolean isIdle() {
		return this.assignedTasks.isEmpty();
	}

	public String toString() {
		return instanceId;
	}

}
