package scheduler;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.joda.time.DateTime;

/**
 * Created by Rogier on 17-10-14.
 */
@AllArgsConstructor
@Data
public class Node {

	private InetAddress ip;
	private Queue<Task> assignedTasks;
	private DateTime idle_since;
	private String instanceId;

	public Node(InetAddress ip, String instanceId) {
		this.ip = ip;
		this.instanceId = instanceId;
		this.assignedTasks = new LinkedList<Task>();
		this.idle_since = DateTime.now();
	}

}
