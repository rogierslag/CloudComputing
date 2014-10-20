package scheduler;

import java.net.Inet4Address;
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

	private Inet4Address ip;
	private Queue<Task> assignedTasks;
	private DateTime idle_since;

}
