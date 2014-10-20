package scheduler;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;
import org.joda.time.DateTime;

/**
 * Created by Rogier on 17-10-14.
 */
@AllArgsConstructor
@Data
@ToString
public class Task {

	private String inputFile;
	private Node assignedNode;
	private String outputBucket;
	private String outputFile;
	private Task.Status status;
	private DateTime created_at;

	enum Status {
		QUEUED,
		STARTED,
		FAILED,
		FINISHED
	}

	public Task(String inputFile) {
		this.inputFile = inputFile;
	}

	public boolean equals(Object other) {
		if (other instanceof Task) {
			Task that = (Task) other;
			return this.inputFile.equals(that.inputFile);
		}
		return false;

	}

}
