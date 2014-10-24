package workload;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import amazon.Credentials;

import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Generates an artificial workload for testing purposes and to serve as an
 * example.
 * 
 */
@Slf4j
public class WorkLoadGenerator implements Runnable {

	private TestTaskManager testFiles = new TestTaskManager();
	private Random random = new Random();
	private AmazonS3Client s3Client;
	private String inputBucket;
	private double newTaskThreshold;
	private int timeoutMinimum;
	private int timeoutMax;;

	public WorkLoadGenerator(Credentials awsCredentials, Properties properties) {
		this.inputBucket = properties.getProperty("aws.s3.input", "input");
		this.newTaskThreshold = Double.valueOf(properties.getProperty("workgenerator.chance_old_task ", "0.3"));
		this.timeoutMinimum = Integer.valueOf(properties.getProperty("workgenerator.timeout_min_ms ", "3000"));
		this.timeoutMax = Integer.valueOf(properties.getProperty("workgenerator.timeout_max_ms ", "10000"));

		s3Client = new AmazonS3Client(awsCredentials);
	}

	/**
	 * Generates a new or previous Task and adds this to the bucket at AWS.
	 * 
	 * @throws IOException
	 */
	public String generateTask() throws IOException {
		// Randomly decide if a previously scheduled task will be created or
		// a new task.
		log.trace("Created a new task.");
		TestTask task;
		double newTask = random.nextDouble();
		if (newTask >= newTaskThreshold) {
			// create a new Task
			log.info("requested a new task.");
			task = testFiles.getNewTask();
		} else {
			// create a previously scheduled task.
			log.info("requested a previous task.");
			task = testFiles.getOldTask();
		}
		// Ensure bucket contains the test file
		s3Client.putObject(inputBucket, task.fileName, task.file);
		return task.fileName;
	}

	public void run() {
		try {
			while (true) {
				generateTask();
				// Timeout
				int timeout = random.nextInt(timeoutMax - timeoutMinimum) + timeoutMinimum;
				Thread.sleep(timeout);
			}
		} catch (IOException io) {
			log.error(io.getMessage());
		} catch (InterruptedException ie) {
			log.error(ie.getMessage());
		}

	}
}
