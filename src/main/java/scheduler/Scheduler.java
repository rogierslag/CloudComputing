package scheduler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.Getter;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

/**
 * Created by Rogier on 17-10-14.
 */
@Slf4j
public class Scheduler {

	private final Properties properties;

	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
	private final AmazonS3Client s3Client;

	@Getter
	private final List<Task> taskQueue = new ArrayList<Task>();

	/**
	 * Create the scheduler.
	 *
	 * Reads the properties, starts and S3 client and starts scheduling the poll tasks
	 */
	public Scheduler() {
		properties = readProperties();
		log.info("Working with the following properties: {}", properties);
		final String awsAccessKey = properties.getProperty("aws.s3.access_key", null);
		final String awsSecretKey = properties.getProperty("aws.s3.secret_key", null);
		int checkInterval = 15;
		try {
			checkInterval = Integer.parseInt(properties.getProperty("scheduler.check_every_x_seconds", "15"));
		}
		catch (NumberFormatException e) {
			log.warn("Could not read properties value '{}'", "scheduler.check_every_x_seconds");
		}

		AWSCredentials awsCredentials = new AWSCredentials() {
			@Override
			public String getAWSAccessKeyId() {
				return awsAccessKey;
			}

			@Override
			public String getAWSSecretKey() {
				return awsSecretKey;
			}
		};
		s3Client = new AmazonS3Client(awsCredentials);

		log.info("Start polling buckets in {} seconds and redo every {} seconds", checkInterval, checkInterval);

		// Every task will start another one in succession. This ensures in case of a high load not all tasks are
		// interfering and makes it easier to reason about stuff and things
		executorService.schedule(this.checkForTasks(checkInterval), checkInterval, TimeUnit.SECONDS);
	}

	/**
	 * Reads the properties file and load them into a Properties object
	 * @return the created Properties object
	 */
	private Properties readProperties() {
		try {
			FileInputStream propertiesFile = new FileInputStream("scheduler.properties");
			Properties props = new Properties();
			props.load(propertiesFile);
			return props;
		}
		catch (FileNotFoundException e) {
			// A problem was found, so returning to sane defaults happens in the app
			log.error("Triggered an FileNotFound error when trying to read the properties file: {}", e);
			return new Properties();
		}
		catch (IOException e) {
			log.error("Triggered an IO error when trying to read the properties file: {}", e);
			return new Properties();
		}
	}

	/**
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
				if ( properties.containsKey("aws.s3.output")) {
					outputBucket = properties.getProperty("aws.s3.output", "output");
				} else {
					log.warn("Output bucket was not defined in properties file: {}",properties);
				}
				log.info("About to do a run of the input bucket: {}", inputBucket);

				// List the object and iterate through them
				ObjectListing list = s3Client.listObjects(inputBucket);
				for (S3ObjectSummary object : list.getObjectSummaries()) {
					Task tmp = new Task(object.getKey());
					// The equals only checks on the `inputFile` so we can safely do this
					if (!taskQueue.contains(tmp)) {
						// If we do not yet have the task, add the other data and schedule it
						tmp.setCreated_at(DateTime.now());
						tmp.setOutputBucket(outputBucket);
						tmp.setOutputFile(UUID.randomUUID() + ".some_extension");
						tmp.setStatus(Task.Status.QUEUED);
						// Should also assign to a node, but will do that later (separate method etc)
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

	public static void main(String[] args) {
		new Scheduler();
	}

}
