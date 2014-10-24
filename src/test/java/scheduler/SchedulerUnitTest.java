package scheduler;

/**
 * Created by Rogier on 20-10-14.
 */

import java.io.File;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import main.Main;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;

@Slf4j
public class SchedulerUnitTest {

	private Main main = new Main();

	@Test
	/**
	 * This tests whether the properties file is correctly loaded and contains the correct property values
	 */
	public void test_initialization() {
		Scheduler scheduler = new Scheduler(main.getCredentials(), main.getProperties());

		Properties properties = getPrivateField(scheduler, Properties.class, "properties");
		Assert.assertTrue(ensureProperty(properties, "aws.s3.input"));
		Assert.assertTrue(ensureProperty(properties, "aws.s3.output"));
		Assert.assertTrue(ensureProperty(properties, "aws.s3.access_key"));
		Assert.assertTrue(ensureProperty(properties, "aws.s3.secret_key"));
		scheduler.stop();
	}

	@Test
	/**
	 * This test actually runs the scheduler and checks if files are added correctly
	 *
	 * It assumes an empty bucket to start with. Once that succeeds, it starts the scheduler, checks if a file is
	 * added as should and not re-added again.
	 */
	public void test_bucketFiles() {
		long allowForNetworkDelays = 2000;

		try {
			Scheduler scheduler = new Scheduler(main.getCredentials(), main.getProperties());

			AmazonS3Client s3Client = new AmazonS3Client(main.getCredentials());
			String inputBucket = main.getProperties().getProperty("aws.s3.input");
			long pollDelay = Long.parseLong(getPrivateField(scheduler, Properties.class, "properties").getProperty("scheduler.check_every_x_seconds")) * 1000;

			// Ensure bucket is empty
			ObjectListing list = s3Client.listObjects(inputBucket);
			Assert.assertEquals("Input bucket is not empty", 0, list.getObjectSummaries().size());

			// Ensure bucket contains the test file
			s3Client.putObject(inputBucket, "unit_test.avi", new File("testVideo.avi"));
			list = s3Client.listObjects(inputBucket);
			Assert.assertEquals("Input bucket doest not have the test file", 1, list.getObjectSummaries().size());

			Thread.sleep(allowForNetworkDelays + pollDelay);
			List<Task> taskList = getPrivateField(scheduler, List.class, "taskQueue");
			Assert.assertEquals("Scheduler did not detect the file", 1, taskList.size());

			Thread.sleep(allowForNetworkDelays + pollDelay);
			taskList = getPrivateField(scheduler, List.class, "taskQueue");
			Assert.assertEquals("Scheduler lost or gained an additional file", 1, taskList.size());

			s3Client.deleteObject(inputBucket, "unit_test.avi");
			scheduler.stop();
		} catch (InterruptedException e) {
			Assert.fail("Something went horribly wrong when trying to sleep a thread");
		}
	}

	/**
	 * This is a simple helper function to check if a property exists in a
	 * Property object
	 * 
	 * @param properties
	 *            The property object
	 * @param field
	 *            The key to check
	 * @return
	 */
	private boolean ensureProperty(Properties properties, String field) {
		return properties.containsKey(field) && properties.getProperty(field) != null;
	}

	/**
	 * Gets an internal private field from a class
	 * 
	 * @param object
	 *            The class to get the field from
	 * @param klass
	 *            The class of the field (no primitives)
	 * @param fieldString
	 *            The name of the field in the class
	 * @param <T>
	 * @return The actual field
	 */
	private <T> T getPrivateField(Object object, Class<T> klass, String fieldString) {
		try {
			Class<?> instance = object.getClass();
			Field field = instance.getDeclaredField(fieldString);
			field.setAccessible(true);
			return klass.cast(field.get(object));
		} catch (NoSuchFieldException e) {
			Assert.fail("Did not correctly find the properties field");
		} catch (IllegalAccessException e) {
			Assert.fail("Did not correctly find the properties field");
		}
		return null;
	}

}
