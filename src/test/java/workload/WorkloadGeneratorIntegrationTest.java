package workload;

import java.io.IOException;

import main.Main;

import org.junit.Assert;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;

public class WorkloadGeneratorIntegrationTest {

	@Test
	public void test_generateTask() throws IOException {
		// Arrange
		Main main = new Main();
		AmazonS3Client s3Client = new AmazonS3Client(main.credentials);
		String inputBucket = main.properties.getProperty("aws.s3" + ".input");

		// Ensure bucket is empty
		ObjectListing list = s3Client.listObjects(inputBucket);
		Assert.assertEquals("Input bucket is not empty", 0, list.getObjectSummaries().size());

		// Act
		String taskName = new WorkLoadGenerator(main.credentials, main.properties).generateTask();

		// Assert
		// Ensure bucket contains the test file
		list = s3Client.listObjects(inputBucket);
		Assert.assertEquals("Input bucket doest not have the test file", 1, list.getObjectSummaries().size());

		// Clean up the object
		s3Client.deleteObject(inputBucket, taskName);

	}
}
