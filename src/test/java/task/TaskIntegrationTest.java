package task;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TaskIntegrationTest {

	String input = "testVideo.avi";
	String output = "outputTestVideo.flv";

	@Test
	public void test_executeTask() {
		// Arrange
		EncodingTask task = new EncodingTask(input, output);
		// Act
		Result result = task.executeTask();
		// Assert
		Assert.assertEquals(result.type, ResultType.Success);
		File f = new File(output);
		Assert.assertTrue(f.exists());
	}

	@After
	public void cleanUp() throws IOException {
		File f = new File(output);
		if (f.exists()) {
			f.delete();
		}
	}

}
