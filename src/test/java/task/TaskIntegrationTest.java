package test.java.task;

import java.io.File;

import main.java.task.EncodingTask;
import main.java.task.Result;
import main.java.task.ResultType;

import org.junit.Assert;
import org.junit.Test;

public class TaskIntegrationTest {

	@Test
	public void test_executeTask() {
		// Arrange
		EncodingTask task = new EncodingTask();
		// Act
		Result result = task.executeTask();
		// Assert
		Assert.assertEquals(result.type, ResultType.Success);
		File f = new File("video1.avi");
		Assert.assertTrue(f.exists());
	}

}
