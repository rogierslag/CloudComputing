package workload;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

/**
 * This integration test depends on having the test zip file installed in the
 * root directory.
 * 
 */
public class TestTaskManagerTest {

	@Test
	public void getTestVideos_getCorrectFiles() {
		// Arrange
		TestTaskManager target = new TestTaskManager();
		// Act
		List<String> result = target.getTestVideos();
		// Assert
		Assert.assertEquals("testVideo.avi", result.get(0));
	}

}
