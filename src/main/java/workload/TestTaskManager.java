package workload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import lombok.extern.slf4j.Slf4j;

/**
 * Manager of test files and tasks.
 * 
 */
@Slf4j
public class TestTaskManager {

	private final String testDirectory = "testvideos";
	private List<String> previousTask = new ArrayList<String>();
	private List<String> testVideos;

	public TestTaskManager() {
		// read the possible testVideos
		try {
			testVideos = listTestVideosForFolder(new File(testDirectory));
		} catch (IOException io) {
			log.error(io.getMessage());
		}
	}

	/**
	 * Get a newtask.
	 * 
	 * @return a {@link TestTask}.
	 * @throws IOException
	 */
	public TestTask getNewTask() throws IOException {
		String taskName = UUID.randomUUID().toString() + ".avi";
		previousTask.add(taskName);
		return new TestTask(taskName, getRandomTestVideo());
	}

	/**
	 * Gets randomly a previously scheduled task. If no task is previously
	 * scheduled, a new task will be created.
	 * 
	 * @return a previously scheduled {@link TestTask}.
	 * @throws IOException
	 */
	public TestTask getOldTask() throws IOException {
		if (previousTask.size() > 0) {
			Random rdm = new Random();
			String taskName = previousTask.get(rdm.nextInt(previousTask.size()));
			// A random video is added, because it will just be tretrieved from
			// the
			// cache by the system.
			return new TestTask(taskName, getRandomTestVideo());
		} else {
			return getNewTask();
		}
	}

	private String getRandomTestVideo() {
		Random rdm = new Random();
		return testVideos.get(rdm.nextInt(testVideos.size()));
	}

	public List<String> getTestVideos() {
		return testVideos;
	}

	private List<String> listTestVideosForFolder(final File folder) throws IOException {
		if (!folder.exists()) {
			throw new IOException("Directory with test videos is missing.");
		}
		log.trace("reading all videos to be used for workload generation.");
		List<String> files = new ArrayList<String>();
		for (final File fileEntry : folder.listFiles()) {
			if (!fileEntry.isDirectory()) {
				String fileName = fileEntry.getName();
				String extension = fileName.substring(fileName.lastIndexOf(".") + 1);
				if ("avi".equals(extension)) {
					files.add(folder.getName() + "/" + fileName);
				}
			}
		}
		return files;
	}
}
