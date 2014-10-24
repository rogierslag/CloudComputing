package workload;

import java.io.File;
import java.io.IOException;

/**
 * Creates a temporary avi file with handles to destroy again.
 * 
 */
public class TestTask {

	public String fileName;
	public File file;

	public TestTask(String newFileName, String OldFileName) throws IOException {
		// Retrieve the video
		File oldFile = new File(OldFileName);
		if (!oldFile.exists()) {
			throw new IOException("Video:" + OldFileName + " is missing.");
		}
		this.file = oldFile;
		this.fileName = newFileName;
	}
}
