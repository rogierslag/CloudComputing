package main.java.task;

import java.io.IOException;

/**
 * Task that can be scheduled and executed.
 * 
 * 
 */
public class EncodingTask implements ITask {

	private static final String INPUT = "video1.avi";
	private static final String OUTPUT = "output1.flv";

	public Result executeTask() {
		ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-i", INPUT, OUTPUT);
		pb.redirectErrorStream();
		try {
			Process ffmpeg = pb.start();
		} catch (IOException e) {
			return new Result("noURL", ResultType.Failure);
		}
		return new Result("url", ResultType.Success);
	}
}
