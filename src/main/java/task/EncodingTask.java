package main.java.task;

import java.io.IOException;

/**
 * Task that can be scheduled and executed.
 * 
 * 
 */
public class EncodingTask implements ITask {

	private String INPUT;
	private String OUTPUT;

	public EncodingTask(String inputFile, String outputFile) {
		this.INPUT = inputFile;
		this.OUTPUT = outputFile;
	}

	public Result executeTask() {
		ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-i", INPUT, OUTPUT);
		pb.redirectErrorStream();
		try {
			Process ffmpeg = pb.start();
			if (ffmpeg.waitFor() == 0) {
				return new Result("url", ResultType.Success);
			}
		} catch (IOException e) {
			return new Result("noURL", ResultType.Failure);
		} catch (InterruptedException e) {
			return new Result("noURL", ResultType.Failure);
		}
		return new Result("noURL", ResultType.Failure);

	}
}
