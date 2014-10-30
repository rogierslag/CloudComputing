package task;

import java.io.File;
import java.io.IOException;

import task.ITask;
import task.ResultType;

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
		ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-y", "-i", INPUT, OUTPUT);
		pb.redirectError(new File("/tmp/error.log"));
		pb.redirectOutput(new File("/tmp/output.log"));
		try {
			Process ffmpeg = pb.start();
			System.out.println(pb.command());
			if (ffmpeg.waitFor() == 0) {
				return new Result("url", ResultType.Success);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return new Result("noURL", ResultType.Failure);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return new Result("noURL", ResultType.Failure);
		}
		System.out.println("whut?");
		return new Result("noURL", ResultType.Failure);

	}
}
