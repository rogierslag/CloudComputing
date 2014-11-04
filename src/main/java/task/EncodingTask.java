package task;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import main.Main;

import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import communication.ClusterMessage;
import communication.Communicator;

/**
 * Task that can be scheduled and executed.
 */
@Slf4j
public class EncodingTask implements ITask {

	private String inputFile;
	private String outputFile;

	public EncodingTask(String inputFile, String outputFile) {
		this.inputFile = inputFile;
		this.outputFile = outputFile;
	}

	/**
	 * Creates a runnable which can be fed into a Thread
	 * 
	 * @param comm
	 *            The communicator for reporting the status
	 * @return Thread input
	 */
	public Runnable createWorkTask(final Communicator comm) {
		return new Runnable() {
			public void run() {
				Main main = new Main();

				// This is a better and faster way to copy to S3 (otherwise
				// timeouts)
				TransferManager tx = new TransferManager(main.getCredentials());

				for (int i = 0; i < 3; i++) {
					try {
						log.trace("Copying the file to myself");
						File file = new File("/tmp/currentInputFile");
						Download dl = tx.download(main.getProperties().getProperty("aws.s3.input"), inputFile, file);
						dl.waitForCompletion();

						log.trace("Got the file locally now!");

						log.trace("Started conversion");
						EncodingTask et = new EncodingTask("/tmp/currentInputFile", "/tmp/currentOutputFile.mp4");
						Result r = et.executeTask();
						log.trace("Result was {}", r);
						if (r == Result.Success) {
							Upload ul = tx.upload(main.getProperties().getProperty("aws.s3.output"), inputFile, new File("/tmp/currentOutputFile.mp4"));
							ul.waitForCompletion();

							// Communicate back, delete from from input bucket
							log.trace("conversion was done and file waz sent back to S3");
							tx.shutdownNow();
							// s3Client.deleteObject(main.getProperties().getProperty("aws.s3.input"),inputFile);

							ClusterMessage m = new ClusterMessage();
							m.setReceiverIdentifier("scheduler");
							m.setReceiverType(Communicator.type.SCHEDULER);
							m.setMessageType("assignment-done");
							Map<String, Object> map = new HashMap<String, Object>();
							map.put("inputFile", inputFile);
							map.put("outputFile", outputFile);
							m.setData(map);
							comm.send(m);
							return;
						}
					} catch (Exception e) {
						log.error("Something went terribly wrong", e);
					}
					log.warn("Iteration {} of 3 failed.", i);
				}
				tx.shutdownNow();

				// We only get here if we did not have any success
				ClusterMessage m = new ClusterMessage();
				m.setReceiverIdentifier("scheduler");
				m.setReceiverType(Communicator.type.SCHEDULER);
				m.setMessageType("assignment-failed");
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("inputFile", inputFile);
				map.put("outputFile", outputFile);
				m.setData(map);
				comm.send(m);
			}
		};
	}

	/**
	 * Executes the conversion task
	 * 
	 * @return Well you should be able to figure that out from the Enum
	 */
	private Result executeTask() {
		ProcessBuilder pb = new ProcessBuilder("ffmpeg", "-y", "-i", inputFile, outputFile);
		pb.redirectError(new File("/tmp/error.log"));
		pb.redirectOutput(new File("/tmp/output.log"));
		try {
			Process ffmpeg = pb.start();
			log.trace(pb.command().toString());
			if (ffmpeg.waitFor() == 0) {
				return Result.Success;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return Result.Failure;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return Result.Failure;
		}
		log.warn("We did not have a good result, but no exception either. Whut?");
		return Result.Failure;
	}
}
