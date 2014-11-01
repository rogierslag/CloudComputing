package task;


import communication.Communicator;

/**
 * Interface for every schedulable Task
 */
public interface ITask {

	/**
	 * Executes the task.
	 */
	public Runnable createWorkTask(final Communicator comm);

}
