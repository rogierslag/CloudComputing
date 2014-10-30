package communication;

/**
 * Created by Rogier on 31-10-14.
 */
public interface IMessageHandler {

	public void handleMessage(ClusterMessage m);
	public void sendMessage(ClusterMessage m);

}
