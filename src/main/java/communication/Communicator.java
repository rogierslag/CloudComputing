package communication;

import java.io.File;
import java.security.InvalidParameterException;

import lombok.extern.slf4j.Slf4j;
import org.jgroups.JChannel;
import org.jgroups.Message;

/**
 * Created by Rogier on 31-10-14.
 */
@Slf4j
public class Communicator {

	private JChannel channel;

	public enum type {
		SCHEDULER,
		WORKER
	}

	private Communicator.type myType;
	private String myIdentifier;

	public Communicator(IMessageHandler handler) {
		this(handler, type.SCHEDULER,"scheduler");
	}

	public Communicator(IMessageHandler handler, type type, String identifier) {
		if ( identifier == null ) {
			throw new InvalidParameterException();
		}
		myIdentifier = identifier;
		myType = type;
		try {
			channel = new JChannel(new File("jgroups_discovery.xml"));
			log.info(channel.getAddressAsString());
			channel.setReceiver(new Receiver(handler,type,identifier));
			channel.connect("CloudComputing");

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					channel.close();
				}
			});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void send(ClusterMessage m) {
		m.setSenderType(this.myType);
		m.setSenderIdentifier(this.myIdentifier);
		Message msg = new Message();
		msg.setObject(m);
		msg.setDest(null);
		try {
			channel.send(msg);
//			log.info("Just sent the message with contents: {}", m);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}