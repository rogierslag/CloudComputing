package communication;

import lombok.extern.slf4j.Slf4j;

import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;

/**
 * Created by Rogier on 31-10-14.
 */
@Slf4j
class Receiver extends ReceiverAdapter {

	private final IMessageHandler handler;
	private final Communicator.type type;
	private String me;

	/**
	 * Initialize a message receiver
	 * 
	 * @param handler
	 *            the IMessageHandler to handle received messages
	 * @param type
	 *            The type of communicator
	 * @param identifier
	 *            the identifier (scheduler of instance ID)
	 */
	public Receiver(IMessageHandler handler, Communicator.type type, String identifier) {
		this.handler = handler;
		this.type = type;
		this.me = identifier;
	}

	/**
	 * Receives a message and possibly relays it - Reply to healthcheck -
	 * Discard messages send by itself - Forwards messages meant for itself to
	 * IMessageHandler - Discards remaining
	 * 
	 * @param msg
	 *            the received message
	 */
	public void receive(final Message msg) {
		final ClusterMessage message = (ClusterMessage) msg.getObject();

		// Healthchecks
		if (message.getMessageType().equals("healthcheck")) {
			Runnable reply = new Runnable() {
				public void run() {
					ClusterMessage response = new ClusterMessage();

					response.setMessageType("healthcheck-response");
					response.setReceiverIdentifier(message.getSenderIdentifier());
					response.setReceiverType(message.getSenderType());
					response.setData(message.getData());
					handler.sendMessage(response);
					log.trace("healthcheck response: {}", response);
				}
			};
			new Thread(reply).start();
			return;
		}

		// Ignore messages send by me
		if (message.getSenderType() == this.type && this.me.equals(message.getSenderIdentifier())) {
			return;
		}

		// Other classes
		if (message.getReceiverType() == this.type && this.me.equals(message.getReceiverIdentifier())) {
			this.handler.handleMessage(message);
			return;
		}
	}
}