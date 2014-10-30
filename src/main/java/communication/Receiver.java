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

	public Receiver(IMessageHandler handler, Communicator.type type, String identifier) {
		this.handler = handler;
		this.type = type;
		this.me = identifier;
	}

	public void receive(final Message msg) {
//		log.info("received msg from " + msg.getSrc() + ": " + msg.getObject());
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
				}
			};
			new Thread(reply).start();
			return;
		}

		if (message.getMessageType().equals("healthcheck-response")) {
			this.handler.handleMessage(message);
			return;
		}

		// Ignore messages send by me
		if (message.getSenderType() == this.type && this.me.equals(message.getSenderIdentifier())) {
//			log.info("I send this message myself");
			return;
		}

		// Other classes
		if (message.getReceiverType() == this.type && this.me.equals(message.getReceiverIdentifier())) {
			this.handler.handleMessage(message);
			return;
		}

		log.error("A ClusterMessage was not processed by a receiver: {}", message);
	}
}