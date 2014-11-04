package communication;

import java.io.Serializable;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Created by Rogier on 31-10-14.
 */
@EqualsAndHashCode
@ToString
@Data
public class ClusterMessage implements Serializable {

	private Communicator.type senderType;
	private String senderIdentifier;

	private Communicator.type receiverType;
	private String receiverIdentifier;

	private String messageType;
	private Map<String, Object> data;
}
