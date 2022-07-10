package pt.unl.fct.di.novasys.channel.emulation.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class EmulatedMessageSerializer<T> implements ISerializer<EmulatedMessage> {

	private final ISerializer<T> innerSerializer;
	//private static final Logger logger = LogManager.getLogger(EmulatedMessageSerializer.class);

	public EmulatedMessageSerializer(ISerializer<T> innerSerializer) {
		this.innerSerializer = innerSerializer;
	}

	@Override
	public void serialize(EmulatedMessage emulatedMessage, ByteBuf out) throws IOException {
		out.writeInt(emulatedMessage.getType().opCode);
		out.writeInt(emulatedMessage.getSeqN());
		Host.serializer.serialize(emulatedMessage.getFrom(), out);
		Host.serializer.serialize(emulatedMessage.getTo(), out);
		out.writeLong(emulatedMessage.getSentTime());
		emulatedMessage.getType().serializer.serialize(emulatedMessage, out, innerSerializer);
		//logger.debug("Serialized {} message {} to {} from {}", emulatedMessage.getType().name(), emulatedMessage.getSeqN(), emulatedMessage.getTo(), emulatedMessage.getFrom());
	}

	@Override
	public EmulatedMessage deserialize(ByteBuf in) throws IOException {
		EmulatedMessage.Type type = EmulatedMessage.Type.fromOpcode(in.readInt());
		int seqN = in.readInt();
		Host from = Host.serializer.deserialize(in);
		Host to = Host.serializer.deserialize(in);
		long sentTime = in.readLong();
		EmulatedMessage emulatedMessage = type.serializer.deserialize(seqN, from, to, sentTime, in, innerSerializer);
		//logger.debug("Deserialized {} message {} to {} from {}", emulatedMessage.getType().name(), emulatedMessage.getSeqN(), emulatedMessage.getTo(), emulatedMessage.getFrom());
		return emulatedMessage;
	}

}
