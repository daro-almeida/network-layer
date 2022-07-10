package pt.unl.fct.di.novasys.channel.emulation.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

@SuppressWarnings("rawtypes")
public class EmulatedConnectionAcceptMessage extends EmulatedMessage {

	public static final IEmulatedSerializer serializer = new IEmulatedSerializer<EmulatedConnectionAcceptMessage>() {
		@Override
		public void serialize(EmulatedConnectionAcceptMessage msg, ByteBuf out, ISerializer innerSerializer) {
			//nothing to be done
		}

		@Override
		public EmulatedConnectionAcceptMessage deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in, ISerializer innerSerializer) {
			return new EmulatedConnectionAcceptMessage(seqN, from, to, sentTime);
		}
	};

	public EmulatedConnectionAcceptMessage(Host from, Host to) {
		super(from, to, Type.CONN_ACCEPT);
	}

	protected EmulatedConnectionAcceptMessage(int seqN, Host from, Host to, long sentTime) {
		super(seqN, from, to, sentTime, Type.CONN_ACCEPT);
	}
}
