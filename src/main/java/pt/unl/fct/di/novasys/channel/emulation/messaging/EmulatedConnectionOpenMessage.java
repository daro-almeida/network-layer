package pt.unl.fct.di.novasys.channel.emulation.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

@SuppressWarnings("rawtypes")
public class EmulatedConnectionOpenMessage extends EmulatedMessage {

	public static final IEmulatedSerializer serializer = new IEmulatedSerializer<EmulatedConnectionOpenMessage>() {
		@Override
		public void serialize(EmulatedConnectionOpenMessage msg, ByteBuf out, ISerializer innerSerializer) {
			//nothing to do here
		}

		@Override
		public EmulatedConnectionOpenMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) {
			return new EmulatedConnectionOpenMessage(seqN, from, to);
		}
	};

	public EmulatedConnectionOpenMessage(Host from, Host to) {
		super(from, to, Type.CONN_OPEN);
	}

	public EmulatedConnectionOpenMessage(int seqN, Host from, Host to) {
		super(seqN, from, to, Type.CONN_OPEN);
	}
}
