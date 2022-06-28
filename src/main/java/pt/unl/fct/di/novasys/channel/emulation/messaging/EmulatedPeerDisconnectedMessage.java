package pt.unl.fct.di.novasys.channel.emulation.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class EmulatedPeerDisconnectedMessage extends EmulatedMessage {

	public static final IEmulatedSerializer serializer = new IEmulatedSerializer<EmulatedPeerDisconnectedMessage>() {
		@Override
		public void serialize(EmulatedPeerDisconnectedMessage msg, ByteBuf out, ISerializer innerSerializer) {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public EmulatedPeerDisconnectedMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new EmulatedPeerDisconnectedMessage(from, to, new IOException(message));
		}
	};
	private final Throwable cause;

	public EmulatedPeerDisconnectedMessage(Host from, Host to, Throwable cause) {
		super(from, to, Type.PEER_DISCONNECTED);
		this.cause = cause;
	}

	public Throwable getCause() {
		return cause;
	}
}
