package pt.unl.fct.di.novasys.channel.emulation.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class EmulatedConnectionFailMessage extends EmulatedMessage {

	public static final IEmulatedSerializer serializer = new IEmulatedSerializer<EmulatedConnectionFailMessage>() {
		@Override
		public void serialize(EmulatedConnectionFailMessage msg, ByteBuf out, ISerializer innerSerializer) {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public EmulatedConnectionFailMessage deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in, ISerializer innerSerializer) {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new EmulatedConnectionFailMessage(seqN, from, to, sentTime, new IOException(message));
		}
	};
	private final Throwable cause;

	protected EmulatedConnectionFailMessage(int seqN, Host from, Host to, long sentTime, Throwable cause) {
		super(seqN, from, to, sentTime, Type.CONN_FAIL);
		this.cause = cause;
	}

	public Throwable getCause() {
		return cause;
	}
}
