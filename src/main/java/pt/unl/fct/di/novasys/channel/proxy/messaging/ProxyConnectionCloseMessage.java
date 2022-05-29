package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

@SuppressWarnings("rawtypes")
public class ProxyConnectionCloseMessage extends ProxyMessage {

	public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionCloseMessage>() {
		@Override
		public void serialize(ProxyConnectionCloseMessage msg, ByteBuf out, ISerializer innerSerializer) {
			out.writeInt(msg.cause.getMessage().getBytes().length);
			out.writeBytes(msg.cause.getMessage().getBytes());
		}

		@Override
		public ProxyConnectionCloseMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) {
			int size = in.readInt();
			byte[] strBytes = new byte[size];
			in.readBytes(strBytes);
			String message = new String(strBytes);

			return new ProxyConnectionCloseMessage(seqN, from, to, new IOException(message));
		}
	};
	private final Throwable cause;

	public ProxyConnectionCloseMessage(Host from, Host to, Throwable cause) {
		super(from, to, Type.CONN_CLOSE);
		this.cause = cause;
	}

	protected ProxyConnectionCloseMessage(int seqN, Host from, Host to, Throwable cause) {
		super(seqN, from, to, Type.CONN_CLOSE);
		this.cause = cause;
	}

	public Throwable getCause() {
		return cause;
	}
}
