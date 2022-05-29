package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

@SuppressWarnings("rawtypes")
public class ProxyConnectionAcceptMessage extends ProxyMessage {

	public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionAcceptMessage>() {
		@Override
		public void serialize(ProxyConnectionAcceptMessage msg, ByteBuf out, ISerializer innerSerializer) {
			//nothing to be done
		}

		@Override
		public ProxyConnectionAcceptMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) {
			return new ProxyConnectionAcceptMessage(seqN, from, to);
		}
	};

	public ProxyConnectionAcceptMessage(Host from, Host to) {
		super(from, to, Type.CONN_ACCEPT);
	}

	protected ProxyConnectionAcceptMessage(int seqN, Host from, Host to) {
		super(seqN, from, to, Type.CONN_ACCEPT);
	}
}
