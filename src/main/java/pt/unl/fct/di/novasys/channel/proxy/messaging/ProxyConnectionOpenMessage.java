package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

@SuppressWarnings("rawtypes")
public class ProxyConnectionOpenMessage extends ProxyMessage {

	public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionOpenMessage>() {
		@Override
		public void serialize(ProxyConnectionOpenMessage msg, ByteBuf out, ISerializer innerSerializer) {
			//nothing to do here
		}

		@Override
		public ProxyConnectionOpenMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) {
			return new ProxyConnectionOpenMessage(seqN, from, to);
		}
	};

	public ProxyConnectionOpenMessage(Host from, Host to) {
		super(from, to, Type.CONN_OPEN);
	}

	public ProxyConnectionOpenMessage(int seqN, Host from, Host to) {
		super(seqN, from, to, Type.CONN_OPEN);
	}
}
