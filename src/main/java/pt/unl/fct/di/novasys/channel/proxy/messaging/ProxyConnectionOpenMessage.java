package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyConnectionOpenMessage<T> extends ProxyMessage<T> {

    public ProxyConnectionOpenMessage(Host from, Host to) {
        super(from, to, Type.CONN_OPEN);
    }

    public ProxyConnectionOpenMessage(int seqN, Host from, Host to) {
        super(seqN, from, to, Type.CONN_OPEN);
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionOpenMessage>() {
        @Override
        public void serialize(ProxyConnectionOpenMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            //nothing to do here
        }

        @Override
        public ProxyConnectionOpenMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) throws IOException {
            return new ProxyConnectionOpenMessage<>(seqN, from, to);
        }
    };
}
