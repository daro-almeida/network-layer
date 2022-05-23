package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyConnectionAcceptMessage<T> extends ProxyMessage<T> {

    public ProxyConnectionAcceptMessage(Host from, Host to) {
        super(from, to, Type.CONN_ACCEPT);
    }

    protected ProxyConnectionAcceptMessage(int seqN, Host from, Host to) {
        super(seqN, from, to, Type.CONN_ACCEPT);
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionAcceptMessage>() {
        @Override
        public void serialize(ProxyConnectionAcceptMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            //nothing to be done
        }

        @Override
        public ProxyConnectionAcceptMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) throws IOException {
            return new ProxyConnectionAcceptMessage(seqN, from, to);
        }
    };
}
