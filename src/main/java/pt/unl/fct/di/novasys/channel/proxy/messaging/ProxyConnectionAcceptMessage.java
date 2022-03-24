package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyConnectionAcceptMessage<T> extends ProxyMessage<T> {

    public ProxyConnectionAcceptMessage(Host from, Host to) {
        super(from, to, Type.CONN_ACCEPT);
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionAcceptMessage>() {
        @Override
        public void serialize(ProxyConnectionAcceptMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);
        }

        @Override
        public ProxyConnectionAcceptMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            return new ProxyConnectionAcceptMessage(from, to);
        }
    };
}
