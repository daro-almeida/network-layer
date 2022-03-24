package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyConnectionOpenMessage<T> extends ProxyMessage<T> {

    public ProxyConnectionOpenMessage(Host from, Host to) {
        super(from, to, Type.CONN_OPEN);
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionOpenMessage>() {
        @Override
        public void serialize(ProxyConnectionOpenMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);
        }

        @Override
        public ProxyConnectionOpenMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);
            
            return new ProxyConnectionOpenMessage(from, to);
        }
    };
}
