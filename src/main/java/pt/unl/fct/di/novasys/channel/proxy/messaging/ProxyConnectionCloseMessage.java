package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyConnectionCloseMessage<T> extends ProxyMessage<T> {

    private Throwable cause;

    public ProxyConnectionCloseMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.CONN_FAIL);

        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionCloseMessage>() {
        @Override
        public void serialize(ProxyConnectionCloseMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);
            out.writeInt(msg.cause.getMessage().length());
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public ProxyConnectionCloseMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);
            int size = in.readInt();
            String message = String.valueOf(in.readBytes(size));

            return new ProxyConnectionCloseMessage<>(from, to, new Throwable(message));
        }
    };
}
