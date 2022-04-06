package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyPeerDeadMessage<T> extends ProxyMessage<T> {

    private final Throwable cause;

    public ProxyPeerDeadMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.PEER_DEAD);

        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyPeerDeadMessage>() {
        @Override
        public void serialize(ProxyPeerDeadMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);

            out.writeInt(msg.cause.getMessage().getBytes().length);
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public ProxyPeerDeadMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws IOException {
            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            int size = in.readInt();
            byte[] strBytes = new byte[size];
            in.readBytes(strBytes);
            String message = new String(strBytes);

            return new ProxyPeerDeadMessage<>(from, to, new Throwable(message));
        }
    };
}
