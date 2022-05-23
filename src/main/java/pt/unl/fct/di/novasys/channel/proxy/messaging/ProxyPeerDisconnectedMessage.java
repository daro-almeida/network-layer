package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyPeerDisconnectedMessage<T> extends ProxyMessage<T> {

    private final Throwable cause;

    public ProxyPeerDisconnectedMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.PEER_DISCONNECTED);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyPeerDisconnectedMessage>() {
        @Override
        public void serialize(ProxyPeerDisconnectedMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            out.writeInt(msg.cause.getMessage().getBytes().length);
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public ProxyPeerDisconnectedMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) throws IOException {
            int size = in.readInt();
            byte[] strBytes = new byte[size];
            in.readBytes(strBytes);
            String message = new String(strBytes);

            return new ProxyPeerDisconnectedMessage<>(from, to, new IOException(message));
        }
    };
}
