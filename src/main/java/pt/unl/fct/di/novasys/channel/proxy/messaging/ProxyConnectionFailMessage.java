package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyConnectionFailMessage<T> extends ProxyMessage<T> {

    private final Throwable cause;

    public ProxyConnectionFailMessage(Host from, Host to, Throwable cause) {
        super(from, to, Type.CONN_FAIL);
        this.cause = cause;
    }

    protected ProxyConnectionFailMessage(int seqN, Host from, Host to, Throwable cause) {
        super(seqN, from, to, Type.CONN_FAIL);
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyConnectionFailMessage>() {
        @Override
        public void serialize(ProxyConnectionFailMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            out.writeInt(msg.cause.getMessage().getBytes().length);
            out.writeBytes(msg.cause.getMessage().getBytes());
        }

        @Override
        public ProxyConnectionFailMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) throws IOException {
            int size = in.readInt();
            byte[] strBytes = new byte[size];
            in.readBytes(strBytes);
            String message = new String(strBytes);

            return new ProxyConnectionFailMessage<>(seqN, from, to, new IOException(message));
        }
    };
}
