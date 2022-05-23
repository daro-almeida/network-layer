package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyAppMessage<T> extends ProxyMessage<T> {
    private final T payload;

    public ProxyAppMessage(Host from, Host to, T payload) {
        super(from,to,Type.APP_MSG);

        this.payload = payload;
    }

    protected ProxyAppMessage(int seqN, Host from, Host to, T payload) {
        super(seqN,from,to,Type.APP_MSG);

        this.payload = payload;
    }

    public T getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return super.toString() + ", payload="+payload;
    }

    public static final IProxySerializer serializer = new IProxySerializer<ProxyAppMessage>() {
        @Override
        public void serialize(ProxyAppMessage msg, ByteBuf out, ISerializer innerSerializer) throws IOException {
            int sizeIndex = out.writerIndex();
            out.writeInt(-1);

            int startIndex = out.writerIndex();

            innerSerializer.serialize(msg.payload, out);

            int serializedSize = out.writerIndex() - startIndex;
            out.markWriterIndex();
            out.writerIndex(sizeIndex);
            out.writeInt(serializedSize);
            out.resetWriterIndex();
        }

        @Override
        public ProxyAppMessage deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) throws IOException {
            Object payload = innerSerializer.deserialize(in);

            return new ProxyAppMessage<>(seqN, from, to, payload);
        }
    };
}
