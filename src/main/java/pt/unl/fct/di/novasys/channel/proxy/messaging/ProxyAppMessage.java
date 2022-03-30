package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;

public class ProxyAppMessage<T> extends ProxyMessage<T> {
    private final T payload;

    public ProxyAppMessage(Host from, Host to, T payload) {
        super(from,to,Type.APP_MSG);

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
            Host.serializer.serialize(msg.from, out);
            Host.serializer.serialize(msg.to, out);

            if(innerSerializer != null)
                innerSerializer.serialize(msg.payload, out);
            else
                out.writeBytes(SerializationUtils.serialize((Serializable) msg.payload));
        }

        @Override
        public ProxyAppMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws IOException {

            Host from = Host.serializer.deserialize(in);
            Host to = Host.serializer.deserialize(in);

            Object payload;
            if(innerSerializer != null)
                payload = innerSerializer.deserialize(in);
            else
                payload = in.readBytes(in.readableBytes());

            return new ProxyAppMessage(from, to, payload);
        }
    };
}
