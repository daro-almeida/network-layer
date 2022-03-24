package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ProxyMessageSerializer<T> implements ISerializer<ProxyMessage<T>> {

    private final ISerializer<T> innerSerializer;

    public ProxyMessageSerializer(ISerializer<T> innerSerializer){
        this.innerSerializer = innerSerializer;
    }

    @Override
    public void serialize(ProxyMessage<T> proxyMessage, ByteBuf out) throws IOException {
        out.writeInt(proxyMessage.getType().opCode);
        proxyMessage.getType().serializer.serialize(proxyMessage, out, innerSerializer);
    }

    @Override
    public ProxyMessage<T> deserialize(ByteBuf in) throws IOException {
        ProxyMessage.Type type = ProxyMessage.Type.fromOpcode(in.readInt());
        return type.serializer.deserialize(in, innerSerializer);
    }

}
