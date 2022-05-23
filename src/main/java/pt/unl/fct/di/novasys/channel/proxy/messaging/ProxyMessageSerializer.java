package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.channel.proxy.ProxyChannel;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class ProxyMessageSerializer<T> implements ISerializer<ProxyMessage<T>> {

    private final ISerializer<T> innerSerializer;

    private static final Logger logger = LogManager.getLogger(ProxyMessageSerializer.class);
    public ProxyMessageSerializer(ISerializer<T> innerSerializer){
        this.innerSerializer = innerSerializer;
    }

    @Override
    public void serialize(ProxyMessage<T> proxyMessage, ByteBuf out) throws IOException {
        out.writeInt(proxyMessage.getType().opCode);
        proxyMessage.getType().serializer.serialize(proxyMessage, out, innerSerializer);
        logger.debug("Serialized message "+proxyMessage.seqN+" to "+proxyMessage.to+" from "+proxyMessage.from);
    }

    @Override
    public ProxyMessage<T> deserialize(ByteBuf in) throws IOException {
        ProxyMessage.Type type = ProxyMessage.Type.fromOpcode(in.readInt());
        ProxyMessage<T> proxyMessage = type.serializer.deserialize(in, innerSerializer);
        logger.debug("Deserialized message "+proxyMessage.seqN+" to "+proxyMessage.to+" from "+proxyMessage.from);
        return proxyMessage;
    }

}
