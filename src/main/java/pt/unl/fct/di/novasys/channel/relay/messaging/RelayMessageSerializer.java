package pt.unl.fct.di.novasys.channel.relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;

public class RelayMessageSerializer<T> implements ISerializer<RelayMessage<T>> {

    private final ISerializer<T> innerSerializer;

    public RelayMessageSerializer(ISerializer<T> innerSerializer){
        this.innerSerializer = innerSerializer;
    }

    @Override
    public void serialize(RelayMessage<T> relayMessage, ByteBuf out) throws IOException {
        out.writeInt(relayMessage.getType().opCode);
        relayMessage.getType().serializer.serialize(relayMessage, out, innerSerializer);
    }

    @Override
    public RelayMessage<T> deserialize(ByteBuf in) throws IOException {
        RelayMessage.Type type = RelayMessage.Type.fromOpcode(in.readInt());
        return type.serializer.deserialize(in, innerSerializer);
    }

}
