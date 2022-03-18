package pt.unl.fct.di.novasys.channel.relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.InetAddress;

public class RelayAppMessage<T> extends RelayMessage<T> {
    private final long id;
    private final byte[] fromAddress, toAddress;
    private final int fromPort, toPort;
    private final T payload;


    public RelayAppMessage(long id, Host from, Host to, T payload) {
        super(RelayMessage.Type.APP_MSG);

        this.id = id;

        fromAddress = from.getAddress().getAddress();
        fromPort = from.getPort();

        toAddress = to.getAddress().getAddress();
        toPort = to.getPort();
        
        this.payload = payload;
    }

    public long getId() { return id; }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayAppMessage>() {
        @Override
        public void serialize(RelayAppMessage msg, ByteBuf out, ISerializer innerSerializer) {
            out.writeLong(msg.id);

            out.writeInt(msg.fromAddress.length);
            out.writeBytes(msg.fromAddress);
            out.writeInt(msg.fromPort);

            out.writeInt(msg.toAddress.length);
            out.writeBytes(msg.toAddress);
            out.writeInt(msg.toPort);
        }

        @Override
        public RelayAppMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws IOException {
            long id = in.readLong();

            int fromSize = in.readInt();
            InetAddress fromAddress = InetAddress.getByAddress(in.readBytes(fromSize).array());
            int fromPort = in.readInt();
            Host from = new Host(fromAddress, fromPort);

            int toSize = in.readInt();
            InetAddress toAddress = InetAddress.getByAddress(in.readBytes(toSize).array());
            int toPort = in.readInt();
            Host to = new Host(toAddress, toPort);

            Object payload = innerSerializer.deserialize(in);

            return new RelayAppMessage(id, from, to, payload);
        }
    };
}
