package pt.unl.fct.di.novasys.channel.relay.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class RelayControlMessage<T> extends RelayMessage<T> {
    
    private final long id;
    private final byte[] fromAddress, toAddress;
    private final int fromPort, toPort;

    
    public RelayControlMessage(long id, Host from, Host to) {
        super(Type.CTRL);

        this.id = id;

        fromAddress = from.getAddress().getAddress();
        fromPort = from.getPort();

        toAddress = to.getAddress().getAddress();
        toPort = to.getPort();
    }
    
    public long getId() { return id; }

    public static final IRelaySerializer serializer = new IRelaySerializer<RelayControlMessage>() {
        @Override
        public void serialize(RelayControlMessage msg, ByteBuf out, ISerializer innerSerializer) {
            out.writeLong(msg.id);

            out.writeInt(msg.fromAddress.length);
            out.writeBytes(msg.fromAddress);
            out.writeInt(msg.fromPort);

            out.writeInt(msg.toAddress.length);
            out.writeBytes(msg.toAddress);
            out.writeInt(msg.toPort);
        }

        @Override
        public RelayControlMessage deserialize(ByteBuf in, ISerializer innerSerializer) throws UnknownHostException {
            long id = in.readLong();

            int fromSize = in.readInt();
            InetAddress fromAddress = InetAddress.getByAddress(in.readBytes(fromSize).array());
            int fromPort = in.readInt();
            Host from = new Host(fromAddress, fromPort);

            int toSize = in.readInt();
            InetAddress toAddress = InetAddress.getByAddress(in.readBytes(toSize).array());
            int toPort = in.readInt();
            Host to = new Host(toAddress, toPort);
            
            return new RelayControlMessage(id, from, to);
        }
    };
}
