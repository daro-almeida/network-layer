package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public abstract class ProxyMessage<T>  {

    public enum Type {
        APP_MSG(0, ProxyAppMessage.serializer),
        CONN_OPEN(1, ProxyConnectionOpenMessage.serializer),
        CONN_CLOSE(2, ProxyConnectionCloseMessage.serializer),
        CONN_ACCEPT(3, ProxyConnectionAcceptMessage.serializer),
        CONN_FAIL(4, ProxyConnectionFailMessage.serializer),
        PEER_DISCONNECTED(5, ProxyPeerDisconnectedMessage.serializer);

        public final int opCode;
        public final IProxySerializer<ProxyMessage> serializer;
        private static final Type[] opcodeIdx;

        static {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opCode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values()) {
                if (opcodeIdx[type.opCode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opCode] = type;
            }
        }

        Type(int opCode, IProxySerializer<ProxyMessage> serializer){
            this.opCode = opCode;
            this.serializer = serializer;
        }

        public static Type fromOpcode(int opcode) {
            if (opcode >= opcodeIdx.length || opcode < 0)
                throw new AssertionError(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new AssertionError(String.format("Unknown opcode %d", opcode));
            return t;
        }
    }

    private final Type type;
    protected Host from, to;
    private static int seqNInc = 0;
    protected int seqN;

    public ProxyMessage(Host from, Host to, Type type){
        this(seqNInc++, from, to, type);
    }

    protected ProxyMessage(int seqN, Host from, Host to, Type type) {
        this.type = type;
        this.from = from;
        this.to = to;
        this.seqN = seqN;
    }

    public Type getType() {
        return type;
    }

    public Host getFrom() {
        return from;
    }

    public Host getTo() {
        return to;
    }

    @Override
    public String toString() {
        return "type=" + type.name() +
                ", from=" + from +
                ", to=" + to;
    }

    public interface IProxySerializer<T extends ProxyMessage> {
        void serialize(T msg,  ByteBuf out, ISerializer innerSerializer) throws IOException;
        T deserialize(int seqN, Host from, Host to, ByteBuf in, ISerializer innerSerializer) throws IOException;
    }
}
