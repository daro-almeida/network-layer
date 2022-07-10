package pt.unl.fct.di.novasys.channel.emulation.messaging;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public abstract class EmulatedMessage {

	private static int seqNInc = 0;
	private final Host from;
	private final Host to;
	private final int seqN;
	private final Type type;
	private final long sentTime;

	protected EmulatedMessage(Host from, Host to, Type type) {
		this(seqNInc++, from, to, type);
	}

	protected EmulatedMessage(int seqN, Host from, Host to, Type type) {
		this(seqN, from, to, System.currentTimeMillis(), type);
	}

	protected EmulatedMessage(int seqN, Host from, Host to, long sentTime, Type type) {
		this.type = type;
		this.from = from;
		this.to = to;
		this.seqN = seqN;
		this.sentTime = sentTime;
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

	public int getSeqN() {
		return seqN;
	}

	public long getSentTime() {
		return sentTime;
	}

	@Override
	public String toString() {
		return "type=" + type.name() +
				", from=" + from +
				", to=" + to;
	}

	public enum Type {
		APP_MSG(0, EmulatedAppMessage.serializer),
		CONN_OPEN(1, EmulatedConnectionOpenMessage.serializer),
		CONN_CLOSE(2, EmulatedConnectionCloseMessage.serializer),
		CONN_ACCEPT(3, EmulatedConnectionAcceptMessage.serializer),
		CONN_FAIL(4, EmulatedConnectionFailMessage.serializer),
		PEER_DISCONNECTED(5, EmulatedPeerDisconnectedMessage.serializer);

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

		public final int opCode;
		public final IEmulatedSerializer<EmulatedMessage> serializer;

		Type(int opCode, IEmulatedSerializer<EmulatedMessage> serializer) {
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

	@SuppressWarnings("rawtypes")
	public interface IEmulatedSerializer<T extends EmulatedMessage> {
		void serialize(T msg, ByteBuf out, ISerializer innerSerializer) throws IOException;

		T deserialize(int seqN, Host from, Host to, long sentTime, ByteBuf in, ISerializer innerSerializer) throws IOException;
	}
}
