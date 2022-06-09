package pt.unl.fct.di.novasys.channel.proxy.messaging;

import io.netty.buffer.ByteBuf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class ProxyMessageSerializer<T> implements ISerializer<ProxyMessage> {

	private final ISerializer<T> innerSerializer;
	public ProxyMessageSerializer(ISerializer<T> innerSerializer) {
		this.innerSerializer = innerSerializer;
	}

	@Override
	public void serialize(ProxyMessage proxyMessage, ByteBuf out) throws IOException {
		out.writeInt(proxyMessage.getType().opCode);
		out.writeInt(proxyMessage.seqN);
		Host.serializer.serialize(proxyMessage.from, out);
		Host.serializer.serialize(proxyMessage.to, out);
		proxyMessage.getType().serializer.serialize(proxyMessage, out, innerSerializer);
	}

	@Override
	public ProxyMessage deserialize(ByteBuf in) throws IOException {
		ProxyMessage.Type type = ProxyMessage.Type.fromOpcode(in.readInt());
		int seqN = in.readInt();
		Host from = Host.serializer.deserialize(in);
		Host to = Host.serializer.deserialize(in);
		return type.serializer.deserialize(seqN, from, to, in, innerSerializer);
	}

}
