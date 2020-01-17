package network.pipeline;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import network.data.Attributes;
import network.messaging.NetworkMessage;
import network.messaging.control.ControlMessage;
import network.messaging.control.FirstHandshakeMessage;
import network.userevents.HandshakeCompleted;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class OutHandshakeHandler extends ChannelDuplexHandler {

    private static final Logger logger = LogManager.getLogger(OutHandshakeHandler.class);

    private Attributes attrs;

    public OutHandshakeHandler(Attributes attrs) {
        this.attrs = attrs;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.channel().writeAndFlush(new NetworkMessage(NetworkMessage.CTRL_MSG, new FirstHandshakeMessage(attrs)));
        ctx.fireChannelActive();
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        ctx.write(msg, promise.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {

        NetworkMessage msg = (NetworkMessage) obj;
        if (msg.code != NetworkMessage.CTRL_MSG)
            throw new Exception("Received application message in handshake: " + msg);
        ControlMessage cMsg = (ControlMessage) msg.payload;
        if(cMsg.type == ControlMessage.Type.SECOND_HS) {
            ctx.fireUserEventTriggered(new HandshakeCompleted(attrs));
            ctx.pipeline().remove(this);
        } else {
            throw new Exception("Received unexpected message in handshake: " + msg);
        }
    }
}
