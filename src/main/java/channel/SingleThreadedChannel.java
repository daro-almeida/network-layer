package channel;

import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.DefaultThreadFactory;
import network.Connection;
import network.data.Host;
import network.listeners.InConnListener;
import network.listeners.MessageListener;
import network.listeners.OutConnListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class SingleThreadedChannel<T, Y> implements IChannel<T>,
        MessageListener<Y>, OutConnListener<Y>, InConnListener<Y> {

    private static final Logger logger = LogManager.getLogger(SingleThreadedChannel.class);

    private final DefaultEventExecutor loop;

    public SingleThreadedChannel() {
        loop = new DefaultEventExecutor(new DefaultThreadFactory(SingleThreadedChannel.class));
    }

    @Override
    public void sendMessage(T msg, Host peer) {
        loop.execute(() -> onSendMessage(msg, peer));
    }

    protected abstract void onSendMessage(T msg, Host peer);

    @Override
    public void closeConnection(Host peer) {
        loop.execute(() -> onCloseConnection(peer));
    }

    protected abstract void onCloseConnection(Host peer);

    @Override
    public void inboundConnectionUp(Connection<Y> con) {
        loop.execute(() -> onInboundConnectionUp(con));
    }

    protected abstract void onInboundConnectionUp(Connection<Y> con);

    @Override
    public void inboundConnectionDown(Connection<Y> con, Throwable cause) {
        loop.execute(() -> onInboundConnectionDown(con, cause));
    }

    protected abstract void onInboundConnectionDown(Connection<Y> con, Throwable cause);

    @Override
    public final void serverSocketBind(boolean success, Throwable cause) {
        loop.execute(() -> onServerSocketBind(success, cause));
    }

    protected abstract void onServerSocketBind(boolean success, Throwable cause);

    @Override
    public final void serverSocketClose(boolean success, Throwable cause) {
        loop.execute(() -> onServerSocketClose(success, cause));
    }

    protected abstract void onServerSocketClose(boolean success, Throwable cause);

    @Override
    public void deliverMessage(Y msg, Connection<Y> conn) {
        loop.execute(() -> onDeliverMessage(msg, conn));
    }

    protected abstract void onDeliverMessage(Y msg, Connection<Y> conn);

    @Override
    public void outboundConnectionUp(Connection<Y> con) {
        loop.execute(() -> onOutboundConnectionUp(con));
    }

    protected abstract void onOutboundConnectionUp(Connection<Y> conn);

    @Override
    public void outboundConnectionDown(Connection<Y> con, Throwable cause) {
        loop.execute(() -> onOutboundConnectionDown(con, cause));
    }

    protected abstract void onOutboundConnectionDown(Connection<Y> conn, Throwable cause);

    @Override
    public void outboundConnectionFailed(Connection<Y> con, Throwable cause) {
        loop.execute(() -> onOutboundConnectionFailed(con, cause));
    }

    protected abstract void onOutboundConnectionFailed(Connection<Y> conn, Throwable cause);
}
