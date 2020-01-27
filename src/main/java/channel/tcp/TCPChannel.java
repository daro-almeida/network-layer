package channel.tcp;

import channel.ChannelListener;
import channel.base.SingleThreadedBiChannel;
import channel.tcp.events.ConnectionFailedEvent;
import channel.tcp.events.NodeDownEvent;
import io.netty.util.concurrent.Promise;
import network.AttributeValidator;
import network.Connection;
import network.ISerializer;
import network.NetworkManager;
import network.data.Attributes;
import network.data.Host;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class TCPChannel<T> extends SingleThreadedBiChannel<T, T> implements AttributeValidator {

    private static final Logger logger = LogManager.getLogger(TCPChannel.class);
    private static final short TCP_MAGIC_NUMBER = 0x4505;

    private static final Attributes TCP_ATTRIBUTES;

    static {
        TCP_ATTRIBUTES = new Attributes();
        TCP_ATTRIBUTES.putShort("channel", TCP_MAGIC_NUMBER);
    }

    public final static int DEFAULT_PORT = 85739;

    private final NetworkManager<T> network;
    private final ChannelListener<T> listener;

    private Map<Host, Pair<Connection<T>, Queue<T>>> pendingConnections;
    private Map<Host, Connection<T>> establishedConnections;

    public TCPChannel(ISerializer<T> serializer, ChannelListener<T> list, Properties properties)
            throws UnknownHostException {
        super("TCPChannel");
        this.listener = list;

        InetAddress addr = null;
        if (properties.containsKey("address"))
            addr = Inet4Address.getByName(properties.getProperty("address"));

        int port = DEFAULT_PORT;
        if (properties.containsKey("port"))
            port = Integer.parseInt(properties.getProperty("port"));

        network = new NetworkManager<>(serializer, this,
                1000, 3000, 1000);

        if (addr != null)
            network.createServerSocket(this, new Host(addr, port), this);

        pendingConnections = new HashMap<>();
        establishedConnections = new HashMap<>();
    }

    @Override
    protected void onSendMessage(T msg, Host peer) {

        Connection<T> established = establishedConnections.get(peer);
        if (established != null) {
            Promise<Void> promise = loop.newPromise();
            promise.addListener(future -> {
                if (!future.isSuccess())
                    listener.messageFailed(msg, peer, future.cause());
                else
                    listener.messageSent(msg, peer);
            });
            established.sendMessage(msg, promise);
        } else {
            Pair<Connection<T>, Queue<T>> pair = pendingConnections.computeIfAbsent(peer, k ->
                    Pair.of(network.createConnection(peer, TCP_ATTRIBUTES, this), new LinkedList<>()));
            pair.getValue().add(msg);
        }
    }

    @Override
    protected void onCloseConnection(Host peer) {
        Pair<Connection<T>, Queue<T>> remove = pendingConnections.remove(peer);
        if (remove != null) remove.getKey().disconnect();

        Connection<T> established = establishedConnections.remove(peer);
        if (established != null) established.disconnect();
    }

    @Override
    protected void onOutboundConnectionUp(Connection<T> conn) {
        Pair<Connection<T>, Queue<T>> remove = pendingConnections.remove(conn.getPeer());
        if (remove == null) throw new RuntimeException("Pending null in connection up");
        logger.debug("Outbound established: " + conn);


        Connection<T> put = establishedConnections.put(conn.getPeer(), conn);
        if (put != null) throw new RuntimeException("Connection already exists in connection up");

        for (T t : remove.getValue()) {
            Promise<Void> promise = loop.newPromise();
            promise.addListener(future -> {
                if (!future.isSuccess())
                    listener.messageFailed(t, conn.getPeer(), future.cause());
                else
                    listener.messageSent(t, conn.getPeer());
            });
            conn.sendMessage(t, promise);
        }
    }

    @Override
    protected void onOutboundConnectionDown(Connection<T> conn, Throwable cause) {
        Connection<T> remove = establishedConnections.remove(conn.getPeer());
        if (remove == null) throw new RuntimeException("Connection down with no context available");

        listener.deliverEvent(new NodeDownEvent(conn.getPeer(), cause));
    }

    @Override
    protected void onOutboundConnectionFailed(Connection<T> conn, Throwable cause) {
        if (establishedConnections.containsKey(conn.getPeer()))
            throw new RuntimeException("Connection exists in conn failed");

        Pair<Connection<T>, Queue<T>> remove = pendingConnections.remove(conn.getPeer());
        if (remove == null) throw new RuntimeException("Connection failed with no pending");

        listener.deliverEvent(new ConnectionFailedEvent<>(conn.getPeer(), remove.getRight(), cause));
    }

    @Override
    protected void onInboundConnectionUp(Connection<T> con) {
        logger.debug("Inbound up: " + con);
    }

    @Override
    protected void onInboundConnectionDown(Connection<T> con, Throwable cause) {
        logger.debug("Inbound down: " + con + " ... " + cause);
    }

    @Override
    public void onServerSocketBind(boolean success, Throwable cause) {
        if (success)
            logger.debug("Server socket ready");
        else
            logger.error("Server socket bind failed: " + cause);
    }

    @Override
    public void onServerSocketClose(boolean success, Throwable cause) {
        logger.debug("Server socket closed. " + (success ? "" : "Cause: " + cause));
    }

    @Override
    public void onDeliverMessage(T msg, Connection<T> conn) {
        listener.deliverMessage(msg, conn.getPeer());
    }

    @Override
    public boolean validateAttributes(Attributes attr) {
        Short channel = attr.getShort("channel");
        return channel != null && channel == TCP_MAGIC_NUMBER;
    }
}