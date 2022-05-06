package pt.unl.fct.di.novasys.channel.proxy;

import io.netty.util.concurrent.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.channel.ChannelListener;
import pt.unl.fct.di.novasys.channel.base.SingleThreadedClientChannel;
import pt.unl.fct.di.novasys.channel.proxy.messaging.*;
import pt.unl.fct.di.novasys.channel.tcp.ConnectionState;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.AttributeValidator;
import pt.unl.fct.di.novasys.network.Connection;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.NetworkManager;
import pt.unl.fct.di.novasys.network.data.Attributes;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

public class ProxyChannel<T> extends SingleThreadedClientChannel<T, ProxyMessage<T>> implements AttributeValidator {

    private static final Logger logger = LogManager.getLogger(ProxyChannel.class);
    private static final short PROXY_MAGIC_NUMBER = 0x1369;

    public final static String NAME = "PROXYChannel";
    public final static String ADDRESS_KEY = "address";
    public final static String PORT_KEY = "port";
    public final static String TRIGGER_SENT_KEY = "trigger_sent";
    public final static String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval";
    public final static String HEARTBEAT_TOLERANCE_KEY = "heartbeat_tolerance";
    public final static String CONNECT_TIMEOUT_KEY = "connect_timeout";

    public final static String RELAY_ADDRESS_KEY = "relay_address"; //single relay implementation
    public final static String RELAY_PORT = "relay_port";
    public final static String DEFAULT_RELAY_PORT = "9082";

    public static final String LISTEN_ADDRESS_ATTRIBUTE = "listen_address";

    public final static String DEFAULT_PORT = "8581";
    public final static String DEFAULT_HB_INTERVAL = "0";
    public final static String DEFAULT_HB_TOLERANCE = "0";
    public final static String DEFAULT_CONNECT_TIMEOUT = "1000";

    public final static int CONNECTION_OUT = 0;
    public final static int CONNECTION_IN = 1;

    private final NetworkManager<ProxyMessage<T>> network;
    private final ChannelListener<T> listener;

    private final Attributes attributes;
    private final Host self;

    private boolean disconnected;

    private Set<Host> inConnections;
    private Map<Host, VirtualConnectionState<ProxyMessage<T>>> outConnections;

    private ConnectionState<ProxyMessage<T>> relayConnectionState;

    private final boolean triggerSent;

    public ProxyChannel(ISerializer<T> serializer, ChannelListener<T> list, Properties properties) throws IOException {
        super("ProxyChannel");
        this.listener = list;

        InetAddress addr;
        if (properties.containsKey(ADDRESS_KEY))
            addr = Inet4Address.getByName(properties.getProperty(ADDRESS_KEY));
        else
            throw new IllegalArgumentException(NAME + " requires binding address");

        int port = Integer.parseInt(properties.getProperty(PORT_KEY, DEFAULT_PORT));
        int hbInterval = Integer.parseInt(properties.getProperty(HEARTBEAT_INTERVAL_KEY, DEFAULT_HB_INTERVAL));
        int hbTolerance = Integer.parseInt(properties.getProperty(HEARTBEAT_TOLERANCE_KEY, DEFAULT_HB_TOLERANCE));
        int connTimeout = Integer.parseInt(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT));
        this.triggerSent = Boolean.parseBoolean(properties.getProperty(TRIGGER_SENT_KEY, "false"));

        Host listenAddress = new Host(addr, port);
        self = listenAddress;

        ProxyMessageSerializer<T> tProxyMessageSerializer = new ProxyMessageSerializer<>(serializer);
        network = new NetworkManager<>(tProxyMessageSerializer, this, hbInterval, hbTolerance, connTimeout);

        attributes = new Attributes();
        attributes.putShort(CHANNEL_MAGIC_ATTRIBUTE, PROXY_MAGIC_NUMBER);
        attributes.putHost(LISTEN_ADDRESS_ATTRIBUTE, listenAddress);

        connectToRelay(properties);

        disconnected = false;

        inConnections = new HashSet<>();
        outConnections = new HashMap<>();
    }

    private void connectToRelay(Properties properties) throws UnknownHostException {
        //only one relay
        InetAddress relayAddr;
        if (properties.containsKey(RELAY_ADDRESS_KEY))
            relayAddr = Inet4Address.getByName(properties.getProperty(RELAY_ADDRESS_KEY));
        else
            throw new IllegalArgumentException(NAME + ": relay address not defined.");
        int relayPort = Integer.parseInt(properties.getProperty(RELAY_PORT, DEFAULT_RELAY_PORT));

        Host relay = new Host(relayAddr, relayPort);

        //only one relay
        this.relayConnectionState = new ConnectionState<>(network.createConnection(relay, attributes, this));

    }

    @Override
    protected void onOpenConnection(Host peer) {
        if(disconnected) {
            logger.debug(self+": onOpenConnection ignored because disconnected from network.");
            return;
        }

        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
        if (conState == null) {
            logger.debug(self+": onOpenConnection creating connection to: " + peer);
            outConnections.put(peer, new VirtualConnectionState<>());
            if(relayConnectionState.getState() == ConnectionState.State.CONNECTED)
                relayConnectionState.getConnection().sendMessage(new ProxyConnectionOpenMessage<>(self,peer));
            else
                relayConnectionState.getQueue().add(new ProxyConnectionOpenMessage<>(self,peer));
        } else
            logger.debug(self+": onOpenConnection ignored: " + peer);
    }

    private void sendWithListener(ProxyAppMessage<T> msg, Host peer, Connection<ProxyMessage<T>> established) {
        Promise<Void> promise = loop.newPromise();
        promise.addListener(future -> {
            if (future.isSuccess() && triggerSent) listener.messageSent(msg.getPayload(), peer);
            else if (!future.isSuccess()) {
                loop.shutdownGracefully();
                listener.messageFailed(msg.getPayload(), peer, future.cause());
            };
        });
        established.sendMessage(msg, promise);
    }

    @Override
    protected void onSendMessage(T msg, Host peer, int connection) {
        if(disconnected) {
            logger.debug(self+": onSendMessage ignored because disconnected from network.");
            return;
        }

        ProxyAppMessage<T> appMsg = new ProxyAppMessage<>(self, peer, msg);

        if(relayConnectionState.getState() == ConnectionState.State.CONNECTING) {
            relayConnectionState.getQueue().add(appMsg);
            logger.debug(self+": sent message without connection established to relay : queueing message");
        }
        else if(relayConnectionState.getState() != ConnectionState.State.CONNECTED)
            throw new AssertionError("Not connected to relay");

        logger.debug(self+": SendMessage " + msg + " " + peer + " " + (connection == CONNECTION_IN ? "IN" : "OUT"));

        if (connection <= CONNECTION_OUT) {
            VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
            if (conState != null) {
                switch (conState.getState()) {
                    case CONNECTING:
                        conState.getQueue().add(appMsg); break;
                    case CONNECTED:
                        sendWithListener(appMsg, peer, relayConnectionState.getConnection());break;
                }
            } else
                listener.messageFailed(msg, peer, new IllegalArgumentException("No outgoing connection"));

        } else if (connection == CONNECTION_IN) {
            boolean hasConnection = inConnections.contains(peer);
            if (hasConnection)
                sendWithListener(appMsg, peer, relayConnectionState.getConnection());
            else
                listener.messageFailed(msg, peer, new IllegalArgumentException("No incoming connection"));
        } else {
            listener.messageFailed(msg, peer, new IllegalArgumentException("Invalid connection: " + connection));
            logger.error(self+": Invalid sendMessage mode " + connection);
        }
    }

    @Override
    protected void onOutboundConnectionUp(Connection<ProxyMessage<T>> conn) {
        //connected to assigned relay, not sending this event to listener
        logger.trace(self+": Connected to relay");

        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("ConnectionUp not with assigned relay");

        if (relayConnectionState.getState() == ConnectionState.State.CONNECTED) {
            throw new AssertionError("ConnectionUp to relay in CONNECTED state: " + conn);

        } else if (relayConnectionState.getState() == ConnectionState.State.CONNECTING) {
            relayConnectionState.setState(ConnectionState.State.CONNECTED);
            relayConnectionState.getQueue().forEach(m -> {
                if(m.getType() == ProxyMessage.Type.APP_MSG)
                    sendWithListener((ProxyAppMessage<T>) m, m.getTo(), relayConnectionState.getConnection());
                else
                    relayConnectionState.getConnection().sendMessage(m);
            });
            relayConnectionState.getQueue().clear();
        }
    }

    @Override
    protected void onCloseConnection(Host peer, int connection) {
        if(disconnected) {
            logger.debug(self+": onCloseConnection ignored because disconnected from network.");
            return;
        }

        logger.debug(self+": CloseConnection " + peer + " " + (connection == CONNECTION_IN ? "IN" : "OUT"));

        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.remove(peer);
        if (conState != null) {
            if (relayConnectionState.getState() == ConnectionState.State.CONNECTED)
                relayConnectionState.getConnection().sendMessage(new ProxyConnectionCloseMessage<>(self, peer, new IOException("Connection closed by "+self)));
            else
                relayConnectionState.getQueue().add(new ProxyConnectionCloseMessage<>(self, peer, new IOException("Connection closed by "+self)));

            listener.deliverEvent(new OutConnectionDown(peer,new IOException("Connection closed.")));
        } else
            logger.error(self+": No outgoing connection");
    }

    @Override
    protected void onOutboundConnectionDown(Connection<ProxyMessage<T>> conn, Throwable cause) {
        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("ConnectionDown not with assigned relay");
        else
            logger.error(self+": Connection to relay down unexpectedly"  + (cause != null ? (" " + cause) : ""));
    }

    @Override
    protected void onOutboundConnectionFailed(Connection<ProxyMessage<T>> conn, Throwable cause) {
        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("ConnectionFailed not with assigned relay");
        else
            logger.error(self+": Connection to relay down unexpectedly"  + (cause != null ? (" " + cause) : ""));
    }

    @Override
    public void onDeliverMessage(ProxyMessage<T> msg, Connection<ProxyMessage<T>> conn) {

        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("onDeliverMessage not from relay");

        Host peer;
        if (conn.isInbound())
            //all (one) real connections should be outbound
            throw new AssertionError("Inbound connection on "+ self);
        else
            peer = msg.getFrom();

        switch (msg.getType()) {
            case APP_MSG: handleAppMessage(((ProxyAppMessage<T>) msg).getPayload(), peer); break;
            case CONN_OPEN: virtualOnInboundConnectionUp(peer); break;
            case CONN_CLOSE: virtualOnInboundConnectionDown(peer, ((ProxyConnectionCloseMessage<T>) msg).getCause()); break;
            case CONN_ACCEPT: virtualOnOutboundConnectionUp(peer); break;
            case CONN_FAIL: virtualOnOutboundConnectionFailed(peer ,((ProxyConnectionFailMessage<T>) msg).getCause()); break;
            case PEER_DISCONNECTED: handlePeerDisconnected((ProxyPeerDisconnectedMessage<T>) msg, peer); break;

        }
    }

    private void handlePeerDisconnected(ProxyPeerDisconnectedMessage<T> msg, Host peer) {
        Throwable cause = msg.getCause();
        if(peer.equals(self)) {
            if(disconnected) { // signal to reconnect to network
                disconnected = false;
            } else { // disconnected from network
                disconnected = true;

                //not sure if deliver down events for every connection here

                outConnections = new HashMap<>();
                inConnections = new HashSet<>();
            }
        } else {
            VirtualConnectionState<ProxyMessage<T>> conState = outConnections.remove(peer);
            if (conState != null) {
                switch (conState.getState()) {
                    case CONNECTING:
                        logger.debug(self + ": OutboundConnectionFailed " + peer + (cause != null ? (" " + cause) : ""));
                        listener.deliverEvent(new OutConnectionFailed<>(peer, conState.getQueue(), cause));
                        break;
                    case CONNECTED:
                        logger.debug(self + ": OutboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
                        listener.deliverEvent(new OutConnectionDown(peer, cause));
                        break;
                }
            }

            if (inConnections.contains(peer)) {
                logger.debug(self + ": InboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
                listener.deliverEvent(new InConnectionDown(peer, cause));
            }
        }
    }

    private void virtualOnOutboundConnectionFailed(Host peer, Throwable cause) {
        logger.debug(self+": OutboundConnectionFailed " + peer + (cause != null ? (" " + cause) : ""));

        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.remove(peer);
        if(conState == null)
            throw new AssertionError(self+": No connection in OutboundConnectionFailed: " + peer );
        listener.deliverEvent(new OutConnectionFailed<>(peer, conState.getQueue(), cause));
    }

    private void virtualOnInboundConnectionDown(Host peer, Throwable cause) {
        boolean hasConnection = inConnections.contains(peer);
        if (!hasConnection)
            throw new AssertionError(self+": No connections in InboundConnectionDown " + peer );

        logger.debug(self+": InboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
        inConnections.remove(peer);

        listener.deliverEvent(new InConnectionDown(peer, cause));
    }

    private void virtualOnOutboundConnectionUp(Host peer) {
        logger.debug(self+": OutboundConnectionUp " + peer);
        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
        if (conState == null) {
            throw new AssertionError(self+": ConnectionUp with no conState: " + self + "-" + peer);
        } else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
            throw new AssertionError(self+": ConnectionUp in CONNECTED state: " + self + "-" + peer);
        } else if (conState.getState() == VirtualConnectionState.State.CONNECTING) {
            conState.setState(VirtualConnectionState.State.CONNECTED);
            conState.getQueue().forEach(m -> sendWithListener((ProxyAppMessage<T>) m, m.getTo(), relayConnectionState.getConnection()));
            conState.getQueue().clear();

            listener.deliverEvent(new OutConnectionUp(peer));
        }
    }

    private void virtualOnInboundConnectionUp(Host peer) {
        logger.debug(self+": InboundConnectionUp " + peer);

        inConnections.add(peer);
        relayConnectionState.getConnection().sendMessage(new ProxyConnectionAcceptMessage<>(self, peer));

        listener.deliverEvent(new InConnectionUp(peer));
    }

    private void handleAppMessage(T msg, Host from) {
        if(outConnections.containsKey(from) || inConnections.contains(from))
            listener.deliverMessage(msg, from);
    }

    @Override
    public boolean validateAttributes(Attributes attr) {
        Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
        return channel != null && channel == PROXY_MAGIC_NUMBER;
    }
}
