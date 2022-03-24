package pt.unl.fct.di.novasys.channel.proxy;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pt.unl.fct.di.novasys.channel.ChannelListener;
import pt.unl.fct.di.novasys.channel.base.SingleThreadedBiChannel;
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

public class ProxyChannel<T> extends SingleThreadedBiChannel<T, ProxyMessage<T>> implements AttributeValidator {

    private static final Logger logger = LogManager.getLogger(ProxyChannel.class);
    private static final short PROXY_MAGIC_NUMBER = 0x1369;

    public final static String NAME = "PROXYChannel";
    public final static String ADDRESS_KEY = "address";
    public final static String PORT_KEY = "port";
    public final static String WORKER_GROUP_KEY = "worker_group";
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

    private final Set<Host> inConnections;
    private final Map<Host, VirtualConnectionState<ProxyMessage<T>>> outConnections;

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

        EventLoopGroup eventExecutors = properties.containsKey(WORKER_GROUP_KEY) ?
                (EventLoopGroup) properties.get(WORKER_GROUP_KEY) :
                NetworkManager.createNewWorkerGroup();

        ProxyMessageSerializer<T> tProxyMessageSerializer = new ProxyMessageSerializer<>(serializer);
        network = new NetworkManager<>(tProxyMessageSerializer, this, hbInterval, hbTolerance, connTimeout);
        network.createServerSocket(this, listenAddress, this, eventExecutors);

        attributes = new Attributes();
        attributes.putShort(CHANNEL_MAGIC_ATTRIBUTE, PROXY_MAGIC_NUMBER);
        attributes.putHost(LISTEN_ADDRESS_ATTRIBUTE, listenAddress);

        connectToRelay(properties);

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
        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
        if (conState == null) {
            logger.debug("onOpenConnection creating connection to: " + peer);
            outConnections.put(peer, new VirtualConnectionState<>());
            if(relayConnectionState.getState() == ConnectionState.State.CONNECTED)
                relayConnectionState.getConnection().sendMessage(new ProxyConnectionOpenMessage<>(self,peer));
            else
                relayConnectionState.getQueue().add(new ProxyConnectionOpenMessage<>(self,peer));
        } else if (conState.getState() == VirtualConnectionState.State.DISCONNECTING) {
            logger.debug("onOpenConnection reopening after close to: " + peer);
            conState.setState(VirtualConnectionState.State.DISCONNECTING_RECONNECT);
        } else
            logger.debug("onOpenConnection ignored: " + peer);
    }

    private void sendWithListener(ProxyAppMessage<T> msg, Host peer, Connection<ProxyMessage<T>> established) {
        Promise<Void> promise = loop.newPromise();
        promise.addListener(future -> {
            if (future.isSuccess() && triggerSent) listener.messageSent(msg.getPayload(), peer);
            else if (!future.isSuccess()) listener.messageFailed(msg.getPayload(), peer, future.cause());
        });
        established.sendMessage(msg, promise);
    }

    @Override
    protected void onSendMessage(T msg, Host peer, int connection) {

        ProxyAppMessage<T> appMsg = new ProxyAppMessage<>(self, peer, msg);

        if(relayConnectionState.getState() == ConnectionState.State.CONNECTING) {
            relayConnectionState.getQueue().add(appMsg);
            logger.debug("sent message without connection established to relay : queueing message");
        }
        else if(relayConnectionState.getState() != ConnectionState.State.CONNECTED)
            throw new AssertionError("Not connected to relay");

        logger.debug("SendMessage " + msg + " " + peer + " " + (connection == CONNECTION_IN ? "IN" : "OUT"));

        if (connection <= CONNECTION_OUT) {
            VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
            if (conState != null) {
                if (conState.getState() == VirtualConnectionState.State.CONNECTING ||
                        conState.getState() == VirtualConnectionState.State.DISCONNECTING_RECONNECT) {
                    conState.getQueue().add(appMsg);
                } else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
                    sendWithListener(appMsg, peer, relayConnectionState.getConnection());
                } else if (conState.getState() == VirtualConnectionState.State.DISCONNECTING) {
                    conState.getQueue().add(appMsg);
                    conState.setState(VirtualConnectionState.State.DISCONNECTING_RECONNECT);
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
            logger.error("Invalid sendMessage mode " + connection);
        }
    }

    @Override
    protected void onOutboundConnectionUp(Connection<ProxyMessage<T>> conn) {
        //connected to assigned relay, not sending this event to listener
        logger.debug("Connected to relay");
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
        logger.debug("CloseConnection " + peer);
        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
        if (conState != null) {
            if (conState.getState() == VirtualConnectionState.State.CONNECTED || conState.getState() == VirtualConnectionState.State.CONNECTING
                    || conState.getState() == VirtualConnectionState.State.DISCONNECTING_RECONNECT) {
                if(relayConnectionState.getState() == ConnectionState.State.CONNECTED)
                    relayConnectionState.getConnection().sendMessage(new ProxyConnectionCloseMessage<>(self,peer,new Throwable("Connection closed.")));
                else
                    relayConnectionState.getQueue().add(new ProxyConnectionOpenMessage<>(self,peer));
                conState.setState(VirtualConnectionState.State.DISCONNECTING);
                conState.getQueue().clear();
            }
        }
    }

    @Override
    protected void onOutboundConnectionDown(Connection<ProxyMessage<T>> conn, Throwable cause) {
        //connection to relay down, should try to reconnect immediately
        logger.debug("Connection down with relay"  + (cause != null ? (" " + cause) : ""));

        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("ConnectionDown not with assigned relay");
        else {
            if (relayConnectionState.getState() == ConnectionState.State.CONNECTING) {
                throw new AssertionError("ConnectionDown to relay in CONNECTING state: " + conn);
            } else if (relayConnectionState.getState() == ConnectionState.State.CONNECTED || relayConnectionState.getState() == ConnectionState.State.DISCONNECTING_RECONNECT) {
                this.relayConnectionState = new ConnectionState<>(network.createConnection(conn.getPeer(), attributes, this));
            }
        }
    }

    @Override
    protected void onOutboundConnectionFailed(Connection<ProxyMessage<T>> conn, Throwable cause) {
        //connection failed with relay, in theory shouldn't happen if relay is up, try to reconnect immediately
        logger.debug("Connection failed with relay" + (cause != null ? (" " + cause) : ""));

        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("ConnectionFailed not with assigned relay");
        else {
            if (relayConnectionState.getState() == ConnectionState.State.DISCONNECTING_RECONNECT || relayConnectionState.getState() == ConnectionState.State.CONNECTING)
                this.relayConnectionState = new ConnectionState<>(network.createConnection(conn.getPeer(), attributes, this));
            else if (relayConnectionState.getState() == ConnectionState.State.CONNECTED)
                throw new AssertionError("ConnectionFailed to relay in state: " + relayConnectionState.getState() + " - " + conn);
        }
    }

    @Override
    protected void onInboundConnectionUp(Connection<ProxyMessage<T>> con) {
        //this event shouldn't happen
        throw new AssertionError("Inbound connection on " + self);

    }

    @Override
    protected void onInboundConnectionDown(Connection<ProxyMessage<T>> con, Throwable cause) {
        //this event shouldn't happen
        throw new AssertionError("Inbound connection on "+ self);
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
    public void onDeliverMessage(ProxyMessage<T> msg, Connection<ProxyMessage<T>> conn) {

        if(!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
            throw new AssertionError("onDeliverMessage not from relay");

        Host host;
        if (conn.isInbound())
            //all (one) real connections should be outbound
            throw new AssertionError("Inbound connection on "+ self);
        else
            host = msg.getFrom();

        switch (msg.getType()) {
            case APP_MSG: handleAppMessage(((ProxyAppMessage<T>) msg).getPayload(), host); break;
            case CONN_OPEN: virtualOnInboundConnectionUp(host); break;
            case CONN_CLOSE: virtualOnInboundConnectionDown(host, ((ProxyConnectionCloseMessage<T>) msg).getCause()); break;
            case CONN_ACCEPT: virtualOnOutboundConnectionUp(host); break;
            case CONN_FAIL: {
                VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(host);
                if(conState != null) {
                    if (conState.getState() == VirtualConnectionState.State.CONNECTED)
                        virtualOnOutboundConnectionDown(host, ((ProxyConnectionFailMessage<T>) msg).getCause());
                    else
                        virtualOnOutboundConnectionFailed(host, ((ProxyConnectionFailMessage<T>) msg).getCause());
                }

                boolean hasConnection = inConnections.contains(host);
                if(hasConnection)
                    virtualOnInboundConnectionDown(host, ((ProxyConnectionFailMessage<T>) msg).getCause());

                if(conState == null && !hasConnection)
                    throw new AssertionError("ConnectionFailed with no valid connection");

                break;
            }
        }
    }

    private void virtualOnOutboundConnectionFailed(Host peer, Throwable cause) {
        logger.debug("OutboundConnectionFailed " + peer + (cause != null ? (" " + cause) : ""));

        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.remove(peer);
        if (conState == null) {
            throw new AssertionError("ConnectionFailed with no conState: " + self + "-" + peer);
        } else {
            if (conState.getState() == VirtualConnectionState.State.CONNECTING)
                listener.deliverEvent(new OutConnectionFailed<>(peer, conState.getQueue(), cause));
            else if (conState.getState() == VirtualConnectionState.State.DISCONNECTING_RECONNECT) {
                outConnections.put(peer, new VirtualConnectionState<>());
                relayConnectionState.getConnection().sendMessage(new ProxyConnectionOpenMessage<>(self, peer));
            }
            else if (conState.getState() == VirtualConnectionState.State.CONNECTED)
                throw new AssertionError("ConnectionFailed in state: " + conState.getState());
        }
    }

    private void virtualOnOutboundConnectionDown(Host peer, Throwable cause) {
        logger.debug("OutboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));

        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.remove(peer);
        if (conState == null) {
            throw new AssertionError("ConnectionDown with no conState: " + self + "-" + peer);
        } else {
            if (conState.getState() == VirtualConnectionState.State.CONNECTING) {
                throw new AssertionError("ConnectionDown in CONNECTING state: " + self + "-" + peer);
            } else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
                listener.deliverEvent(new OutConnectionDown(peer, cause));
            } else if (conState.getState() == VirtualConnectionState.State.DISCONNECTING_RECONNECT) {
                outConnections.put(peer, new VirtualConnectionState<>());
                relayConnectionState.getConnection().sendMessage(new ProxyConnectionOpenMessage<>(self, peer));
            }
        }
    }

    private void virtualOnInboundConnectionDown(Host peer, Throwable cause) {
        boolean hasConnection = inConnections.contains(peer);
        if (!hasConnection)
            throw new AssertionError("No connections in InboundConnectionDown " + peer );

        logger.debug("InboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
        inConnections.remove(peer);

        listener.deliverEvent(new InConnectionDown(peer, cause));
    }

    private void virtualOnOutboundConnectionUp(Host peer) {
        listener.deliverEvent(new OutConnectionUp(peer));

        logger.debug("OutboundConnectionUp " + peer);
        VirtualConnectionState<ProxyMessage<T>> conState = outConnections.get(peer);
        if (conState == null) {
            throw new AssertionError("ConnectionUp with no conState: " + self + "-" + peer);
        } else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
            throw new AssertionError("ConnectionUp in CONNECTED state: " + self + "-" + peer);
        } else if (conState.getState() == VirtualConnectionState.State.CONNECTING) {
            conState.setState(VirtualConnectionState.State.CONNECTED);
            conState.getQueue().forEach(m -> sendWithListener((ProxyAppMessage<T>) m, m.getTo(), relayConnectionState.getConnection()));
            conState.getQueue().clear();

            listener.deliverEvent(new OutConnectionUp(peer));
        }
    }

    private void virtualOnInboundConnectionUp(Host peer) {
        inConnections.add(peer);
        relayConnectionState.getConnection().sendMessage(new ProxyConnectionAcceptMessage<>(self, peer));

        listener.deliverEvent(new InConnectionUp(peer));
    }

    private void handleAppMessage(T msg, Host from) {
        listener.deliverMessage(msg, from);
    }

    @Override
    public boolean validateAttributes(Attributes attr) {
        Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
        return channel != null && channel == PROXY_MAGIC_NUMBER;
    }
}
