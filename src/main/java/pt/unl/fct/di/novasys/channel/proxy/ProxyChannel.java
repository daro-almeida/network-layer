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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@SuppressWarnings("unchecked")
public class ProxyChannel<T> extends SingleThreadedClientChannel<T, ProxyMessage> implements AttributeValidator {

	public static final String NAME = "PROXYChannel";
	public static final String ADDRESS_KEY = "address";
	public static final String PORT_KEY = "port";
	public static final String TRIGGER_SENT_KEY = "trigger_sent";
	public static final String HEARTBEAT_INTERVAL_KEY = "heartbeat_interval";
	public static final String HEARTBEAT_TOLERANCE_KEY = "heartbeat_tolerance";
	public static final String CONNECT_TIMEOUT_KEY = "connect_timeout";
	public static final String RELAY_ADDRESS_KEY = "relay_address";
	public static final String RELAY_PORT = "relay_port";
	public static final String DEFAULT_RELAY_PORT = "9082";
	public static final String LISTEN_ADDRESS_ATTRIBUTE = "listen_address";
	public static final String DEFAULT_PORT = "8581";
	public static final String DEFAULT_HB_INTERVAL = "0";
	public static final String DEFAULT_HB_TOLERANCE = "0";
	public static final String DEFAULT_CONNECT_TIMEOUT = "1000";
	public static final int CONNECTION_OUT = 0;
	public static final int CONNECTION_IN = 1;
	private static final Logger logger = LogManager.getLogger(ProxyChannel.class);
	private static final short PROXY_MAGIC_NUMBER = 0x1369;
	private final NetworkManager<ProxyMessage> network;
	private final ChannelListener<T> listener;

	private final Attributes attributes;
	private final Host self;
	private final boolean triggerSent;
	private boolean disconnected;
	private Set<Host> inConnections;
	private Map<Host, VirtualConnectionState<ProxyMessage>> outConnections;
	private ConnectionState<ProxyMessage> relayConnectionState;

	public ProxyChannel(ISerializer<T> serializer, ChannelListener<T> list, Properties properties) throws IOException {
		super("ProxyChannel");
		this.listener = list;

		InetAddress address;
		if (properties.containsKey(ADDRESS_KEY))
			address = InetAddress.getByName(properties.getProperty(ADDRESS_KEY));
		else
			throw new IllegalArgumentException(NAME + " requires binding address");

		int port = Integer.parseInt(properties.getProperty(PORT_KEY, DEFAULT_PORT));
		int hbInterval = Integer.parseInt(properties.getProperty(HEARTBEAT_INTERVAL_KEY, DEFAULT_HB_INTERVAL));
		int hbTolerance = Integer.parseInt(properties.getProperty(HEARTBEAT_TOLERANCE_KEY, DEFAULT_HB_TOLERANCE));
		int connTimeout = Integer.parseInt(properties.getProperty(CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT));
		this.triggerSent = Boolean.parseBoolean(properties.getProperty(TRIGGER_SENT_KEY, "false"));

		Host listenAddress = new Host(address, port);
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
		InetAddress relayAddress;
		if (properties.containsKey(RELAY_ADDRESS_KEY))
			relayAddress = InetAddress.getByName(properties.getProperty(RELAY_ADDRESS_KEY));
		else
			throw new IllegalArgumentException(NAME + ": relay address not defined.");
		int relayPort = Integer.parseInt(properties.getProperty(RELAY_PORT, DEFAULT_RELAY_PORT));

		Host relay = new Host(relayAddress, relayPort);

		//only one relay
		this.relayConnectionState = new ConnectionState<>(network.createConnection(relay, attributes, this));

	}

	@Override
	protected void onOpenConnection(Host peer) {
		if (disconnected) {
			logger.trace(self + ": onOpenConnection ignored because disconnected from network.");
			return;
		}

		VirtualConnectionState<ProxyMessage> conState = outConnections.get(peer);
		if (conState == null) {
			logger.trace(self + ": onOpenConnection creating connection to: " + peer);
			outConnections.put(peer, new VirtualConnectionState<>());

			sendMessage(peer, new ProxyConnectionOpenMessage(self, peer));

		} else
			logger.trace(self + ": onOpenConnection ignored: " + peer);
	}

	private void sendWithListener(ProxyAppMessage<T> msg, Host peer) {
		Promise<Void> promise = loop.newPromise();
		promise.addListener(future -> {
			if (future.isSuccess() && triggerSent) listener.messageSent(msg.getPayload(), peer);
			else if (!future.isSuccess()) {
				listener.messageFailed(msg.getPayload(), peer, future.cause());
			}
		});
		sendMessage(peer, msg);
	}

	@Override
	protected void onSendMessage(T msg, Host peer, int connection) {
		if (disconnected) {
			logger.trace(self + ": onSendMessage ignored because disconnected from network.");
			return;
		}

		ProxyAppMessage<T> appMsg = new ProxyAppMessage<>(self, peer, msg);

		if (relayConnectionState.getState() != ConnectionState.State.CONNECTED)
			throw new AssertionError("Not connected to relay");

		logger.trace(self + ": SendMessage " + msg + " " + peer + " " + (connection == CONNECTION_IN ? "IN" : "OUT"));

		if (connection <= CONNECTION_OUT) {
			VirtualConnectionState<ProxyMessage> conState = outConnections.get(peer);
			if (conState != null) {
				if (conState.getState() == VirtualConnectionState.State.CONNECTING) {
					conState.getQueue().add(appMsg);
				} else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
					sendWithListener(appMsg, peer);
				}
			} else
				listener.messageFailed(msg, peer, new IllegalArgumentException("No outgoing connection"));
		} else if (connection == CONNECTION_IN) {
			boolean hasConnection = inConnections.contains(peer);
			if (hasConnection)
				sendWithListener(appMsg, peer);
			else
				listener.messageFailed(msg, peer, new IllegalArgumentException("No incoming connection"));
		} else {
			listener.messageFailed(msg, peer, new IllegalArgumentException("Invalid connection: " + connection));
			logger.error(self + ": Invalid sendMessage mode " + connection);
		}
	}

	@Override
	protected void onOutboundConnectionUp(Connection<ProxyMessage> conn) {
		//connected to assigned relay, not sending this event to listener
		logger.trace(self + ": Connected to relay");

		if (!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
			throw new AssertionError("ConnectionUp not with assigned relay");

		if (relayConnectionState.getState() != ConnectionState.State.CONNECTING) {
			throw new AssertionError("ConnectionUp to relay in " + relayConnectionState.getState().name() + " state: " + conn);
		} else {
			relayConnectionState.setState(ConnectionState.State.CONNECTED);
			relayConnectionState.getQueue().forEach(msg -> {
				if (msg.getType() == ProxyMessage.Type.APP_MSG)
					sendWithListener((ProxyAppMessage<T>) msg, msg.getTo());
				else
					sendMessage(msg.getTo(), msg);
			});
			relayConnectionState.getQueue().clear();
		}
	}

	@Override
	protected void onCloseConnection(Host peer, int connection) {
		if (disconnected) {
			logger.trace(self + ": onCloseConnection ignored because disconnected from network.");
			return;
		}

		logger.trace(self + ": CloseConnection " + peer + " " + (connection == CONNECTION_IN ? "IN" : "OUT"));

		VirtualConnectionState<ProxyMessage> conState = outConnections.remove(peer);
		if (conState != null) {
			sendMessage(peer, new ProxyConnectionCloseMessage(self, peer, new IOException("Connection closed by " + self)));
			listener.deliverEvent(new OutConnectionDown(peer, new IOException("Connection closed.")));
		} else
			logger.error(self + ": No outgoing connection");
	}

	@Override
	protected void onOutboundConnectionDown(Connection<ProxyMessage> conn, Throwable cause) {
		if (!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
			throw new AssertionError("ConnectionDown not with assigned relay");
		else
			logger.error(self + ": Connection to relay down unexpectedly" + (cause != null ? (" " + cause) : ""));
	}

	@Override
	protected void onOutboundConnectionFailed(Connection<ProxyMessage> conn, Throwable cause) {
		if (!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
			throw new AssertionError("ConnectionFailed not with assigned relay");
		else
			logger.error(self + ": Connection to relay down unexpectedly" + (cause != null ? (" " + cause) : ""));
	}

	@Override
	public void onDeliverMessage(ProxyMessage msg, Connection<ProxyMessage> conn) {

		if (!conn.getPeer().equals(relayConnectionState.getConnection().getPeer()))
			throw new AssertionError("onDeliverMessage not from relay");

		Host peer;
		if (conn.isInbound())
			//all (one) real connections should be outbound
			throw new AssertionError("Inbound connection on " + self);
		else
			peer = msg.getFrom();

		logger.debug("Received {} message {} to {} from {}", msg.getType().name(), msg.getSeqN(), msg.getTo(), msg.getFrom());

		switch (msg.getType()) {
			case APP_MSG:
				handleAppMessage(((ProxyAppMessage<T>) msg).getPayload(), peer);
				break;
			case CONN_OPEN:
				virtualOnInboundConnectionUp(peer);
				break;
			case CONN_CLOSE:
				virtualOnInboundConnectionDown(peer, ((ProxyConnectionCloseMessage) msg).getCause());
				break;
			case CONN_ACCEPT:
				virtualOnOutboundConnectionUp(peer);
				break;
			case CONN_FAIL:
				virtualOnOutboundConnectionFailed(peer, ((ProxyConnectionFailMessage) msg).getCause());
				break;
			case PEER_DISCONNECTED:
				handlePeerDisconnected((ProxyPeerDisconnectedMessage) msg, peer);
				break;

		}
	}

	private void handlePeerDisconnected(ProxyPeerDisconnectedMessage msg, Host peer) {
		Throwable cause = msg.getCause();
		if (peer.equals(self)) {
			if (disconnected) { // signal to reconnect to network
				disconnected = false;
			} else { // disconnected from network
				disconnected = true;

				//not sure if deliver down events for every connection here

				outConnections = new HashMap<>();
				inConnections = new HashSet<>();
			}
		} else {
			VirtualConnectionState<ProxyMessage> conState = outConnections.remove(peer);
			if (conState != null) {
				if (conState.getState() == VirtualConnectionState.State.CONNECTING) {
					logger.trace(self + ": OutboundConnectionFailed " + peer + (cause != null ? (" " + cause) : ""));
					listener.deliverEvent(new OutConnectionFailed<>(peer, conState.getQueue(), cause));
				} else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
					logger.trace(self + ": OutboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
					listener.deliverEvent(new OutConnectionDown(peer, cause));
				}
			}

			if (inConnections.contains(peer)) {
				logger.trace(self + ": InboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
				listener.deliverEvent(new InConnectionDown(peer, cause));
			}
		}
	}

	private void virtualOnOutboundConnectionFailed(Host peer, Throwable cause) {
		logger.trace(self + ": OutboundConnectionFailed " + peer + (cause != null ? (" " + cause) : ""));

		VirtualConnectionState<ProxyMessage> conState = outConnections.remove(peer);
		if (conState == null)
			throw new AssertionError(self + ": No connection in OutboundConnectionFailed: " + peer);
		listener.deliverEvent(new OutConnectionFailed<>(peer, conState.getQueue(), cause));
	}

	private void virtualOnInboundConnectionDown(Host peer, Throwable cause) {
		boolean hasConnection = inConnections.contains(peer);
		if (!hasConnection)
			throw new AssertionError(self + ": No connections in InboundConnectionDown " + peer);

		logger.trace(self + ": InboundConnectionDown " + peer + (cause != null ? (" " + cause) : ""));
		inConnections.remove(peer);

		listener.deliverEvent(new InConnectionDown(peer, cause));
	}

	private void virtualOnOutboundConnectionUp(Host peer) {
		logger.trace(self + ": OutboundConnectionUp " + peer);
		VirtualConnectionState<ProxyMessage> conState = outConnections.get(peer);
		if (conState == null) {
			logger.trace(self + ": got ACCEPT with no conState: " + self + "-" + peer);
		} else if (conState.getState() == VirtualConnectionState.State.CONNECTED) {
			logger.trace(self + ": got ACCEPT in CONNECTED state: " + self + "-" + peer);
		} else if (conState.getState() == VirtualConnectionState.State.CONNECTING) {
			conState.setState(VirtualConnectionState.State.CONNECTED);
			conState.getQueue().forEach(m -> sendWithListener((ProxyAppMessage<T>) m, m.getTo()));
			conState.getQueue().clear();

			listener.deliverEvent(new OutConnectionUp(peer));
		}
	}

	private void virtualOnInboundConnectionUp(Host peer) {
		logger.trace(self + ": InboundConnectionUp " + peer);

		inConnections.add(peer);
		sendMessage(peer, new ProxyConnectionAcceptMessage(self, peer));

		listener.deliverEvent(new InConnectionUp(peer));
	}

	private void handleAppMessage(T msg, Host from) {
		if (outConnections.containsKey(from) || inConnections.contains(from))
			listener.deliverMessage(msg, from);
	}

	private void sendMessage(Host peer, ProxyMessage msg) {
		if (peer.equals(self)) {
			logger.debug("Sending {} message {} to {} from {}", msg.getType().name(), msg.getSeqN(), msg.getTo(), msg.getFrom());
			onDeliverMessage(msg, relayConnectionState.getConnection());
		}
		else if (relayConnectionState.getState() == ConnectionState.State.CONNECTED) {
			logger.debug("Sending {} message {} to {} from {}", msg.getType().name(), msg.getSeqN(), msg.getTo(), msg.getFrom());
			relayConnectionState.getConnection().sendMessage(msg);
		} else {
			relayConnectionState.getQueue().add(msg);
		}
	}

	@Override
	public boolean validateAttributes(Attributes attr) {
		Short channel = attr.getShort(CHANNEL_MAGIC_ATTRIBUTE);
		return channel != null && channel == PROXY_MAGIC_NUMBER;
	}
}
