package pt.unl.fct.di.novasys.channel.proxy;

import java.util.LinkedList;
import java.util.Queue;

/**
 * State of connection between peers that doesn't really exist.
 */
public class VirtualConnectionState<T> {

    public enum State {CONNECTING, CONNECTED}

    private State state;
    private final Queue<T> queue;

    public VirtualConnectionState() {
        this.state = State.CONNECTING;
        this.queue = new LinkedList<>();
    }

    public VirtualConnectionState(Queue<T> initialQueue) {
        this.state = State.CONNECTING;
        this.queue = new LinkedList<>(initialQueue);
    }

    public Queue<T> getQueue() {
        return queue;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }
}
