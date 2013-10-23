package com.bazaarvoice.priam.client;

import com.google.common.base.Objects;

/** Cassandra hint information returned by the "..host../v1/cassadmin/hints/ring" REST API. */
public class HintsInfo {
    enum State {OK, UNREACHABLE, ERROR}

    private String _endpoint;
    private State _state;

    // When state == OK
    private int _totalEndpointsPendingHints;

    // When state == ERROR
    private String _exception;

    /** Returns the IP address of the Cassandra server this information applies ti. */
    public String getEndpoint() {
        return _endpoint;
    }

    public void setEndpoint(String endpoint) {
        _endpoint = endpoint;
    }

    /**
     * Returns a state enumeration describing whether a node is up, down (according to Cassandra gossip info),
     * or up but not responding to requests as expected.
     */
    public State getState() {
        return _state;
    }

    public void setState(State state) {
        _state = state;
    }

    /** Returns the number of servers for which an endpoint has hints queued up for eventual delivery. */
    public int getTotalEndpointsPendingHints() {
        return _totalEndpointsPendingHints;
    }

    public void setTotalEndpointsPendingHints(int totalEndpointsPendingHints) {
        _totalEndpointsPendingHints = totalEndpointsPendingHints;
    }

    /** Returns a string describing the exception encountered when a server is not responding to requests as expected. */
    public String getException() {
        return _exception;
    }

    public void setException(String exception) {
        _exception = exception;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("endpoint", _endpoint)
                .add("state", _state)
                .add("totalEndpointsPendingHints", _totalEndpointsPendingHints)
                .toString();
    }
}
