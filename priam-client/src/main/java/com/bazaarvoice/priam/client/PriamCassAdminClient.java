package com.bazaarvoice.priam.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * HTTP client implements remote access to a System of Record server.
 */
public class PriamCassAdminClient implements PriamCassAdmin {
    private final Client _client;
    private final UriBuilder _priamCassAdmin;

    public PriamCassAdminClient(URI endPoint, Client jerseyClient) {
        _client = checkNotNull(jerseyClient, "jerseyClient");
        _priamCassAdmin = UriBuilder.fromUri(endPoint);
    }

    @Override
    public List<HintsInfo> getHintsForRing() {
        URI uri = _priamCassAdmin.clone()
                .segment("cassadmin", "hints", "ring")
                .build();
        return _client.resource(uri)
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<List<HintsInfo>>() {});
    }
}
