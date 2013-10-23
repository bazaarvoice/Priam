package com.bazaarvoice.priam.client;

import com.sun.jersey.api.client.Client;

import java.net.URI;
import java.util.List;

/**
 * Command-line utility for testing the {@link com.bazaarvoice.priam.client.PriamCassAdmin#getHintsForRing()} method.
 */
public class ListHintsForRing {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("usage: ListHintsForRing <host>:<priam-port>");
            System.exit(2);
        }

        String hostAndPort = args[0];
        URI priamUri = URI.create("http://" + hostAndPort + "/v1");
        PriamCassAdmin priam = new PriamCassAdminClient(priamUri, Client.create());

        List<HintsInfo> hints = priam.getHintsForRing();
        for (HintsInfo hint : hints) {
            System.out.println(hint);
        }
    }
}
