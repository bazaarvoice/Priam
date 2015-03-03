package com.bazaarvoice.priam.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;

import java.io.IOException;

class JsonHelper {
    private static final ObjectMapper JSON = Jackson.newObjectMapper();

    static <T> T fromJson(String string, Class<T> type) {
        try {
            return JSON.readValue(string, type);
        } catch (IOException e) {
            // Must be malformed JSON.  Other kinds of I/O errors don't get thrown when reading from a string.
            throw new IllegalArgumentException(e.toString());
        }
    }
}
