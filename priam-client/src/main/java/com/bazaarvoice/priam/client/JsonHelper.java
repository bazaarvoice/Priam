package com.bazaarvoice.priam.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.dropwizard.json.ObjectMapperFactory;

import java.io.IOException;

class JsonHelper {
    private static final ObjectMapper JSON = new ObjectMapperFactory().build();

    static <T> T fromJson(String string, Class<T> type) {
        try {
            return JSON.readValue(string, type);
        } catch (IOException e) {
            // Must be malformed JSON.  Other kinds of I/O errors don't get thrown when reading from a string.
            throw new IllegalArgumentException(e.toString());
        }
    }
}
