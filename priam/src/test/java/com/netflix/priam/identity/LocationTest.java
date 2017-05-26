package com.netflix.priam.identity;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocationTest {

    @Test
    public void testLocationSerialization() {
        Location expectedNoSuffix = new SimpleLocation("fake-region", "");
        Location expectedWithSuffix = new SimpleLocation("fake-region", "sfx");

        assertEquals("fake-region", expectedNoSuffix.toString());
        assertEquals("fake-region/sfx", expectedWithSuffix.toString());

        assertEquals(Location.from("fake-region"), expectedNoSuffix);
        assertEquals(Location.from("fake-region/sfx"), expectedWithSuffix);
    }
}
