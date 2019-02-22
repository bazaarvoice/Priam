package com.netflix.priam.cassandra.extensions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class VersionComparatorTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "2.0.9", "2.0.1", 1 },
                { "2.0.9", "2.0.17", -1 },
                { "2.0.9", "2.0.9", 0 },
                { "2.0", "2.0.0.0", 0 },
                { "2.0.9", "2.0", 1 },
                { "2.0.9", "2.1.15", -1 },
                { "2.0.9", "2.0.9-PATCH", 0 },
                { "2.1.15", "3.0.14", -1 },
                { "2.1.15", "3.11.4", -1 }

        });
    }

    private String left;
    private String right;
    private int expected;

    public VersionComparatorTest(String left, String right, int expected) {
        this.left = left;
        this.right = right;
        this.expected = expected;
    }

    @Test
    public void testComparisons() {
        assertEquals("Incorrect comparison", expected, (int) Math.signum(new VersionComparator().compare(left, right)));
        assertEquals("Incorrect comparison", -1 * expected, (int) Math.signum(new VersionComparator().compare(right, left)));
    }
}
