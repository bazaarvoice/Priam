package com.netflix.priam.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.netflix.priam.identity.Location;
import com.netflix.priam.identity.SimpleLocation;
import org.apache.cassandra.dht.RandomPartitioner;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Murmur3PartitionerTokenManagerTest {
    private static final BigInteger MINIMUM_TOKEN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger MAXIMUM_TOKEN = BigInteger.valueOf(Long.MAX_VALUE);

    private static final BigIntegerTokenManager tokenManager = BigIntegerTokenManager.forMurmur3Partitioner();

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_zeroSize() {
        tokenManager.initialToken(0, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativePosition() {
        tokenManager.initialToken(1, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativeOffset() {
        tokenManager.initialToken(1, 0, -1);
    }

    @Test
    public void initialToken_positionZero() {
        assertEquals(MINIMUM_TOKEN, tokenManager.initialToken(1, 0, 0));
        assertEquals(MINIMUM_TOKEN, tokenManager.initialToken(10, 0, 0));
        assertEquals(MINIMUM_TOKEN, tokenManager.initialToken(133, 0, 0));
    }

    @Test
    public void initialToken_offsets_zeroPosition() {
        assertEquals(MINIMUM_TOKEN.add(BigInteger.valueOf(7)), tokenManager.initialToken(1, 0, 7));
        assertEquals(MINIMUM_TOKEN.add(BigInteger.valueOf(11)), tokenManager.initialToken(2, 0, 11));
        assertEquals(MINIMUM_TOKEN.add(BigInteger.valueOf(Integer.MAX_VALUE)),
                tokenManager.initialToken(256, 0, Integer.MAX_VALUE));
    }

    @Test
    public void initialToken_cannotExceedMaximumToken() {
        final int maxRingSize = Integer.MAX_VALUE;
        final int maxPosition = maxRingSize - 1;
        final int maxOffset = Integer.MAX_VALUE;
        assertTrue(MAXIMUM_TOKEN.compareTo(tokenManager.initialToken(maxRingSize, maxPosition, maxOffset)) > 0);
    }

    @Test
    public void createToken() {
        assertEquals(MAXIMUM_TOKEN
                        .add(BigInteger.ONE)
                        .subtract(MINIMUM_TOKEN)
                        .divide(BigInteger.valueOf(8 * 32))
                        .multiply(BigInteger.TEN)
                        .add(BigInteger.valueOf(TokenManager.locationOffset(Location.from("region"))))
                        .add(MINIMUM_TOKEN)
                        .toString(),
                tokenManager.createToken(10, 8, 32, Location.from("region")));
    }

    @Test
    public void createToken_typical() {
        // 6 node clusters should have 6 tokens distributed evenly from -2^63 to 2^63 (exclusive) + region offset
        Location usEast1 = Location.from("us-east-1");
        assertEquals("-9223372035046200208", tokenManager.createToken(0, 3, 2, usEast1));
        assertEquals("-6148914689427941606", tokenManager.createToken(1, 3, 2, usEast1));
        assertEquals("-3074457343809683004", tokenManager.createToken(2, 3, 2, usEast1));
        assertEquals("1808575598", tokenManager.createToken(3, 3, 2, usEast1));
        assertEquals("3074457347426834200", tokenManager.createToken(4, 3, 2, usEast1));
        assertEquals("6148914693045092802", tokenManager.createToken(5, 3, 2, usEast1));

        Location euWest1 = Location.from("eu-west-1");
        assertEquals("-9223372036482027696", tokenManager.createToken(0, 3, 2, euWest1));
        assertEquals("-6148914690863769094", tokenManager.createToken(1, 3, 2, euWest1));
        assertEquals("-3074457345245510492", tokenManager.createToken(2, 3, 2, euWest1));
        assertEquals("372748110", tokenManager.createToken(3, 3, 2, euWest1));
        assertEquals("3074457345991006712", tokenManager.createToken(4, 3, 2, euWest1));
        assertEquals("6148914691609265314", tokenManager.createToken(5, 3, 2, euWest1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void findClosestToken_emptyTokenList() {
        tokenManager.findClosestToken("0", Collections.<String>emptyList());
    }

    @Test
    public void findClosestToken_singleTokenList() {
        assertEquals("100", tokenManager.findClosestToken("10", ImmutableList.of("100")));
    }

    @Test
    public void findClosestToken_multipleTokenList() {
        List<String> tokenList = ImmutableList.of("1", "10", "100");
        assertEquals("1", tokenManager.findClosestToken("1", tokenList));
        assertEquals("10", tokenManager.findClosestToken("9", tokenList));
        assertEquals("10", tokenManager.findClosestToken("10", tokenList));
        assertEquals("10", tokenManager.findClosestToken("12", tokenList));
        assertEquals("10", tokenManager.findClosestToken("51", tokenList));
        assertEquals("100", tokenManager.findClosestToken("56", tokenList));
        assertEquals("100", tokenManager.findClosestToken("100", tokenList));
    }

    @Test
    public void findClosestToken_tieGoesToLargerToken() {
        assertEquals("10", tokenManager.findClosestToken("5", ImmutableList.of("0", "10")));
    }

    @Test
    public void test4Splits() {
        final String expectedTokens = "-9223372036854775808,-4611686018427387904,0,4611686018427387904";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(new BigInteger(tokens[i]), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void test16Splits() {
        final String expectedTokens =
                "-9223372036854775808,-8070450532247928832,-6917529027641081856,-5764607523034234880," +
                        "-4611686018427387904,-3458764513820540928,-2305843009213693952,-1152921504606846976," +
                        "0,1152921504606846976,2305843009213693952,3458764513820540928," +
                        "4611686018427387904,5764607523034234880,6917529027641081856,8070450532247928832";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(new BigInteger(tokens[i]), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void testLocationOffset() {
        String allRegions = "us-west-2,us-east,us-west,eu-east,eu-west,ap-northeast,ap-southeast";
        String dcSuffixes = ",-dev,-qa,-prod";

        for (String region1 : allRegions.split(",")) {
            for (String region2 : allRegions.split(",")) {
                for (String dcSuffix1 : dcSuffixes.split(",")) {
                    for (String dcSuffix2: dcSuffixes.split(",")) {
                        if (region1.equals(region2) && dcSuffix1.equals(dcSuffix2)) {
                            continue;
                        }
                        Location loc1 = new SimpleLocation(region1, dcSuffix1);
                        Location loc2 = new SimpleLocation(region2, dcSuffix2);
                        assertFalse("Difference seems to be low",
                                Math.abs(TokenManager.locationOffset(loc1) - TokenManager.locationOffset(loc2)) < 100);
                    }
                }
            }
        }
    }

    @Test
    public void testMultiToken() {
        int h1 = TokenManager.locationOffset(Location.from("vijay"));
        int h2 = TokenManager.locationOffset(Location.from("vijay2"));
        BigInteger t1 = tokenManager.initialToken(100, 10, h1);
        BigInteger t2 = tokenManager.initialToken(100, 10, h2);

        BigInteger tokenDistance = t1.subtract(t2).abs();
        int hashDifference = h1 - h2;

        assertEquals(new BigInteger("" + hashDifference).abs(), tokenDistance);

        BigInteger t3 = tokenManager.initialToken(100, 99, h1);
        BigInteger t4 = tokenManager.initialToken(100, 99, h2);
        tokenDistance = t3.subtract(t4).abs();

        assertEquals(new BigInteger("" + hashDifference).abs(), tokenDistance);
    }

    @Test
    public void testSanitizeToken() {
        Random random = new Random();
        byte[] bytes = new byte[random.nextInt(16)];
        random.nextBytes(bytes);
        BigInteger number = new BigInteger(1, bytes);
        RandomPartitioner.BigIntegerToken token = new RandomPartitioner.BigIntegerToken(Ordering.natural().min(number, MAXIMUM_TOKEN));

        String string = tokenManager.sanitizeToken(token.toString());
        assertTrue(string, string.matches("[0-9]*"));

        assertEquals(token, new RandomPartitioner().getTokenFactory().fromString(string));
    }
}
