package com.netflix.priam.utils;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.netflix.priam.identity.Location;
import com.netflix.priam.identity.SimpleLocation;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Hex;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BOPTokenManagerTest {
    @Test(expected = IllegalArgumentException.class)
    public void initialToken_zeroSize() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        tokenManager.initialToken(0, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativePosition() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        tokenManager.initialToken(1, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initialToken_negativeOffset() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        tokenManager.initialToken(1, 0, -1);
    }

    @Test
    public void initialToken_positionZero() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        assertEquals(toToken("00"), tokenManager.initialToken(1, 0, 0));
        assertEquals(toToken("00"), tokenManager.initialToken(10, 0, 0));
        assertEquals(toToken("00"), tokenManager.initialToken(133, 0, 0));
    }

    @Test
    public void initialToken_offsets_zeroPosition() {
        BOPTokenManager tokenManager = newBOPTokenManager(16);
        assertEquals(toToken("00000000000000000000000000000007"), tokenManager.initialToken(1, 0, 7));
        assertEquals(toToken("0000000000000000000000000000000b"), tokenManager.initialToken(2, 0, 11));
        assertEquals(toToken("0000000000000000000000007fffffff"), tokenManager.initialToken(256, 0, Integer.MAX_VALUE));
    }

    @Test
    public void initialToken_cannotExceedMaximumToken() {
        // With 16-byte min/max and 4-byte size/position/offset it's impossible to get an initial token > max-token
        BOPTokenManager tokenManager = newBOPTokenManager(16);
        int maxRingSize = Integer.MAX_VALUE;
        int maxPosition = maxRingSize - 1;
        int maxOffset = Integer.MAX_VALUE;
        assertTrue(toToken("ffffffffffffffffffffffffffffffff")
                .compareTo(tokenManager.initialToken(maxRingSize, maxPosition, maxOffset)) > 0);
    }

    @Test
    public void initialToken_cannotExceedMaximumTokenWrapAround() {
        // With 5-byte min/max and 4-byte size/position/offset the offset can cause the initial token to wrap around
        BOPTokenManager tokenManager = newBOPTokenManager(5);
        int maxRingSize = Integer.MAX_VALUE;
        int maxPosition = maxRingSize - 1;
        int maxOffset = Integer.MAX_VALUE;
        assertTrue(toToken("7ffffc00")
                .compareTo(tokenManager.initialToken(maxRingSize, maxPosition, maxOffset)) > 0);
    }

    @Test
    public void createToken() {
        BOPTokenManager tokenManager = newBOPTokenManager(16);
        assertEquals(Strings.padStart(new BigInteger("ffffffffffffffffffffffffffffffff", 16)
                        .add(BigInteger.ONE)
                        .divide(BigInteger.valueOf(8 * 32))
                        .multiply(BigInteger.TEN)
                        .add(BigInteger.valueOf(TokenManager.locationOffset(Location.from("region"))))
                        .toString(16), 32, '0'),
                tokenManager.createToken(10, 8, 32, Location.from("region")));
    }

    @Test
    public void createToken_typical() {
        // 6 node clusters should have 6 tokens distributed evenly from 0 to 2^128 (exclusive) + region offset
        BOPTokenManager tokenManager = newBOPTokenManager(16);

        Location usEast1 = Location.from("us-east-1");
        assertEquals("0000000000000000000000006bccac70", tokenManager.createToken(0, 3, 2, usEast1));
        assertEquals("2aaaaaaaaaaaaaaaaaaaaaab1677571a", tokenManager.createToken(1, 3, 2, usEast1));
        assertEquals("555555555555555555555555c12201c4", tokenManager.createToken(2, 3, 2, usEast1));
        assertEquals("8000000000000000000000006bccac6e", tokenManager.createToken(3, 3, 2, usEast1));
        assertEquals("aaaaaaaaaaaaaaaaaaaaaaab16775718", tokenManager.createToken(4, 3, 2, usEast1));
        assertEquals("d55555555555555555555555c12201c2", tokenManager.createToken(5, 3, 2, usEast1));

        Location euWest1 = Location.from("eu-west-1");
        assertEquals("0000000000000000000000001637af50", tokenManager.createToken(0, 3, 2, euWest1));
        assertEquals("2aaaaaaaaaaaaaaaaaaaaaaac0e259fa", tokenManager.createToken(1, 3, 2, euWest1));
        assertEquals("5555555555555555555555556b8d04a4", tokenManager.createToken(2, 3, 2, euWest1));
        assertEquals("8000000000000000000000001637af4e", tokenManager.createToken(3, 3, 2, euWest1));
        assertEquals("aaaaaaaaaaaaaaaaaaaaaaaac0e259f8", tokenManager.createToken(4, 3, 2, euWest1));
        assertEquals("d555555555555555555555556b8d04a2", tokenManager.createToken(5, 3, 2, euWest1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void findClosestToken_emptyTokenList() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        tokenManager.findClosestToken("0", Collections.emptyList());
    }

    @Test
    public void findClosestToken_singleTokenList() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        assertEquals("20", tokenManager.findClosestToken("01", ImmutableList.of("20")));
        assertEquals("20", tokenManager.findClosestToken("ff", ImmutableList.of("20")));
    }

    @Test
    public void findClosestToken_multipleTokenList() {
        BOPTokenManager tokenManager = new BOPTokenManager(2, "0000", "ffff");
        List<String> tokenList = ImmutableList.of("0010", "0100", "1000");
        assertEquals("0010", tokenManager.findClosestToken("0010", tokenList));
        assertEquals("0100", tokenManager.findClosestToken("0090", tokenList));
        assertEquals("0100", tokenManager.findClosestToken("0100", tokenList));
        assertEquals("0100", tokenManager.findClosestToken("0120", tokenList));
        assertEquals("0100", tokenManager.findClosestToken("0810", tokenList));
        assertEquals("1000", tokenManager.findClosestToken("8860", tokenList));
        assertEquals("1000", tokenManager.findClosestToken("1000", tokenList));
        assertEquals("1000", tokenManager.findClosestToken("f000", tokenList));
    }

    @Test
    public void findClosestToken_tieGoesToLargerToken() {
        BOPTokenManager tokenManager = new BOPTokenManager(1, "00", "ff");
        assertEquals("a0", tokenManager.findClosestToken("50", ImmutableList.of("00", "a0")));
    }

    @Test
    public void test4Splits() {
        BOPTokenManager tokenManager = newBOPTokenManager(16);
        String expectedTokens = "00,40,80,c0";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i] + "000000000000000000000000000000"), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void test4SplitsWithPrefixSufficientPrecision() {
        BOPTokenManager tokenManager = new BOPTokenManager(
                20, "123456789a000000000000000000000000000000", "123456789b000000000000000000000000000000");
        String expectedTokens = "" +
                "123456789a000000000000000000000000000001,123456789a400000000000000000000000000001," +
                "123456789a800000000000000000000000000001,123456789ac00000000000000000000000000001";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i]), tokenManager.initialToken(splits, i, 1));
        }
    }

    @Test
    public void test4SplitsOffset() {
        BOPTokenManager tokenManager = newBOPTokenManager(16);
        String expectedTokens = "" +
                "00000000000000000000000000000001,40000000000000000000000000000001," +
                "80000000000000000000000000000001,c0000000000000000000000000000001";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i]), tokenManager.initialToken(splits, i, 1));
        }
    }

    @Test
    public void test16Splits() {
        BOPTokenManager tokenManager = newBOPTokenManager(16);
        String expectedTokens = "00,10,20,30,40,50,60,70,80,90,a0,b0,c0,d0,e0,f0";
        String[] tokens = expectedTokens.split(",");
        int splits = tokens.length;
        for (int i = 0; i < splits; i++) {
            assertEquals(toToken(tokens[i] + "000000000000000000000000000000"), tokenManager.initialToken(splits, i, 0));
        }
    }

    @Test
    public void testLocationOffset() {
        String allRegions = "us-west-2,us-east,us-west,eu-east,eu-west,ap-northeast,ap-southeast";
        String dcSuffixes = ",-dev,-qa,-prod";

        for (String region1 : allRegions.split(",")) {
            for (String region2 : allRegions.split(",")) {
                for (String dcSuffix1 : dcSuffixes.split(",")) {
                    for (String dcSuffix2 : dcSuffixes.split(",")) {
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
        BOPTokenManager tokenManager = new BOPTokenManager(8, "0000000000000000", "ffffffffffffffff");

        int h1 = TokenManager.locationOffset(Location.from("vijay"));
        int h2 = TokenManager.locationOffset(Location.from("vijay2"));
        Token t1 = tokenManager.initialToken(100, 10, h1);
        Token t2 = tokenManager.initialToken(100, 10, h2);

        BigInteger tokenDistance = new BigInteger(1, (byte[]) t1.getTokenValue()).subtract(new BigInteger(1, (byte[]) t2.getTokenValue()));
        long hashDifference = h1 - h2;

        assertEquals(BigInteger.valueOf(hashDifference), tokenDistance);

        Token t3 = tokenManager.initialToken(100, 99, h1);
        Token t4 = tokenManager.initialToken(100, 99, h2);
        tokenDistance = new BigInteger(1, (byte[]) t3.getTokenValue()).subtract(new BigInteger(1, (byte[]) t4.getTokenValue()));

        assertEquals(BigInteger.valueOf(hashDifference), tokenDistance);
    }

    @Test
    public void testNumberToToken() {
        assertEquals(toToken("00"), newBOPTokenManager(1).numberToToken(BigInteger.ZERO));
        assertEquals(toToken("0000000000"), newBOPTokenManager(5).numberToToken(BigInteger.ZERO));

        assertEquals(toToken("01"), newBOPTokenManager(1).numberToToken(BigInteger.ONE));
        assertEquals(toToken("ff"), newBOPTokenManager(1).numberToToken(BigInteger.valueOf(255)));
        assertEquals(toToken("0100"), newBOPTokenManager(2).numberToToken(BigInteger.valueOf(256)));
        assertEquals(toToken("ff00"), newBOPTokenManager(2).numberToToken(BigInteger.valueOf(255 * 256)));

        assertEquals(toToken("00000000000000000001"), newBOPTokenManager(10).numberToToken(BigInteger.ONE));

        assertEquals(toToken("0000000000000000ffffffffffffffff"), newBOPTokenManager(16).numberToToken(BigInteger.valueOf(2).pow(64).subtract(BigInteger.ONE)));
        assertEquals(toToken("ffffffffffffffffffffffffffffffff"), newBOPTokenManager(16).numberToToken(BigInteger.valueOf(2).pow(128).subtract(BigInteger.ONE)));

        assertEquals(toToken("00000000000000008000000000000000"), newBOPTokenManager(16).numberToToken(BigInteger.valueOf(2).pow(63)));
        assertEquals(toToken("00000000000000010000000000000000"), newBOPTokenManager(16).numberToToken(BigInteger.valueOf(2).pow(64)));
        assertEquals(toToken("80000000000000000000000000000000"), newBOPTokenManager(16).numberToToken(BigInteger.valueOf(2).pow(127)));
    }

    @Test
    public void testLongMinMaxTokens() {
        // First test with 18-byte min/max values, verify the token is created with the expected precision
        BOPTokenManager tokenManager1 = new BOPTokenManager(18,
                "555500112233445566778899aabbccddeeff",
                "5555ffeeddccbbaa99887766554433221100");
        assertEquals("55552ab616ccd838eefa5b111c7d49764ea4", tokenManager1.createToken(1, 3, 2, Location.from("eu-west-1")));

        // Next, test with much longer min/max values.
        BOPTokenManager tokenManager2 = new BOPTokenManager(50,
                "555500112233445566778899aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
                "5555ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100");
        assertEquals("55552ab616ccd838eefa5b111c7d333e9f54aab616ccd838eefa5b111c7d333e9f54aab616ccd838eefa5b111c7d49764ea4",
                tokenManager2.createToken(1, 3, 2, Location.from("eu-west-1")));
    }

    @Test
    public void testSanitizeToken() {
        Random random = new Random();
        byte[] bytes = new byte[random.nextInt(16)];
        random.nextBytes(bytes);
        Token token = new ByteOrderedPartitioner.BytesToken(bytes);

        BOPTokenManager tokenManager = newBOPTokenManager(16);
        String string = tokenManager.sanitizeToken(token.toString());
        assertTrue(string, string.matches("[0-9a-f]*"));

        assertEquals(token, new ByteOrderedPartitioner().getTokenFactory().fromString(string));
    }

    private BOPTokenManager newBOPTokenManager(int tokenLength) {
        return new BOPTokenManager(tokenLength, Strings.repeat("00", tokenLength), Strings.repeat("ff", tokenLength));
    }

    private static ByteOrderedPartitioner.BytesToken toToken(String string) {
        return new ByteOrderedPartitioner.BytesToken(Hex.hexToBytes(string));
    }
}
