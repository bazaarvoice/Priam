/**
 * Copyright 2014 Bazaarvoice, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;


import static com.google.common.base.Preconditions.checkArgument;

/**
 * Token manager for clusters using ByteOrderedPartitioner.  By default, generates and assigns 16-byte tokens
 * evenly distributed between 0x00000000000000000000000000000000 and 0xffffffffffffffffffffffffffffffff.  This
 * is a good choice for ByteOrderedPartitioner-based clusters where the first {@code n} bits of partition keys are
 * distributed evenly across all possible combinations, where {@code (2^n)*replicationFactor >= clusterSize}.
 * <p/>
 * 16-byte tokens are the same size as RandomPartitioner tokens, but the tokens aren't interchangeable.  Among other
 * things, RandomPartitioner tokens are formatted as decimal numbers, ByteOrderedPartitioner tokens are formatted as
 * hex strings.  RandomPartitioner tokens range from 0 to 2^127, ByteOrderedPartitioner tokens w/this class range
 * from 0 to 2^128-1.
 */
public class BOPTokenManager extends TokenManager {
    private static final Logger logger = LoggerFactory.getLogger(BOPTokenManager.class);
    // Tokens are expected to be lowercase hex.  The findClosestToken method will break if uppercase hex.
    private static final CharMatcher VALID_TOKEN = CharMatcher.inRange('0', '9').or(CharMatcher.inRange('a', 'f'));

    private final ByteOrderedPartitioner partitioner = new ByteOrderedPartitioner();
    private final int tokenLength; // In bytes
    private final Token<byte[]> minimumToken;
    private final Token<byte[]> maximumToken;

    @Inject
    public BOPTokenManager(int tokenLength, String minimumToken, String maximumToken) {
        this.tokenLength = tokenLength;
        this.minimumToken = partitioner.getTokenFactory().fromString(checkTokenString(minimumToken));
        this.maximumToken = partitioner.getTokenFactory().fromString(checkTokenString(maximumToken));
        checkArgument(this.minimumToken.compareTo(this.maximumToken) < 0,
                "Minimum token must be < maximum token: %s %s", minimumToken, maximumToken);
    }

    /**
     * Calculate a token for the given position, evenly spaced from other size-1 nodes.  See
     * http://wiki.apache.org/cassandra/Operations.
     *
     * @param size     number of slots by which the token space will be divided
     * @param position slot number, multiplier
     * @param offset   added to token
     * @return MAXIMUM_TOKEN / size * position + offset, if <= MAXIMUM_TOKEN, otherwise wrap around the MINIMUM_TOKEN
     */
    @VisibleForTesting
    Token<byte[]> initialToken(int size, int position, int offset) {
        checkArgument(size > 0, "size must be > 0");
        checkArgument(offset >= 0, "offset must be >= 0");
        checkArgument(position >= 0, "position must be >= 0");

        // Assume keys are distributed evenly between the minimum and maximum token.  This is often a bad
        // assumption with the ByteOrderedPartitioner, but that's why everyone is discouraged from using it.

        // Subdivide between the min and max using tokenLength bytes of precision.
        BigInteger min = new BigInteger(1, minimumToken.token);
        BigInteger max = new BigInteger(1, maximumToken.token);
        BigInteger range = max.subtract(min);

        // Fix per EMO-5319
        if( size == 3 ){
            int coefficient = position;
        }else if( size < 3 ){
            logger.info("Cluster size is too small. It needs to be at least 3. Terminating program.");
            System.exit(0); 
        }else{
            int coefficient = (1 + 2 * position - size );     // coefficient as per the formula in EMO-5319,but not divided by size
        }
        BigInteger value = max.add(BigInteger.ONE)     // add 1 since max is inclusive, helps get the splits to round #s
                .subtract(min)
                .divide(BigInteger.valueOf(size))             // use the coefficient as per the formula in EMO-5319
                .multiply(BigInteger.valueOf(coefficient))    // use the coefficient as per the formula in EMO-5319
                .add(BigInteger.valueOf(offset))
                .mod(range)  // Wrap around if out of range
                .add(min);
        Token<byte[]> token = numberToToken(value);
        logger.info("Token data per EMO-5319 coefficient / size: {} / {}, position: {}, token: {}, tokenchecked:", coefficient.toString() , size.toString(), position.toString(), numberToToken(value).toString(), Ordering.natural().min(Ordering.natural().max(token, minimumToken), maximumToken).toString());

        // Make sure the token stays within the configured bounds.
        return Ordering.natural().min(Ordering.natural().max(token, minimumToken), maximumToken);
    }

    @Override
    public String createToken(int mySlot, int totalCount, String region) {
        return partitioner.getTokenFactory().toString(initialToken(totalCount, mySlot, regionOffset(region)));
    }

    @Override
    public String findClosestToken(String tokenToSearch, List<String> tokenList) {
        checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        checkTokenString(tokenToSearch);
        for (String token : tokenList) {
            checkTokenString(token);
        }

        // Rely on the fact that hex-encoded strings sort in the same relative order as the BytesToken byte arrays.
        List<String> sortedTokens = Ordering.natural().sortedCopy(tokenList);
        int i = Ordering.natural().binarySearch(sortedTokens, tokenToSearch);
        if (i < 0) {
            i = -i - 1;
            if ((i >= sortedTokens.size()) ||
                    (i > 0 && lessThanMidPoint(sortedTokens.get(i - 1), tokenToSearch, sortedTokens.get(i)))) {
                --i;
            }
        }
        return sortedTokens.get(i);
    }

    private boolean lessThanMidPoint(String min, String token, String max) {
        Token.TokenFactory<byte[]> tf = partitioner.getTokenFactory();
        BytesToken midpoint = partitioner.midpoint(tf.fromString(min), tf.fromString(max));
        return tf.fromString(token).compareTo(midpoint) < 0;
    }

    @VisibleForTesting
    Token<byte[]> numberToToken(BigInteger number) {
        checkArgument(number.signum() >= 0, "Token math should not yield negative numbers: %s", number);
        byte[] numberBytes = number.toByteArray();
        int numberOffset = numberBytes[0] != 0 ? 0 : 1;  // Ignore the first if it's zero ("sign byte") to ensure byte[] length <= tokenLength.
        int numberLength = numberBytes.length - numberOffset;
        checkArgument(numberLength <= tokenLength, "Token math should not yield tokens bigger than maxToken (%s bytes): %s", tokenLength, number);

        // Left-pad the number with zeros when creating the number array.
        byte[] tokenBytes = new byte[tokenLength];
        System.arraycopy(numberBytes, numberOffset, tokenBytes, tokenBytes.length - numberLength, numberLength);
        return partitioner.getToken(ByteBuffer.wrap(tokenBytes));
    }

    public String sanitizeToken(String jmxTokenString) {
        // BytesToken.toString() returns "Token(bytes[<hex>])" but ByteOrderedPartitioner expects just "<hex>".
        String prefix = "Token(bytes[", suffix = "])";
        if (jmxTokenString.startsWith(prefix) && jmxTokenString.endsWith(suffix)) {
            jmxTokenString = jmxTokenString.substring(prefix.length(), jmxTokenString.length() - suffix.length());
        }
        return jmxTokenString;
    }

    private String checkTokenString(String token) {
        checkArgument(token.length() == tokenLength * 2,
                "Token string should be %s characters long (%s bytes): %s", tokenLength * 2, tokenLength, token);
        checkArgument(VALID_TOKEN.matchesAllOf(token), "Token must be lowercase hex: %s", token);
        return token;
    }
}
