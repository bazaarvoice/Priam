/**
 * Copyright 2013 Netflix, Inc.
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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.netflix.priam.identity.Location;

import java.math.BigInteger;
import java.util.List;

/**
 * A token manager appropriate for use with the RandomPartitioner and the Murmur3Partitioner that
 */
public class BigIntegerTokenManager extends TokenManager {
    private final BigInteger minimumToken;
    private final BigInteger maximumToken;

    public static BigIntegerTokenManager forRandomPartitioner() {
        return new BigIntegerTokenManager(BigInteger.ZERO, BigInteger.valueOf(2).pow(127));
    }

    public static BigIntegerTokenManager forMurmur3Partitioner() {
        return new BigIntegerTokenManager(BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE));
    }

    private BigIntegerTokenManager(BigInteger minimumToken, BigInteger maximumToken) {
        this.minimumToken = minimumToken;
        this.maximumToken = maximumToken;
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
    BigInteger initialToken(int size, int position, int offset) {
        Preconditions.checkArgument(size > 0, "size must be > 0");
        Preconditions.checkArgument(offset >= 0, "offset must be >= 0");
        /*
         * TODO: Is this it valid to add "&& position < size" to the following precondition?  This currently causes
         * unit test failures.
         */
        Preconditions.checkArgument(position >= 0, "position must be >= 0");
        return maximumToken.add(BigInteger.ONE)  // add 1 since max is inclusive, helps get the splits to round #s
                .subtract(minimumToken)
                .divide(BigInteger.valueOf(size))
                .multiply(BigInteger.valueOf(position))
                .add(BigInteger.valueOf(offset))
                .add(minimumToken);
    }

    @Override
    public String createToken(int mySlot, int totalCount, Location location) {
        return initialToken(totalCount, mySlot, locationOffset(location)).toString();
    }

    @Override
    public String findClosestToken(String tokenStringToSearch, List<String> tokenList) {
        Preconditions.checkArgument(!tokenList.isEmpty(), "token list must not be empty");
        BigInteger tokenToSearch = new BigInteger(tokenStringToSearch);
        List<BigInteger> sortedTokens = Ordering.natural().immutableSortedCopy(Lists.transform(tokenList,
                new Function<String, BigInteger>() {
                    @Override
                    public BigInteger apply(String token) {
                        return new BigInteger(token);
                    }
                }));
        int i = Ordering.natural().binarySearch(sortedTokens, tokenToSearch);
        if (i < 0) {
            i = -i - 1;
            if ((i >= sortedTokens.size()) ||
                    (i > 0 && sortedTokens.get(i).subtract(tokenToSearch).compareTo(tokenToSearch.subtract(sortedTokens.get(i - 1))) > 0)) {
                --i;
            }
        }
        return sortedTokens.get(i).toString();
    }

    @Override
    public String sanitizeToken(String jmxTokenString) {
        // BigIntegerToken.toString() returns BigInteger.toString() which is the format expected by the RandomPartitioner.
        return jmxTokenString;
    }
}
