package com.netflix.priam.utils;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.config.CassandraConfiguration;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Guice factory for {@link TokenManager}.
 */
public class TokenManagerProvider implements Provider<TokenManager> {
    private final CassandraConfiguration _cassandraConfiguration;

    @Inject
    public TokenManagerProvider(CassandraConfiguration cassandraConfiguration) {
        _cassandraConfiguration = cassandraConfiguration;
    }

    @Override
    public TokenManager get() {
        IPartitioner partitioner;
        try {
            partitioner = FBUtilities.newPartitioner(TokenManager.clientPartitioner(_cassandraConfiguration.getPartitioner()));
        } catch (ConfigurationException e) {
            throw Throwables.propagate(e);
        }
        if (partitioner instanceof RandomPartitioner) {
            return BigIntegerTokenManager.forRandomPartitioner();
        }
        if (partitioner instanceof Murmur3Partitioner) {
            return BigIntegerTokenManager.forMurmur3Partitioner();
        }
        if (partitioner instanceof ByteOrderedPartitioner) {
            return new BOPTokenManager(_cassandraConfiguration.getTokenLength(),
                    _cassandraConfiguration.getMinimumToken(), _cassandraConfiguration.getMaximumToken());
        }
        throw new UnsupportedOperationException(partitioner.getClass().getName());
    }
}
