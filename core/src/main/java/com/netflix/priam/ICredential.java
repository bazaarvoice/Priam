package com.netflix.priam;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.google.inject.ImplementedBy;
import com.netflix.priam.aws.DefaultCredentials;

/**
 * Credential file interface for services supporting
 * Access ID and key authentication
 */
@ImplementedBy(DefaultCredentials.class)
public interface ICredential {
    /**
     * Returns AWS credentials provider.
     */
    AWSCredentialsProvider getCredentialsProvider();
}
