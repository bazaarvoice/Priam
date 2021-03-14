package com.netflix.priam.tools;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.netflix.priam.aws.SDBInstanceData;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.PriamConfiguration;
import com.netflix.priam.identity.PriamInstance;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

/**
 * Copy simple db data for a particular cluster from one AWS region to another.  This can be useful when migrating a
 * live cluster that used to store simple db data in us-east-1 but now wants to store it in the local region for better
 * performance and cross-data center isolation.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class CopyInstanceData extends ConfiguredCommand<PriamConfiguration> {

    public CopyInstanceData() {
        super("copy-instance-data", "Copies SimpleDB instance data from one region to another.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-c", "--cluster").required(true).help("Cassandra cluster name");
        subparser.addArgument("-d", "--domain").required(true).help("AWS SimpleDB domain");
        subparser.addArgument("--src-region").required(true).help("AWS SimpleDB source region");
        subparser.addArgument("--dest-region").required(true).help("AWS SimpleDB destination region");
        subparser.addArgument("--sdb-assume-role-arn").required(false).help("assume role ARN for simpleDB");
    }

    @Override
    protected void run(Bootstrap<PriamConfiguration> bootstrap, Namespace namespace, PriamConfiguration priamConfiguration)
            throws Exception {
        String cluster = namespace.getString("cluster");
        String domain = namespace.getString("domain");
        String srcRegion = namespace.getString("src-region");
        String destRegion = namespace.getString("dest-region");
        String assumeRoleARN = Optional.fromNullable(namespace.getString("sdb-assume-role-arn"))
                .or(priamConfiguration.getCassandraConfiguration().getSdbRoleAssumptionArn())
                .orNull();

        SDBInstanceData srcSdb = getSimpleDB(domain, srcRegion, assumeRoleARN);
        SDBInstanceData destSdb = getSimpleDB(domain, destRegion, assumeRoleARN);

        for (PriamInstance id : Ordering.natural().sortedCopy(srcSdb.getAllIds(cluster))) {
            System.out.println("Copying " + id + "...");
            try {
                destSdb.createInstance(id);
            } catch (Exception e) {
                System.err.println("Copy failed for " + id + ":" + e);
            }
        }
    }

    private static SDBInstanceData getSimpleDB(String domain, String region, String assumeRoleARN) {
        AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        if (!Strings.isNullOrEmpty(assumeRoleARN)) {
            awsCredentialsProvider = new STSAssumeRoleSessionCredentialsProvider(awsCredentialsProvider, assumeRoleARN, "awsRoleAssumptionSessionName");
        }

        AmazonConfiguration awsConfig = new AmazonConfiguration();
        awsConfig.setSimpleDbDomain(domain);
        awsConfig.setSimpleDbRegion(region);
        return new SDBInstanceData(awsCredentialsProvider, awsConfig);
    }
}