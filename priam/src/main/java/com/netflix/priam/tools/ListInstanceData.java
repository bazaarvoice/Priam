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
 * Print simple db data for a particular cluster to stdout.
 * <p>
 * AWS credentials can be supplied via environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_KEY" or JVM system
 * properties "aws.accessKeyId" and "aws.secretKey" or IAM instance profiles.
 */
public class ListInstanceData extends ConfiguredCommand<PriamConfiguration> {

    public ListInstanceData() {
        super("list-instance-data", "Lists SimpleDB instance data for a particular Cassandra cluster.");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("-c", "--cluster").required(true).help("Cassandra cluster name");
        subparser.addArgument("-d", "--domain").required(true).help("AWS SimpleDB domain");
        subparser.addArgument("-r", "--region").required(false).help("AWS SimpleDB region");
        subparser.addArgument("--sdb-assume-role-arn").required(false).help("assume role ARN for simpleDB");
    }

    @Override
    protected void run(Bootstrap<PriamConfiguration> bootstrap, Namespace namespace, PriamConfiguration priamConfiguration)
            throws Exception {
        String cluster = namespace.getString("cluster");
        String domain = namespace.getString("domain");
        String region = namespace.getString("region");
        String assumeRoleARN = Optional.fromNullable(namespace.getString("sdb-assume-role-arn"))
                .or(priamConfiguration.getCassandraConfiguration().getSdbRoleAssumptionArn())
                .orNull();

        SDBInstanceData sdb = getSimpleDB(domain, region, assumeRoleARN);

        for (PriamInstance id : Ordering.natural().sortedCopy(sdb.getAllIds(cluster))) {
            System.out.println(id);
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
