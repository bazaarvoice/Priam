package com.netflix.priam.volume;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AttachmentStatus;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceBlockDeviceMapping;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of {@link IVolumeMetadataManager}.  This implementation makes the following assumptions:
 * <ol>
 *     <li>
 *         The Cassandra data location is on an EBS volume.  Because of this assumption it writes a JSON metadata
 *         file to the data location so that it will always be co-located with the data if the EBS volume is
 *         transferred to another instance.
 *     </li>
 *     <li>
 *         The block device for the EBS volume has been configured in {@link AmazonConfiguration#cassandraVolumeBlockDevice}.
 *         This could be introspected from the OS using the Cassandra data location.  However, due to the variance in
 *         OS distributions and resulting uncertainty this implementation relies on the configuration to explicitly provide
 *         the block device name (e.g.; "/dev/sdi") and then uses the AWS API to resolve the volume ID from there.
 *         It is up to the admin to ensure the Cassandra data location is in fact on a directory on the volume mounted
 *         on that device.
 *     </li>
 * </ol>
 */
public class DefaultVolumeMetadataManager implements IVolumeMetadataManager {

    private static final String METADATA_FILE_NAME = "priam_volume_metadata.json";
    private static final Logger logger = LoggerFactory.getLogger(DefaultVolumeMetadataManager.class);

    private final File metadataFile;
    private final String blockDeviceName;
    private final AmazonConfiguration amazonConfiguration;
    private final AWSCredentialsProvider credentialsProvider;
    private final ObjectMapper objectMapper;

    @Inject
    public DefaultVolumeMetadataManager(AmazonConfiguration amazonConfiguration,
                                        CassandraConfiguration cassandraConfiguration,
                                        AWSCredentialsProvider credentialsProvider) {
        this.metadataFile = new File(cassandraConfiguration.getDataLocation(), METADATA_FILE_NAME);
        this.blockDeviceName = amazonConfiguration.getCassandraVolumeBlockDevice();
        this.credentialsProvider = credentialsProvider;
        this.amazonConfiguration = amazonConfiguration;
        objectMapper = Jackson.getObjectMapper();
    }

    @Override
    @Nullable
    public VolumeMetadata getVolumeMetadata() throws IOException {
        if (metadataFile.exists()) {
            try {
                return objectMapper.readValue(metadataFile, VolumeMetadata.class);
            } catch (JsonProcessingException e) {
                logger.warn("EBS volume metadata did not contain valid JSON");
            }
        }

        return null;
    }

    @Override
    public void setVolumeMetadata(VolumeMetadata volumeMetadata) throws IOException {
        checkNotNull(volumeMetadata, "volumeMetadata");
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(metadataFile, volumeMetadata);
    }

    @Override
    public void clearVolumeMetadata() throws IOException {
        if (metadataFile.exists()) {
            if (!metadataFile.delete()) {
                throw new IOException("Failed to delete volume metadata file");
            }
        }
    }

    @Override
    @Nullable
    public String getVolumeID() {
        try {
            return new GetVolumeID().call();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private class GetVolumeID extends RetryableCallable<String> {
        @Override
        public String retriableCall() throws Exception {
            if (blockDeviceName != null) {
                AmazonEC2 client = new AmazonEC2Client(credentialsProvider);
                client.setRegion(amazonConfiguration.getRegion());
                try {
                    DescribeInstancesRequest desc = new DescribeInstancesRequest().withInstanceIds(amazonConfiguration.getInstanceID());
                    DescribeInstancesResult res = client.describeInstances(desc);

                    for (Reservation resr : res.getReservations()) {
                        for (Instance ins : resr.getInstances()) {
                            for (InstanceBlockDeviceMapping blockDevice : ins.getBlockDeviceMappings()) {
                                if (blockDeviceName.equals(blockDevice.getDeviceName()) &&
                                        AttachmentStatus.fromValue(blockDevice.getEbs().getStatus()) == AttachmentStatus.Attached) {
                                    return blockDevice.getEbs().getVolumeId();
                                }
                            }
                        }
                    }
                } finally {
                    client.shutdown();
                }
            }
            return null;
        }
    }
}
