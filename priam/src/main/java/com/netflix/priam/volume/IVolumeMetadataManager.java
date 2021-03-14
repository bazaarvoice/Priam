package com.netflix.priam.volume;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Defining interface for managing metadata about the Cassandra volume.  Although it works in all deploy environments
 * the real potential comes when the Cassandra data resides on an EBS volume.
 */
public interface IVolumeMetadataManager {
    @Nullable
    VolumeMetadata getVolumeMetadata() throws IOException;

    void setVolumeMetadata(VolumeMetadata volumeMetadata) throws IOException;

    void clearVolumeMetadata() throws IOException;

    @Nullable
    String getVolumeID();
}
