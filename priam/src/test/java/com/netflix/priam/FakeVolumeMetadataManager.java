package com.netflix.priam;

import com.netflix.priam.volume.VolumeMetadata;
import com.netflix.priam.volume.IVolumeMetadataManager;

import javax.annotation.Nullable;
import java.io.IOException;

public class FakeVolumeMetadataManager implements IVolumeMetadataManager {

    private VolumeMetadata md;
    private String volumeId;

    public FakeVolumeMetadataManager(String volumeId) {
        this.volumeId = volumeId;
    }

    @Nullable
    @Override
    public VolumeMetadata getVolumeMetadata() throws IOException {
        return md;
    }

    @Override
    public void setVolumeMetadata(VolumeMetadata volumeMetadata) throws IOException {
        md = volumeMetadata;
    }

    @Override
    public void clearVolumeMetadata() throws IOException {
        md = null;
    }

    @Nullable
    @Override
    public String getVolumeID() {
        return volumeId;
    }
}
