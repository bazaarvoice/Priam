package com.netflix.priam.aws;

public interface EBSFileSystemMBean {
    String MBEAN_NAME = "com.priam.aws.EBSFileSystemMBean:name=EBSFileSystemMBean";

    public int downloadCount();

    public int uploadCount();

    public int getActivecount();

    public long bytesUploaded();

    public long bytesDownloaded();
}
