package com.netflix.priam.backup;

import java.util.Date;
import java.util.Iterator;

/**
 * Interface representing a backup storage as a file system
 */
public interface
        IBackupFileSystem<InStream,OutStream> { // generics to support using FileChannel for EBS filesystem, but allow S3 to continue using InputStream/OutputStream
    /**
     * Write the contents of the specified remote path to the output stream and
     * close
     */
    public void download(AbstractBackupPath path, OutStream os) throws BackupRestoreException;

    /**
     * Upload/Backup to the specified location with contents from the input
     * stream. Closes the InputStream after its done.
     */
    public void upload(AbstractBackupPath path, InStream in) throws BackupRestoreException;

    /**
     * List all files in the backup location for the specified time range.
     */
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till);

    /**
     * Get a list of prefixes for the cluster available in backup for the specified date
     */
    public Iterator<AbstractBackupPath> listPrefixes(Date date);

    /**
     * Runs cleanup or set retention
     */
    public void cleanup();

    /**
     * Get number of active upload or downloads
     */
    public int getActivecount();

    /**
     * Any post-backup tasks you may want to perform
     */
    public void snapshotEbs(String snapshotName);
}
