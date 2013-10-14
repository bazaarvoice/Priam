package com.netflix.priam.backup;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract Backup class for uploading files to backup location
 */
public abstract class AbstractBackup extends Task {

    private static final Logger logger = LoggerFactory.getLogger(AbstractBackup.class);

    protected final List<String> FILTER_KEYSPACE = Arrays.asList("OpsCenter");
    protected final List<String> FILTER_COLUMN_FAMILY = Arrays.asList("LocationInfo");
    protected final Provider<AbstractBackupPath> pathFactory;
    protected final IBackupFileSystem fs;

    @Inject
    public AbstractBackup(IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory) {
        super();
        this.fs = fs;
        this.pathFactory = pathFactory;
    }

    /**
     * Upload files in the specified dir. Does not delete the file in case of
     * error
     *
     * @param parent Parent dir
     * @param type   Type of file (META, SST, SNAP etc)
     * @return
     * @throws Exception
     */
    protected List<AbstractBackupPath> upload(File parent, BackupFileType type) throws Exception {
        List<AbstractBackupPath> bps = Lists.newArrayList();
        for (File file : parent.listFiles()) {
            final AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(file, type);
            upload(bp);
            bps.add(bp);
            file.delete();
        }
        return bps;
    }

    protected void maybeHardlinkLatest(String snapshotName) {
        AbstractBackupPath path = pathFactory.get();

        // only for ebs
        if (!"ebs".equals(path.backupConfiguration.getBackupTarget())){
            return;
        }

        logger.info("Hardlink snapshot {}", snapshotName);
        String baseBackup = path.backupConfiguration.getRestorePrefix() + "/" + path.remotePrefixBase("");
        logger.info("Backup base lives at: {}", baseBackup);
        String nextBackup = baseBackup + snapshotName;
        logger.info("Upcoming snapshot will be in: {}", nextBackup);
        String latestBackup = baseBackup + "latest";
        logger.info("Latest symlink should live at: {}", latestBackup);
        logger.info("Let's start by ensuring latest backup exists");

        File latestBackupFile = new File(latestBackup);

        try {

            if (!latestBackupFile.exists()){
                logger.info("It does not exist, so we should create it");
                logger.info("Symlink: ln -s " + nextBackup + " " + latestBackup);
                Runtime.getRuntime().exec("ln -s " + nextBackup + " " + latestBackup);
            } else {
                logger.info("It already exists, so we just need to copy from it");
                logger.info("cp -R -l -v " + latestBackup + "/* "+ nextBackup);
                Runtime.getRuntime().exec("cp -R -l -v " + latestBackup + "/* "+ nextBackup);
                logger.info("Now that we hard linked everything out of latest, update latest to point at the next snapshot");
                latestBackupFile.delete();
                logger.info("Running: {}", "ln -s " + nextBackup + " " + latestBackup);
                Runtime.getRuntime().exec("ln -s " + nextBackup + " " + latestBackup);
            }

        } catch (Exception e){
            throw Throwables.propagate(e);
        }

    }

    /**
     * Upload specified file (RandomAccessFile) with retries
     */
    protected void upload(final AbstractBackupPath bp) throws Exception {
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                if ("ebs".equals(bp.backupConfiguration.getBackupTarget())){
                    fs.upload(bp, bp.getBackupFile());
                } else { // s3 uses FileInputStream
                    fs.upload(bp, new AbstractBackupPath.RafInputStream(RandomAccessReader.open(bp.getBackupFile(), true)));
                }
                return null;
            }
        }.call();
    }

    /**
     * Filters unwanted keyspaces and column families
     */
    public boolean isValidBackupDir(File keyspaceDir, File columnFamilyDir, File backupDir) {
        if (!backupDir.isDirectory() && !backupDir.exists()) {
            return false;
        }
        String keyspaceName = keyspaceDir.getName();
        if (FILTER_KEYSPACE.contains(keyspaceName)) {
            return false;
        }
        String columnFamilyName = columnFamilyDir.getName();
        if (FILTER_COLUMN_FAMILY.contains(columnFamilyName)) {
            return false;
        }
        return true;
    }

}
