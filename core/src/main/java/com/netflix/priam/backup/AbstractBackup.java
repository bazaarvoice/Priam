/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;
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
    protected final Multimap<String, String> FILTER_COLUMN_FAMILY = ImmutableMultimap.<String, String>builder()
            .put("system", "local")
            .put("system", "peers")
            .put("system", "LocationInfo")
            .build();
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
     * @throws Exception
     */
    protected List<AbstractBackupPath> upload(File parent, final BackupFileType type) throws Exception {
        List<AbstractBackupPath> bps = Lists.newArrayList();
        for (final File file : parent.listFiles()) {
            logger.debug("Uploading file {} for backup", file.getCanonicalFile());
            try {
                AbstractBackupPath abp = new RetryableCallable<AbstractBackupPath>(3, RetryableCallable.DEFAULT_WAIT_TIME) {
                    public AbstractBackupPath retriableCall() throws Exception {
                        final AbstractBackupPath bp = pathFactory.get();
                        bp.parseLocal(file, type);
                        upload(bp);
                        file.delete();
                        return bp;
                    }
                }.call();

                bps.add(abp);
            } catch (Exception e) {
                logger.error("Failed to upload local file {}. Ignoring to continue with rest of backup.", file, e);
            }
        }
        return bps;
    }

    /**
     * Upload specified file (RandomAccessFile) with retries
     */
    protected void upload(final AbstractBackupPath bp) throws Exception {
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                fs.upload(bp, bp.localReader());
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
        if (FILTER_COLUMN_FAMILY.containsEntry(keyspaceName, columnFamilyName)) {
            return false;
        }
        return true;
    }

}
