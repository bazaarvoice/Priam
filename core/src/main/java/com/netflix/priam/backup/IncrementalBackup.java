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

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.CassandraConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;

/*
 * Incremental/SSTable backup
 */
@Singleton
public class IncrementalBackup extends AbstractBackup {
    public static final String JOBNAME = "INCR_BACKUP_THREAD";
    private static final Logger logger = LoggerFactory.getLogger(IncrementalBackup.class);
    private final CassandraConfiguration cassandraConfiguration;

    @Inject
    public IncrementalBackup(CassandraConfiguration cassandraConfiguration, IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory) {
        super(fs, pathFactory);
        this.cassandraConfiguration = cassandraConfiguration;
    }

    @Override
    public void execute() throws Exception {
        File dataDir = new File(cassandraConfiguration.getDataLocation());
        logger.debug("Scanning for backup in: {}", dataDir.getAbsolutePath());
        File[] keyspaceDirs = dataDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        });
        for (File keyspaceDir : keyspaceDirs) {
            for (File columnFamilyDir : keyspaceDir.listFiles()) {
                File backupDir = new File(columnFamilyDir, "backups");
                if (!isValidBackupDir(keyspaceDir, columnFamilyDir, backupDir)) {
                    continue;
                }
                upload(backupDir, BackupFileType.SST);
            }
        }
    }


    @Override
    public String getName() {
        return JOBNAME;
    }

    @Override
    public long getIntervalInMilliseconds() {
        return 10L * 1000;
    }

    public String getTriggerName() {
        return "incrementalbackup-trigger";
    }

}
