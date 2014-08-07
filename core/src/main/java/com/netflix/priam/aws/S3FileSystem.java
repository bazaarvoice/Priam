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
package com.netflix.priam.aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.ICredential;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.compress.ICompression;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.BlockingSubmitThreadPoolExecutor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of IBackupFileSystem for S3
 */
@Singleton
public class S3FileSystem implements IBackupFileSystem, S3FileSystemMBean {
    private static final Logger logger = LoggerFactory.getLogger(S3FileSystem.class);
    private static final int MAX_CHUNKS = 10000;
    private static final long UPLOAD_TIMEOUT = (2 * 60 * 60 * 1000L);

    private final Provider<AbstractBackupPath> pathProvider;
    private final ICompression compress;
    private final BackupConfiguration backupConfiguration;
    private final CassandraConfiguration cassandraConfiguration;
    private final AmazonConfiguration amazonConfiguration;
    private final ICredential cred;
    private RateLimiter rateLimiter;
    private final BlockingSubmitThreadPoolExecutor partUploadExecutor;

    private final AtomicLong bytesDownloaded = new AtomicLong();
    private final AtomicLong bytesUploaded = new AtomicLong();
    private final AtomicInteger uploadCount = new AtomicInteger();
    private final AtomicInteger downloadCount = new AtomicInteger();

    @Inject
    public S3FileSystem(Provider<AbstractBackupPath> pathProvider, ICompression compress, final BackupConfiguration backupConfiguration, CassandraConfiguration cassandraConfiguration, AmazonConfiguration amazonConfiguration, ICredential cred) {
        this.pathProvider = pathProvider;
        this.compress = compress;
        this.cassandraConfiguration = cassandraConfiguration;
        this.backupConfiguration = backupConfiguration;
        this.amazonConfiguration = amazonConfiguration;
        this.cred = cred;
        int numBackupThreads = backupConfiguration.getBackupThreads();
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(numBackupThreads);
        this.partUploadExecutor = new BlockingSubmitThreadPoolExecutor(numBackupThreads, queue, UPLOAD_TIMEOUT);
        double throttleLimit = backupConfiguration.getUploadThrottleBytesPerSec();
        rateLimiter = RateLimiter.create(throttleLimit < 1 ? Double.MAX_VALUE : throttleLimit);

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        String mbeanName = MBEAN_NAME;
        try {
            mbs.registerMBean(this, new ObjectName(mbeanName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void download(AbstractBackupPath path, OutputStream outputStream) throws BackupRestoreException {
        try {
            //logger.info("Downloading {}", path.getRemotePath());
            downloadCount.incrementAndGet();
            AmazonS3 client = getS3Client();

            S3Object obj = client.getObject(getPrefix(), path.getRemotePath());
            compress.decompressAndClose(obj.getObjectContent(), outputStream);

            bytesDownloaded.addAndGet(obj.getObjectMetadata().getContentLength());
        } catch (Exception e) {
            throw new BackupRestoreException(e.getMessage(), e);
        }
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException {
        uploadCount.incrementAndGet();

        AmazonS3 s3Client = getS3Client();
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(backupConfiguration.getS3BucketName(), path.getRemotePath());
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        DataPart initialDataPart = new DataPart(backupConfiguration.getS3BucketName(), path.getRemotePath(), initResponse.getUploadId());

        List<PartETag> partETags = Lists.newArrayList();

        long chunkSize = backupConfiguration.getChunkSizeMB();
        if (path.getSize() > 0) {
            chunkSize = (path.getSize() / chunkSize >= MAX_CHUNKS) ? (path.getSize() / (MAX_CHUNKS - 1)) : chunkSize;
        }
        logger.info("Uploading to {} with chunk size {}", path.getRemotePath(), chunkSize);

        try {
            Iterator<byte[]> chunks = compress.compress(in, chunkSize);
            // Upload parts.
            int partNum = 0;
            while (chunks.hasNext()) {
                byte[] chunk = chunks.next();
                rateLimiter.acquire(chunk.length);
                DataPart dp = new DataPart(++partNum, chunk, backupConfiguration.getS3BucketName(), path.getRemotePath(), initResponse.getUploadId());
                S3PartUploader partUploader = new S3PartUploader(s3Client, dp, partETags);
                partUploadExecutor.submit(partUploader);
                bytesUploaded.addAndGet(chunk.length);
            }
            partUploadExecutor.sleepTillEmpty();
            if (partNum != partETags.size()) {
                throw new BackupRestoreException("Number of parts (" + partNum + ") does not match the uploaded parts (" + partETags.size() + ")");
            }
            new S3PartUploader(s3Client, initialDataPart, partETags).completeUpload();
        } catch (Exception e) {
            new S3PartUploader(s3Client, initialDataPart, partETags).abortUpload();
            throw new BackupRestoreException("Error uploading file " + path.getFileName(), e);
        }
    }

    @Override
    public int getActivecount() {
        return partUploadExecutor.getActiveCount();
    }

    @Override
    public Iterator<AbstractBackupPath> list(String path, Date start, Date till) {
        return new S3FileIterator(pathProvider, getS3Client(), path, start, till);
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date) {
        return new S3PrefixIterator(cassandraConfiguration, amazonConfiguration, backupConfiguration, pathProvider, getS3Client(), date);
    }

    /**
     * Note: Current limitation allows only 100 object expiration rules to be
     * set. Removes the rule is set to 0.
     */
    @Override
    public void cleanup() {
        AmazonS3 s3Client = getS3Client();
        String clusterPath = pathProvider.get().clusterPrefix("");
        BucketLifecycleConfiguration lifeConfig = s3Client.getBucketLifecycleConfiguration(backupConfiguration.getS3BucketName());
        if (lifeConfig == null) {
            lifeConfig = new BucketLifecycleConfiguration();
            List<Rule> rules = Lists.newArrayList();
            lifeConfig.setRules(rules);
        }
        List<Rule> rules = lifeConfig.getRules();
        if (updateLifecycleRule(rules, clusterPath)) {
            if (rules.size() > 0) {
                lifeConfig.setRules(rules);
                s3Client.setBucketLifecycleConfiguration(backupConfiguration.getS3BucketName(), lifeConfig);
            } else {
                s3Client.deleteBucketLifecycleConfiguration(backupConfiguration.getS3BucketName());
            }
        }
    }

    private boolean updateLifecycleRule(List<Rule> rules, String prefix) {
        Rule rule = null;
        for (BucketLifecycleConfiguration.Rule lcRule : rules) {
            if (lcRule.getPrefix().equals(prefix)) {
                rule = lcRule;
                break;
            }
        }
        if (rule == null && backupConfiguration.getRetentionDays() <= 0) {
            return false;
        }
        if (rule != null && rule.getExpirationInDays() == backupConfiguration.getRetentionDays()) {
            logger.info("Cleanup rule already set");
            return false;
        }
        if (rule == null) {
            // Create a new rule
            rule = new BucketLifecycleConfiguration.Rule().withExpirationInDays(backupConfiguration.getRetentionDays()).withPrefix(prefix);
            rule.setStatus(BucketLifecycleConfiguration.ENABLED);
            rule.setId(prefix);
            rules.add(rule);
            logger.info("Setting cleanup for {} to {} days", rule.getPrefix(), rule.getExpirationInDays());
        } else if (backupConfiguration.getRetentionDays() > 0) {
            logger.info("Setting cleanup for {} to {} days", rule.getPrefix(), backupConfiguration.getRetentionDays());
            rule.setExpirationInDays(backupConfiguration.getRetentionDays());
        } else {
            logger.info("Removing cleanup rule for {}", rule.getPrefix());
            rules.remove(rule);
        }
        return true;
    }

    private AmazonS3 getS3Client() {
        return new AmazonS3Client(cred.getCredentialsProvider());
    }

    /**
     * Get S3 prefix which will be used to locate S3 files
     */
    public String getPrefix() {
        String prefix;
        if (StringUtils.isNotBlank(backupConfiguration.getRestorePrefix())) {
            prefix = backupConfiguration.getRestorePrefix();
        } else {
            prefix = backupConfiguration.getS3BucketName();
        }

        String[] paths = prefix.split(String.valueOf(S3BackupPath.PATH_SEP));
        return paths[0];
    }

    @Override
    public int downloadCount() {
        return downloadCount.get();
    }

    @Override
    public int uploadCount() {
        return uploadCount.get();
    }

    @Override
    public long bytesUploaded() {
        return bytesUploaded.get();
    }

    @Override
    public long bytesDownloaded() {
        return bytesDownloaded.get();
    }

}
