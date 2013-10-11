package com.netflix.priam.aws;

import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator representing list of backup files available on attached EBS volume(s)
 */
public class EBSFileIterator implements Iterator<AbstractBackupPath> {
    private static final Logger logger = LoggerFactory.getLogger(EBSFileIterator.class);
    private final Provider<AbstractBackupPath> pathProvider;
    private final Date start;
    private final Date till;
    private Iterator<AbstractBackupPath> iterator;
    private List<File> objectListing = Lists.newArrayList();

    public EBSFileIterator(Provider<AbstractBackupPath> pathProvider, String path, Date start, Date till) {
        this.start = start;
        this.till = till;
        this.pathProvider = pathProvider;

        logger.info("start: {}", start);
        logger.info("till: {}", till);
        logger.info("path: {}", path);
        logger.info("remote prefix: {}", pathProvider.get().remotePrefix(start, till, path));

        String filePath = pathProvider.get().getRestorePrefix();

        File fileBucket = new File(filePath + EBSBackupPath.PATH_SEP + pathProvider.get().remotePrefixBase(path));

        if (null != fileBucket.listFiles()){
            for (File fileBucketHolder : fileBucket.listFiles()){
                // only add files matching the date pattern
                if (fileBucketHolder.getName().startsWith(pathProvider.get().match(start, till))){
                    logger.info("Adding path to object listing bucket: {}", fileBucketHolder);
                    objectListing.add(fileBucketHolder);
                }
            }
        }

        logger.info("object listing: {}", objectListing);

        iterator = createIterator();

    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    private Iterator<AbstractBackupPath> createIterator() {

        List<AbstractBackupPath> temp = Lists.newArrayList();

        for ( File objectList : objectListing){

            logger.info("object listing root path: {}", objectList.getAbsolutePath());
            logger.info("object listing: {}", objectList.listFiles());

            if (null == objectList.listFiles()){
                logger.error("No files yielded from your EBS volume!");
            } else {
                for ( File nextFile : FileUtils.listFiles(objectList, null, true) ) {
                    AbstractBackupPath path = pathProvider.get();
                    String nextFilePath = nextFile.getAbsolutePath().replaceFirst(path.getRestorePrefix(), "");
                    logger.info("Absolute path of file to process: {}", nextFile.getAbsolutePath());
                    logger.info("Next file path: {}", nextFilePath);
                    path.parseRemote(nextFilePath);
                    logger.info("New file " + nextFilePath + " path = " + path.getRemotePath() + " " + start + " end: " + till + " my " + path.getTime());
                    if ((path.getTime().after(start) && path.getTime().before(till)) || path.getTime().equals(start)) {
                        temp.add(path);
                        logger.debug("Added file " + nextFile.getName());
                    }
                }
            }

        }

        return temp.iterator();
    }

    @Override
    public AbstractBackupPath next() {
        return iterator.next();
    }

    @Override
    public void remove() {
        throw new IllegalStateException();
    }
}
