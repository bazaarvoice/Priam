package com.netflix.priam.utils;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.util.List;


public class SystemUtils {
    private static final Logger logger = LoggerFactory.getLogger(SystemUtils.class);

    public static String getDataFromUrl(String url) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setConnectTimeout(1000);
            conn.setReadTimeout(1000);
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200) {
                throw new ConfigurationException("Unable to get data for URL " + url);
            }
            byte[] b = new byte[2048];
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataInputStream d = new DataInputStream((FilterInputStream) conn.getContent());
            int c;
            while ((c = d.read(b, 0, b.length)) != -1) {
                bos.write(b, 0, c);
            }
            String return_ = new String(bos.toByteArray(), Charsets.UTF_8);
            logger.info("Calling URL API: {} returns: {}", url, return_);
            conn.disconnect();
            return return_;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * Delete all the files/dirs in the given Directory but don't delete the dir
     * itself.
     */
    public static void cleanupDir(String dirPath, List<String> childDirs) throws IOException {
        if (childDirs == null || childDirs.size() == 0) {
            FileUtils.cleanDirectory(new File(dirPath));
        } else {
            for (String childDir : childDirs) {
                FileUtils.cleanDirectory(new File(dirPath, childDir));
            }
        }
    }

    public static byte[] md5(byte[] buf) {
        try {
            MessageDigest mdigest = MessageDigest.getInstance("MD5");
            mdigest.update(buf, 0, buf.length);
            return mdigest.digest();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a Md5 string which is similar to OS Md5sum
     */
    public static String md5(File file) {
        try {
            return Files.hash(file, Hashing.md5()).toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toHex(byte[] digest) {
        StringBuilder sb = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            String hex = Integer.toHexString(b);
            if (hex.length() == 1) {
                sb.append("0");
            } else if (hex.length() == 8) {
                hex = hex.substring(6);
            }
            sb.append(hex);
        }
        return sb.toString().toLowerCase();
    }

    public static String toBase64(byte[] md5) {
        byte encoded[] = Base64.encodeBase64(md5, false);
        return new String(encoded);
    }

    public static void closeQuietly(JMXNodeTool tool) {
        try {
            tool.close();
        } catch (IOException e) {
            // Do nothing.
        }
    }

}
