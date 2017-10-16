package com.netflix.priam.cassandra.extensions;

import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compare two Cassandra versions.  In some versions of Cassandra the class <code>com.datastax.driver.core.VersionNumber</code>
 * can be used for this.  However, the Priam agent shouldn't be compiled to any particular version of Cassandra, so
 * instead the relevant computation is performed here with no additional dependencies.
 */
public class VersionComparator implements Comparator<String> {

    @Override
    public int compare(String leftVersion, String rightVersion) {
        Pattern versionPattern = Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:\\.(\\d+))?([~\\-].+)?");
        Matcher leftMatcher = versionPattern.matcher(leftVersion);
        Matcher rightMatcher = versionPattern.matcher(rightVersion);

        if (!leftMatcher.matches()) {
            throw new IllegalArgumentException("Unsupported version: " + leftVersion);
        }
        if (!rightMatcher.matches()) {
            throw new IllegalArgumentException("Unsupported version: " + rightVersion);
        }

        for (int i=1; i <= 4; i++) {
            int left = leftMatcher.group(i) != null ? Integer.parseInt(leftMatcher.group(i)) : 0;
            int right = rightMatcher.group(i) != null ? Integer.parseInt(rightMatcher.group(i)) : 0;
            if (left != right) {
                return left < right ? -1 : 1;
            }
        }

        return 0;
    }
}
