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
package com.netflix.priam.cassandra.extensions;

import org.apache.cassandra.utils.FBUtilities;

import java.lang.instrument.Instrumentation;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A <a href="http://docs.oracle.com/javase/6/docs/api/java/lang/instrument/package-summary.html">PreMain</a> class
 * to run inside of the cassandra process. Contacts Priam for essential cassandra startup information
 * like token and seeds.
 */
public class PriamStartupAgent {
    public static String REPLACED_ADDRESS_MIN_VER = "1.2.11";
    public static String REPLACED_ADDRESS_FIRST_BOOT_MIN_VER = "2.0.9";

    public static void premain(String agentArgs, Instrumentation inst) {
        PriamStartupAgent agent = new PriamStartupAgent();
        agent.setPriamProperties();
    }

    private void setPriamProperties() {
        String token = null;
        String seeds = null;
        boolean isReplace = false;
        String replacedIp = "";

        while (true) {
            try {
                token = DataFetcher.fetchData("http://127.0.0.1:8080/v1/cassconfig/get_token");
                seeds = DataFetcher.fetchData("http://127.0.0.1:8080/v1/cassconfig/get_seeds");
                isReplace = Boolean.parseBoolean(DataFetcher.fetchData("http://127.0.0.1:8080/v1/cassconfig/is_replace_token"));
                replacedIp = DataFetcher.fetchData("http://127.0.0.1:8080/v1/cassconfig/get_replaced_ip");
            } catch (Exception e) {
                System.out.println("Failed to obtain startup data from priam, can not start yet. will retry shortly");
                e.printStackTrace();
            }

            if (token != null && seeds != null) {
                break;
            }
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e1) {
                // do nothing.
            }
        }

        System.setProperty("cassandra.initial_token", token);

        if (isReplace) {
            System.out.println("Detect cassandra version : " + FBUtilities.getReleaseVersionString());
            if (compareVersions(FBUtilities.getReleaseVersionString(), REPLACED_ADDRESS_MIN_VER) < 0) {
                System.out.println("Setting property cassandra.replace_token = " + token);
                System.setProperty("cassandra.replace_token", token);
            } else if (compareVersions(FBUtilities.getReleaseVersionString(), REPLACED_ADDRESS_FIRST_BOOT_MIN_VER) < 0) {
                System.out.println("Setting property cassandra.replace_address = " + replacedIp);
                System.setProperty("cassandra.replace_address", replacedIp);
            } else {
                System.out.println("Setting property cassandra.replace_address_first_boot = " + replacedIp);
                System.setProperty("cassandra.replace_address_first_boot", replacedIp);
            }
        }

    }

    /**
     * Compare to Cassandra versions.  In some versions of Cassandra the class <code>com.datastax.driver.core.VersionNumber</code>
     * can be used for this.  However, the Priam agent shouldn't be compiled to any particular version of Cassandra, so
     * instead the relevant computation is performed here with no additional dependencies.
     * @param leftVersion  A Cassandra version string
     * @param rightVersion A Cassandra version string
     * @return An negative, positive, or zero value if left is less than, greater than, or equal to right, respectively
     */
    private static int compareVersions(String leftVersion, String rightVersion) {
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
