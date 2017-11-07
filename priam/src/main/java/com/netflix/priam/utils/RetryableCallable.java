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
package com.netflix.priam.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;

public abstract class RetryableCallable<T> implements Callable<T> {
    private static final Logger logger = LoggerFactory.getLogger(RetryableCallable.class);
    public static final int DEFAULT_NUMBER_OF_RETRIES = 15;
    public static final long DEFAULT_WAIT_TIME = 100;
    private int retries;
    private long waitTime;
    private boolean logErrors = true;

    public RetryableCallable() {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME);
    }

    public RetryableCallable(int retries, long waitTime) {
        this.retries = retries;
        this.waitTime = waitTime;
    }

    public RetryableCallable(boolean logErrors) {
        this(DEFAULT_NUMBER_OF_RETRIES, DEFAULT_WAIT_TIME);

        this.logErrors = logErrors;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    public abstract T retriableCall() throws Exception;

    public T call() throws Exception {
        int retry = 0;
        while (true) {
            try {
                return retriableCall();
            } catch (CancellationException e) {
                throw e;
            } catch (NoMoreRetriesException e) {
                throw (Exception) e.getCause();
            } catch (Exception e) {
                retry++;
                if (retry == retries) {
                    throw e;
                }
                if (logErrors) {
                    StringWriter stringWriter = new StringWriter();
                    e.printStackTrace(new PrintWriter(stringWriter, true));
                    logger.error("Retry #{} for: {}", retry, e.getMessage());
                    logger.error("Details: " + stringWriter.getBuffer().toString());
                }
                Thread.sleep(waitTime);
            } finally {
                forEachExecution();
            }
        }
    }

    /**
     * Helper method to raise an exception and abandon any future retries.  This method return Exception
     * so it can be part of a <code>throw</code> statement.  This method always throws a
     * <code>NoMoreRetriesException</code> and never returns.
     */
    protected Exception exceptionWithNoRetries(Exception e) throws NoMoreRetriesException {
        throw new NoMoreRetriesException(e);
    }

    public void forEachExecution() {
        // do nothing by default.
    }

    /**
     * Encapsulating exception used to prevent retries.
     */
    private static class NoMoreRetriesException extends Exception {
        public NoMoreRetriesException(Throwable cause) {
            super(cause);
        }
    }
}