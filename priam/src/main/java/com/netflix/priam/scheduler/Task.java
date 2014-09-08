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
package com.netflix.priam.scheduler;

import com.google.common.base.Throwables;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Task class that should be implemented by all cron tasks. Jobconf will contain
 * any instance specific data
 * <p/>
 * NOTE: Constructor must not throw any exception. This will cause Quartz to set the job to failure
 */
public abstract class Task implements Job, TaskMBean {
    public State status = State.DONE;

    public static enum State {
        ERROR, RUNNING, DONE
    }

    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger executions = new AtomicInteger();

    protected Task() {
        this(ManagementFactory.getPlatformMBeanServer());
    }

    protected Task(MBeanServer mBeanServer) {
        // TODO: don't do mbean registration here
        String mbeanName = "com.priam.scheduler:type=" + this.getClass().getName();
        try {
            mBeanServer.registerMBean(this, new ObjectName(mbeanName));
            initialize();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * This method has to be implemented and cannot throw any exception.
     */
    public void initialize() {
        // nothing to initialize
    }

    public abstract void execute() throws Exception;

    /**
     * Main method to execute a task
     */
    public void execute(JobExecutionContext context) throws JobExecutionException {
        executions.incrementAndGet();
        try {
            if (status == State.RUNNING) {
                return;
            }
            status = State.RUNNING;
            execute();

        } catch (Throwable e) {
            status = State.ERROR;
            logger.error("Couldn't execute the task because of {}", e.toString(), e);
            errors.incrementAndGet();
        }
        if (status != State.ERROR) {
            status = State.DONE;
        }
    }

    public State state() {
        return status;
    }

    public int getErrorCount() {
        return errors.get();
    }

    public int getExecutionCount() {
        return executions.get();
    }

    public abstract String getName();

    public JobDetail getJobDetail() {
        return JobBuilder.newJob(getClass())
                .withIdentity("priam-scheduler", getName())
                .build();
    }

    public Trigger getCronTimeTrigger() {
        return TriggerBuilder
                .newTrigger()
                .withIdentity("priam-scheduler", getTriggerName())
                .withSchedule(CronScheduleBuilder.cronSchedule(getCronTime()))
                .build();
    }

    public String getCronTime() {
        return null;
    }

    public abstract String getTriggerName();

    public Trigger getTriggerToStartNow() {
        return TriggerBuilder
                .newTrigger()
                .withIdentity("priam-scheduler", getTriggerName())
                .startNow()
                .build();
    }

    // This method returns a trigger which schedules a job to run with interval in milliseconds.
    // If there is a requirement to schedule a job to run with interval in seconds/minutes/hours implement in the same way in this class.
    public Trigger getTriggerToStartNowAndRepeatInMillis() {
        return TriggerBuilder
                .newTrigger()
                .withIdentity("priam-scheduler", getTriggerName())
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withIntervalInMilliseconds(getIntervalInMilliseconds()).repeatForever().withMisfireHandlingInstructionFireNow())
                .build();
    }

    //Override this method in subclass if using repeated interval
    public long getIntervalInMilliseconds() {
        return -0L;
    }
}
