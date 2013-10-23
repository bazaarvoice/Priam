package com.netflix.priam.backup;

import com.netflix.priam.utils.Throttle;
import org.junit.Test;

import java.util.Random;

public class ThrottleTest
{
    protected static final int THROTTAL_MB_PER_SEC = 1;

    @Test
    public void throttleTest()
    {
        Throttle throttle = new Throttle(this.getClass().getCanonicalName(), new Throttle.ThroughputFunction()
        {
            public int targetThroughput()
            {
                int totalBytesPerMS = (THROTTAL_MB_PER_SEC * 1024 * 1024)/1000;
                return totalBytesPerMS;
            }
        });
        long start = System.currentTimeMillis();
        Random ran = new Random();
        for (int i = 0; i < 10; i++)
        {
            long simulated = ran.nextInt(10) * 1024 * 1024;
            //System.out.println("Simulating upload of "+ simulated  + " @ " + System.currentTimeMillis());
            throttle.throttle(simulated);
        }
        
        //System.out.println("Completed in: " + (System.currentTimeMillis() - start));
    }
}
