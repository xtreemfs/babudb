/*  Copyright (c) 2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin and
    Barcelona Supercomputing Center - Centro Nacional de Supercomputacion.

    This file is part of XtreemFS. XtreemFS is part of XtreemOS, a Linux-based
    Grid Operating System, see <http://www.xtreemos.eu> for more details.
    The XtreemOS project has been developed with the financial support of the
    European Commission's IST program under contract #FP6-033576.

    XtreemFS is free software: you can redistribute it and/or modify it under
    the terms of the GNU General Public License as published by the Free
    Software Foundation, either version 2 of the License, or (at your option)
    any later version.

    XtreemFS is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with XtreemFS. If not, see <http://www.gnu.org/licenses/>.
 */
/*
 * AUTHORS: Jan Stender (ZIB), Björn Kolbeck (ZIB), Jesús Malo (BSC)
 */

package org.xtreemfs.include.common;

import org.xtreemfs.include.common.logging.Logging;
import org.xtreemfs.include.foundation.LifeCycleThread;

/**
 * A class that offers a local time with adjustable granularity.
 * 
 * @author bjko
 * @author flangner
 */
public final class TimeSync extends LifeCycleThread {
        
    /**
     * interval between updates of the local system clock.
     */
    private final int       localTimeRenew;
    
    /**
     * local sys time as of last update
     */
    private volatile long   localSysTime;
    
    /**
     * set to true to stop thread
     */
    private volatile boolean quit;
    
    private static TimeSync theInstance;
    
    /**
     * Creates a new instance of TimeSync
     */
    private TimeSync(int localTimeRenew) {
        super("TimeSync Thread");
        setDaemon(true);
        this.localTimeRenew = localTimeRenew;
    }
    
    /**
     * main loop
     */
    @Override
    public void run() {
        TimeSync.theInstance = this;
        notifyStarted();
        String tsStatus = " using the local clock (precision is "+this.localTimeRenew+"ms)";
        Logging.logMessage(Logging.LEVEL_INFO, this,"TimeSync is running "+tsStatus);
        while (!quit) {
            localSysTime = System.currentTimeMillis();
            if (! quit) {
                try {
                    TimeSync.sleep(localTimeRenew);
                } catch (InterruptedException ex) {
                    break;
                }
            }
            
        }
        Logging.logMessage(Logging.LEVEL_DEBUG, this,"shutdown complete");
        notifyStopped();
        theInstance = null;
    }

    public static TimeSync initialize(int localTimeRenew) {
        if (theInstance != null) {
            Logging.logMessage(Logging.LEVEL_WARN, null,"time sync already running");
            return theInstance;
        }

        TimeSync s = new TimeSync(localTimeRenew);
        s.start();
        return s;
    }

    public static void close() {
        if (theInstance == null)
            return;
        theInstance.shutdown();
    }
    
    /**
     * stop the thread
     */
    public void shutdown() {
        quit = true;
        this.interrupt();
    }
    
    /**
     * returns the current value of the local system time variable. Has a
     * resolution of localTimeRenew ms.
     */
    public static long getLocalSystemTime() {
        return getInstance().localSysTime;
    }
    
    public static long getLocalRenewInterval() {
        return getInstance().localTimeRenew;
    }
    
    public static TimeSync getInstance() {
        if (theInstance == null)
            throw new RuntimeException("TimeSync not initialized!");
        return theInstance;
    }
}
