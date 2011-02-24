package org.xtreemfs.babudb.replication.service;


import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xtreemfs.babudb.lsmdb.LSN;

public class HeartbeatTest {

    private final static int MAX_PARTICIPANTS = 20;
    
    private final static int MIN_PARTICIPANTS = 1;
    
    private final static int BASIC_PORT = 12345;
    
    private HeartbeatThread hbt;
    
    @Before
    public void setUp() throws Exception {
        int participants = new Random().nextInt(MAX_PARTICIPANTS) + MIN_PARTICIPANTS;
        hbt = new HeartbeatThread(
                new ParticipantsOverviewMock(BASIC_PORT, participants), BASIC_PORT);
        hbt.start(new LSN(0,0L));
        hbt.waitForStartup();
    }

    @After
    public void tearDown() throws Exception {
        hbt.shutdown();
        hbt.waitForShutdown();
    }

    @Test
    public void testInfarctAndUpdate() throws Exception {
        
    }
}
