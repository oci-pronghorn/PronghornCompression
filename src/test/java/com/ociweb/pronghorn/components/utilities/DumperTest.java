package com.ociweb.pronghorn.components.utilities;

import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class DumperTest {

    private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

    @Test 
    public void verifyDumperInstantiation() {

        GraphManager manager = new GraphManager();

        RingBuffer buffer = new RingBuffer(config);
        Dumper dumper = new Dumper(manager, buffer);
    }

    @Test 
    public void verifyDumperStopsOnEOF() {
        GraphManager manager = new GraphManager();

        RingBuffer buffer = new RingBuffer(config);
        Dumper dumper = new Dumper(manager, buffer);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(manager);
        scheduler.startup();

        RingStreams.writeEOF(buffer);

        boolean completed = scheduler.awaitTermination(1, TimeUnit.SECONDS);
        assertTrue(completed);
    }
}
