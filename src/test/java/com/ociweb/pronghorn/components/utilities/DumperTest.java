package com.ociweb.pronghorn.components.utilities;

import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class DumperTest {

    private final PipeConfig config = new PipeConfig(FieldReferenceOffsetManager.RAW_BYTES);

    @Test 
    public void verifyDumperInstantiation() {

        GraphManager manager = new GraphManager();

        Pipe buffer = new Pipe(config);
        Dumper dumper = new Dumper(manager, buffer);
    }

    @Test 
    public void verifyDumperStopsOnEOF() {
        GraphManager manager = new GraphManager();

        Pipe buffer = new Pipe(config);
        Dumper dumper = new Dumper(manager, buffer);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(manager);
        scheduler.startup();

        RingStreams.writeEOF(buffer);

        boolean completed = scheduler.awaitTermination(1, TimeUnit.SECONDS);
        assertTrue(completed);
    }
}
