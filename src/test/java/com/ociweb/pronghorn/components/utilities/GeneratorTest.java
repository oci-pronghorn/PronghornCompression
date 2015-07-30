package com.ociweb.pronghorn.components.utilities;

import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class GeneratorTest {

    private final RingBufferConfig config = new RingBufferConfig((byte) 4,(byte) 18, null, FieldReferenceOffsetManager.RAW_BYTES);

    @Test 
    public void verifyGeneratorInstantiation() {
        GraphManager manager = new GraphManager();

        RingBuffer buffer = new RingBuffer(config);
        Generator generator = new Generator(manager, buffer, 100);
    }

    @Test 
    public void verifyGeneratorStopsAfterNumberOfArrays() {
        GraphManager manager = new GraphManager();

        RingBuffer buffer = new RingBuffer(config);
        Generator generator = new Generator(manager, buffer, 100);
        Dumper dumper = new Dumper(manager, buffer);

        ThreadPerStageScheduler scheduler = new ThreadPerStageScheduler(manager);
        scheduler.startup();
 
        boolean completed = scheduler.awaitTermination(4, TimeUnit.SECONDS);
        assertTrue(completed);

        assertArrayEquals(generator.data(), dumper.data());

        List<byte[]> bytes = generator.dataAsArray();
        assertEquals(100, bytes.size());
    }
}
