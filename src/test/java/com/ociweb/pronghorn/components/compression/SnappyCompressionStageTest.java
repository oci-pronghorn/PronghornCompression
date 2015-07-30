package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// Snappy
import org.xerial.snappy.SnappyOutputStream;

import com.ociweb.pronghorn.components.compression.SnappyCompressionComponent.SnappyCompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class SnappyCompressionStageTest {

		private final Logger logger = LoggerFactory.getLogger(SnappyCompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig((byte)10, (byte)24, null, FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);
	        	
	       	SnappyCompressionStage stage = new SnappyCompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignal()  {
	    	
	    	GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);
	        
	       	SnappyCompressionStage stage = new SnappyCompressionStage(manager, input, output);
	       	Dumper dumper = new Dumper(manager, output);

	       	ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	       	service.startup(); 
        
            RingStreams.writeEOF(input);

	    	boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
	    	assertTrue(completed);
	    }

        private byte[] compress(List<byte[]> list) throws IOException {

            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            SnappyOutputStream compressionStream = new SnappyOutputStream(byteStream);

            for(byte[] data : list) {
                compressionStream.write(data);
            }

            compressionStream.flush();
            compressionStream.close();

            return byteStream.toByteArray();
        }

	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() throws IOException {

	    	GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);

            Generator generator = new Generator(manager, input, 100);
    		SnappyCompressionStage stage = new SnappyCompressionStage(manager, input, output);
    		Dumper dumper = new Dumper(manager, output);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();

			boolean completed = service.awaitTermination(1, TimeUnit.MINUTES);
    		assertTrue(completed);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(compress(generator.dataAsArray()), dumper.data());
	    }
}
