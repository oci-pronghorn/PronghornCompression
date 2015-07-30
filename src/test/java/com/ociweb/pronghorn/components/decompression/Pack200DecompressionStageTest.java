package com.ociweb.pronghorn.components.decompression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.Pack200CompressionComponent.Pack200CompressionStage;
import com.ociweb.pronghorn.components.decompression.Pack200DecompressionComponent.Pack200DecompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class Pack200DecompressionStageTest {
		
		private final Logger logger = LoggerFactory.getLogger(Pack200DecompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        	
       		Pack200DecompressionStage stage = new Pack200DecompressionStage(manager, input, output);
	    }
	    
	    @Ignore("As it turns out, Pack200 is not a reversible compression. We can't verify working stages using this technieque")
	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() {

	    	GraphManager manager = new GraphManager();

    		RingBuffer[] rings = new RingBuffer[] { 
    			  new RingBuffer(config)	// input to compression stage
    			, new RingBuffer(config)	// output for compression stage, input for decompression stage
    			, new RingBuffer(config) 	// output for decompression stage, input for dumper.
    		};

    		Generator generator = new Generator(manager, rings[0], 100);
    		Pack200CompressionStage compressor = new Pack200CompressionStage(manager, rings[0], rings[1]);
    		Pack200DecompressionStage decompressor = new Pack200DecompressionStage(manager, rings[1], rings[2]);
    		Dumper dumper = new Dumper(manager, rings[2]);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();


			boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
			assertTrue(completed);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(generator.data(), dumper.data());
	    }

}
