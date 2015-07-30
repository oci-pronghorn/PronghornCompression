package com.ociweb.pronghorn.components.decompression;

import com.ociweb.pronghorn.components.compression.GzipCompressionComponent.GzipCompressionStage;
import com.ociweb.pronghorn.components.decompression.GzipDecompressionComponent.GzipDecompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GzipDecompressionStageTest {
		
		private final Logger logger = LoggerFactory.getLogger(GzipDecompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        	
       		GzipDecompressionStage stage = new GzipDecompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() {

	    	GraphManager manager = new GraphManager();

    		RingBuffer[] rings = new RingBuffer[] { 
    			  new RingBuffer(config)	// input to compression stage
    			, new RingBuffer(config)	// output for compression stage, input for decompression stage
    			, new RingBuffer(config) 	// output for decompression stage, input for dumper.
    		};

    		Generator generator = new Generator(manager, rings[0], 100);
    		GzipCompressionStage compressor = new GzipCompressionStage(manager, rings[0], rings[1]);
    		GzipDecompressionStage decompressor = new GzipDecompressionStage(manager, rings[1], rings[2]);
    		Dumper dumper = new Dumper(manager, rings[2]);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();

			boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
    		assertTrue(completed);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(generator.data(), dumper.data());
	    }

}
