package com.ociweb.pronghorn.components.decompression;

import com.ociweb.pronghorn.components.compression.GzipCompressionComponent.GzipCompressionStage;
import com.ociweb.pronghorn.components.decompression.GzipDecompressionComponent.GzipDecompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
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
		private final PipeConfig config = new PipeConfig(RawDataSchema.instance);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

    		Pipe input = new Pipe(config);
    		Pipe output = new Pipe(config);
	        	
       		GzipDecompressionStage stage = new GzipDecompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() {

	    	GraphManager manager = new GraphManager();

    		Pipe[] rings = new Pipe[] { 
    			  new Pipe(config)	// input to compression stage
    			, new Pipe(config)	// output for compression stage, input for decompression stage
    			, new Pipe(config) 	// output for decompression stage, input for dumper.
    		};

    		Generator generator = new Generator(manager, rings[0], 100);
    		GzipCompressionStage compressor = new GzipCompressionStage(manager, rings[0], rings[1]);
    		GzipDecompressionStage decompressor = new GzipDecompressionStage(manager, rings[1], rings[2]);
    		Dumper dumper = new Dumper(manager, rings[2]);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();

			boolean completed = service.awaitTermination(1, TimeUnit.SECONDS);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(generator.data(), dumper.data());
			
            if (!completed) {
                logger.warn("Did not shut down cleanly, should investigate");
            }
	    }

}
