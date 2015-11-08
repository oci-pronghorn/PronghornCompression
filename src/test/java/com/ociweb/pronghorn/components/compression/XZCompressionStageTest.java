package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.XZCompressionComponent.XZCompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class XZCompressionStageTest {

		private final Logger logger = LoggerFactory.getLogger(XZCompressionStageTest.class);
		private final PipeConfig config = new PipeConfig(RawDataSchema.instance);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

	    	Pipe input = new Pipe(config);
	    	Pipe output = new Pipe(config);
	        	
	       	XZCompressionStage stage = new XZCompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignal()  {
	    	
	    	GraphManager manager = new GraphManager();

	    	Pipe input = new Pipe(config);
	    	Pipe output = new Pipe(config);
	        
	       	XZCompressionStage stage = new XZCompressionStage(manager, input, output);
	       	Dumper dumper = new Dumper(manager, output);

	       	ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	       	service.startup();

	        RingStreams.writeEOF(input);

	       	boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
	    	assertTrue(completed);
	    }

	    // compress the input using the compression stream class
	    // return the results.
	    private byte[] compress(byte[] input) throws IOException {

    		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    		XZCompressorOutputStream compressionStream = new XZCompressorOutputStream(byteStream);

    		compressionStream.write(input);
    		compressionStream.flush();
    		compressionStream.close();

    		return byteStream.toByteArray();
	    }


	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() {
	    	try {
	    		GraphManager manager = new GraphManager();

	    		Pipe input = new Pipe(config);
	    		Pipe output = new Pipe(config);


	    		Generator generator = new Generator(manager, input, 100);
	    		XZCompressionStage stage = new XZCompressionStage(manager, input, output, 6);
	    		Dumper dumper = new Dumper(manager, output);

	    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	    		service.startup();

	    		boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
	    		assertTrue(completed);

				// make sure data traversing RingBuffers didn't mangle anything.
				assertArrayEquals(compress(generator.data()), dumper.data());
	    	}
	    	catch(IOException e) {
	    		logger.error("", e);
	    	}
	    }
}
