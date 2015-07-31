package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.BZip2CompressionComponent.BZip2CompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class BZip2CompressionStageTest {
		
		private final int compressionBlockSize = 2;
		
		private final Logger logger = LoggerFactory.getLogger(BZip2CompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        	
       		BZip2CompressionStage stage = new BZip2CompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignal()  {

	    	GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);	        

	       	BZip2CompressionStage stage = new BZip2CompressionStage(manager, input, output);
	       	Dumper dumper = new Dumper(manager, output);

	       	ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	       	service.startup();

	        RingStreams.writeEOF(input);

			boolean completed = service.awaitTermination(1, TimeUnit.SECONDS);
            if (!completed) {
                logger.warn("Did not shut down cleanly, should investigate");
            }
	    }

	    // compress the input using the compression stream class
	    // return the results.
	    private byte[] compress(byte[] input) throws IOException {

	    		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	    		BZip2CompressorOutputStream compressionStream = new BZip2CompressorOutputStream(byteStream, compressionBlockSize);

	    		compressionStream.write(input);
	    		compressionStream.flush();
	    		compressionStream.close();

	    		return byteStream.toByteArray();
	    }

	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() {
	    	try {
	    		GraphManager manager = new GraphManager();

	    		RingBuffer input = new RingBuffer(config);
	    		RingBuffer output = new RingBuffer(config);
	    		
	    		
	    		Generator generator = new Generator(manager, input, 100);
	    		BZip2CompressionStage stage = new BZip2CompressionStage(manager, input, output, compressionBlockSize);
	    		Dumper dumper = new Dumper(manager, output);

	    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	    		service.startup();
	    			
	    		boolean completed = service.awaitTermination(2, TimeUnit.SECONDS);

	    		assertArrayEquals(compress(generator.data()), dumper.data());
	    		
	            if (!completed) {
	                logger.warn("Did not shut down cleanly, should investigate");
	            }
	    		
	    		
	    	}
	    	catch(IOException e) {
	    		logger.error("", e);
	    	}
	    }
}
