package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// LZ4
import net.jpountz.lz4.LZ4BlockOutputStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.LZ4CompressionComponent.LZ4CompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class LZ4CompressionStageTest {

		private final Logger logger = LoggerFactory.getLogger(LZ4CompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);
	        	
	       	LZ4CompressionStage stage = new LZ4CompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignal()  {
	    	
	    	GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);
	        
	       	LZ4CompressionStage stage = new LZ4CompressionStage(manager, input, output);
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
    		LZ4BlockOutputStream compressionStream = new LZ4BlockOutputStream(byteStream, 1 << 16);
    		
	    	compressionStream.write(input);
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
    		LZ4CompressionStage stage = new LZ4CompressionStage(manager, input, output);
    		Dumper dumper = new Dumper(manager, output);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();

    		boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
    		assertTrue(completed);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(compress(generator.data()), dumper.data());
	    }
}
