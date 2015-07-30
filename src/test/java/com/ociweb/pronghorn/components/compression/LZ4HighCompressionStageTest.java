package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// LZ4
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.LZ4CompressionComponent.LZ4HighCompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class LZ4HighCompressionStageTest {

		private final Logger logger = LoggerFactory.getLogger(LZ4HighCompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiationHighCompressionStage() {
	        
	        GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);
	        	
	       	LZ4HighCompressionStage stage = new LZ4HighCompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignalHighCompressionStage()  {
	    	
	    	GraphManager manager = new GraphManager();

	    	RingBuffer input = new RingBuffer(config);
	    	RingBuffer output = new RingBuffer(config);
	        
	       	LZ4HighCompressionStage stage = new LZ4HighCompressionStage(manager, input, output);
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

    		LZ4Compressor compressor = LZ4Factory.fastestInstance().highCompressor();
    		LZ4BlockOutputStream compressionStream = new LZ4BlockOutputStream(byteStream, 1 << 16, compressor);
    		
	   		compressionStream.write(input);
	   		compressionStream.flush();
	   		compressionStream.close();

    		return byteStream.toByteArray();
	    }

	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyHighlyCompressedOutput() throws IOException {
	    	
	    	GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);

    		Generator generator = new Generator(manager, input, 100);
    		LZ4HighCompressionStage stage = new LZ4HighCompressionStage(manager, input, output);
    		Dumper dumper = new Dumper(manager, output);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();

  			boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
    		assertTrue(completed);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(compress(generator.data()), dumper.data());
	    }
}
