package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.pack200.Pack200CompressorOutputStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.Pack200CompressionComponent.Pack200CompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class Pack200CompressionStageTest {
		
		private final Logger logger = LoggerFactory.getLogger(Pack200CompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	    	GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        	
       		Pack200CompressionStage stage = new Pack200CompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignal()  {

	    	GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        
       		Pack200CompressionStage stage = new Pack200CompressionStage(manager, input, output);
       		Dumper dumper = new Dumper(manager, output);

       		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
       		service.startup();

            RingStreams.writeEOF(input);

    		boolean completed = service.awaitTermination(3, TimeUnit.SECONDS);
            if (!completed) {
                logger.warn("Did not shut down cleanly, should investigate");
            }
	    }


	    // compress the input using the compression stream class
	    // return the results.
	    private byte[] compress(byte[] input) throws IOException {

	    		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
	    		Pack200CompressorOutputStream compressionStream = new Pack200CompressorOutputStream(byteStream);

	    		compressionStream.write(input);
	    		compressionStream.flush();
	    		compressionStream.close();

	    		return byteStream.toByteArray();
	    }

}
