package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

// PAQ
import kanzi.io.CompressedOutputStream;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.KanziCompressionComponent.KanziCompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class KanziCompressionStageTest {

		private final Logger logger = LoggerFactory.getLogger(KanziCompressionStageTest.class);
		private final PipeConfig config = new PipeConfig(RawDataSchema.instance, 50, 4096);

	    private final String codecs[] = new String[] {"PAQ", "FPAQ", "ANS", "HUFFMAN", "RANGE", "NONE"};
        private final String transforms[] = new String[] {"BWT", "BWTS", "SNAPPY", "LZ4", "RLT", "BWT+MTF", "BWT+TIMESTAMP", "NONE"};

		private void verifyInstantiation(String codec, String transform) {

			GraphManager manager = new GraphManager();
			
	    	Pipe input = new Pipe(config);
	    	Pipe output = new Pipe(config);
	        	
	       	KanziCompressionStage stage = new KanziCompressionStage(manager, input, output, codec, transform);
		}

		@Test 
		public void verifyInstantiation() {

	    	for(String codec : codecs) {
	    		for(String transform : transforms) {

	    			if(codec == "NONE" && transform == "SNAPPY") continue;

	    			verifyInstantiation(codec, transform);
	    		}
	    	}

		}

	    private void verifyRunStopsOnSignal(String codec, String transform) {

	    	GraphManager manager = new GraphManager();

	    	Pipe input = new Pipe(config);
	    	Pipe output = new Pipe(config);
	        
	       	KanziCompressionStage stage = new KanziCompressionStage(manager, input, output, codec, transform);
	       	Dumper dumper = new Dumper(manager, output);

	       	ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	       	service.startup();

			RingStreams.writeEOF(input);

	    	boolean completed = service.awaitTermination(1, TimeUnit.SECONDS);
            if (!completed) {
                logger.warn("Did not shut down cleanly, should investigate");
            }
	    }

	    @Test
	    public void verifyRunStopsOnSignal()  {

	    	for(String codec : codecs) {
	    		for(String transform : transforms) {

	    			if(codec == "NONE" && transform == "SNAPPY") continue;
	    			verifyRunStopsOnSignal(codec, transform);
	    		}
	    	}	    	
	    }

	    // compress the input using the compression stream class
	    // return the results.
	    private byte[] compress(String codec, String transform, byte[] input) throws IOException {

	    	ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    		CompressedOutputStream compressionStream = new CompressedOutputStream(codec, transform, byteStream);
    		
	    	compressionStream.write(input);
	    	compressionStream.flush();
	    	compressionStream.close();

	    	return byteStream.toByteArray();
	    }

	    private void verify(String codec, String transform) throws IOException {

	    	GraphManager manager = new GraphManager();

    		Pipe input = new Pipe(config);
    		Pipe output = new Pipe(config);

    		Generator generator = new Generator(manager, input, 100);
    		KanziCompressionStage stage = new KanziCompressionStage(manager, input, output, codec, transform);
    		Dumper dumper = new Dumper(manager, output);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
    		service.startup();

    		boolean completed = service.awaitTermination(1, TimeUnit.MINUTES);
    		assertTrue(completed);

			// make sure data traversing RingBuffers didn't mangle anything.
			assertArrayEquals(compress(codec, transform, generator.data()), dumper.data());
	    } 	    

	    @Test
	    public void verify() throws IOException {

	    	for(String codec : codecs) {
	    		for(String transform : transforms) {
	    			
	    			if(codec == "NONE" && transform == "SNAPPY") continue;
	    			verify(codec, transform);
	    		}
	    	}
	    }	    
}
