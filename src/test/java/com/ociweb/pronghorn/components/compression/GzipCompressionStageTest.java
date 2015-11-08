package com.ociweb.pronghorn.components.compression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.GzipCompressionComponent.GzipCompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.stream.RingStreams;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;
// Apache Commons Compression 

public class GzipCompressionStageTest {
		
		private final Logger logger = LoggerFactory.getLogger(GzipCompressionStageTest.class);
		private final PipeConfig config = new PipeConfig(RawDataSchema.instance);

	    @Test
	    public void verifyInstantiation() {
	        
	    	GraphManager manager = new GraphManager();

    		Pipe input = new Pipe(config);
    		Pipe output = new Pipe(config);
	        	
       		GzipCompressionStage stage = new GzipCompressionStage(manager, input, output);
	    }


	    @Test
	    public void verifyRunStopsOnSignal()  {
	    	GraphManager manager = new GraphManager();

    		Pipe input = new Pipe(config);
    		Pipe output = new Pipe(config);
	       
       		GzipCompressionStage stage = new GzipCompressionStage(manager, input, output);
       		Dumper dumper = new Dumper(manager, output);

    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
			service.startup();

        	RingStreams.writeEOF(input);

			boolean completed = service.awaitTermination(15, TimeUnit.SECONDS);
			assertTrue(completed);
	    }

        private byte[] decompress(byte[] compressedBytes) throws IOException {
            
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            GzipCompressorInputStream input = new GzipCompressorInputStream(new ByteArrayInputStream(compressedBytes));
            byte[] data = new byte[4096];

            int length = input.read(data);
            while(length > 0) {

                output.write(data, 0, length);
                length = input.read(data);
            }

            return output.toByteArray();
        }

	    @Test
	    public void verifyContentsOfCompressionGoingThroughRingBufferMatchDirectlyCompressedOutput() throws IOException {
	    	try {
	    		GraphManager manager = new GraphManager();

	    		Pipe input = new Pipe(config);
	    		Pipe output = new Pipe(config);
	    		
	    		Generator generator = new Generator(manager, input, 100);
	    		GzipCompressionStage stage = new GzipCompressionStage(manager, input, output);
	    		Dumper dumper = new Dumper(manager, output);

	    		ThreadPerStageScheduler service = new ThreadPerStageScheduler(manager);
	    		service.startup();

	    		boolean completed = service.awaitTermination(1, TimeUnit.MINUTES);
	    		assertTrue(completed);

				// make sure data traversing RingBuffers didn't mangle anything.
				assertArrayEquals(generator.data(), decompress(dumper.data()));
	    	}
	    	catch(IOException e) {
	    		logger.error("", e);
	    	}
	    }
}
