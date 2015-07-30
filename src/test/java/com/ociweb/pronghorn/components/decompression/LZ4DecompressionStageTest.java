package com.ociweb.pronghorn.components.decompression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.LZ4CompressionComponent.LZ4CompressionStage;
import com.ociweb.pronghorn.components.decompression.LZ4DecompressionComponent.LZ4DecompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class LZ4DecompressionStageTest {
		
		private final Logger logger = LoggerFactory.getLogger(LZ4DecompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        	
       		LZ4DecompressionStage stage = new LZ4DecompressionStage(manager, input, output);
	    }

	    // Create some test data. 
	    private byte[][] generateTestData() {

			byte [] data1 = new byte[4096];
	    	byte [] data2 = new byte[245];
	    	byte [] data3 = new byte[100];

	    	for(int i = 0; i < data1.length; i++) {
	    		data1[i] = (byte)(Math.random() * 255 + 1);
	    	}

	    	for(int i = 0; i < data2.length; i++) {
	    		data2[i] = (byte)(Math.random() * 255 + 1);
	    	}

	    	for(int i = 0; i < data3.length; i++) {
	    		data3[i] = (byte)(Math.random() * 255 + 1);
	    	}

	    	byte[][] data = new byte[][]{data1, data2, data3};
	    	return data;
	    }

	    private void publishInputData(OutputStream output, byte[][] testValues) {
	    	try {

				for(int i = 0; i < testValues.length; i++) {
	    			output.write(testValues[i]);
	    		}

	    		output.flush();
	    		output.close();
	    	}
	    	catch(IOException e) {
	    		logger.error("publishInputData: ", e);
	    	}
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
    		LZ4CompressionStage compressor = new LZ4CompressionStage(manager, rings[0], rings[1], 1 << 16);
    		LZ4DecompressionStage decompressor = new LZ4DecompressionStage(manager, rings[1], rings[2]);
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
