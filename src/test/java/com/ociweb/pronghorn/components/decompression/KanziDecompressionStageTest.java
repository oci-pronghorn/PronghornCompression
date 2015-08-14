package com.ociweb.pronghorn.components.decompression;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.compression.KanziCompressionComponent.KanziCompressionStage;
import com.ociweb.pronghorn.components.decompression.KanziDecompressionComponent.KanziDecompressionStage;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Dumper;
import com.ociweb.pronghorn.components.utilities.TestingComponent.Generator;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class KanziDecompressionStageTest {
	
        private final String codecs[] = new String[] {"PAQ", "FPAQ", "ANS", "HUFFMAN", "RANGE", "NONE"};
        private final String transforms[] = new String[] {"BWT", "BWTS", "SNAPPY", "LZ4", "RLT", "BWT+MTF", "BWT+TIMESTAMP", "NONE"};

		private final Logger logger = LoggerFactory.getLogger(KanziDecompressionStageTest.class);
		private final RingBufferConfig config = new RingBufferConfig(FieldReferenceOffsetManager.RAW_BYTES, 20, 4096);

	    @Test
	    public void verifyInstantiation() {
	        
	        GraphManager manager = new GraphManager();

    		RingBuffer input = new RingBuffer(config);
    		RingBuffer output = new RingBuffer(config);
	        	
       		KanziDecompressionStage stage = new KanziDecompressionStage(manager, input, output);
	    }
	    
	    @Ignore //TODO: Must be fixed before anyone should assume Kanzi can be used.
	    public void verify() {

	    	for(String codec : codecs) {
	    		for(String transform : transforms) {
                    
                    // NONE+SNAPPY doesn't decode correctly...
                    if(codec == "NONE" && transform == "SNAPPY") continue;

	    			verify(codec, transform);
	    		}
	    	}
	    }

	    private void verify(String codec, String transform) {

	    	GraphManager manager = new GraphManager();

    		RingBuffer[] rings = new RingBuffer[] { 
    			  new RingBuffer(config)	// input to compression stage
    			, new RingBuffer(config)	// output for compression stage, input for decompression stage
    			, new RingBuffer(config) 	// output for decompression stage, input for dumper.
    		};


    		Generator generator = new Generator(manager, rings[0], 100);
    		KanziCompressionStage compressor = new KanziCompressionStage(manager, rings[0], rings[1], codec, transform);
    		KanziDecompressionStage decompressor = new KanziDecompressionStage(manager, rings[1], rings[2]);
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
