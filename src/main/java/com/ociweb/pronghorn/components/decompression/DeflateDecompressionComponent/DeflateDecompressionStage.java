package com.ociweb.pronghorn.components.decompression.DeflateDecompressionComponent;

import java.io.IOException;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class DeflateDecompressionStage extends PronghornStage {

    private RingBuffer inputBuffer; 
    private DeflateCompressorInputStream input;

    private RingBuffer outputBuffer;
    private RingOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(DeflateDecompressionStage.class);

    public DeflateDecompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
		super(manager, inputRing, outputRing);

        this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;
    }

    @Override 
    public void startup() {
        input = new DeflateCompressorInputStream(new RingInputStream(inputBuffer));
        output = new RingOutputStream(outputBuffer);
     
    }

	@Override
	public void run() {
		
		try {
            int length = input.read(data);
            
            while(length > 0) {
                output.write(data, 0, length);
                length = input.read(data);
            }

            if(length < 0) {
                requestShutdown();
            }
		}
		catch(IOException e) {
			throw new RuntimeException("IOException in run()");
		}
	}

	@Override
	public void shutdown() {
        output.close();
	}
}
