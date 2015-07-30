package com.ociweb.pronghorn.components.compression.SnappyCompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// Snappy
import org.xerial.snappy.SnappyOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnappyCompressionStage extends PronghornStage {

    private RingBuffer inputBuffer; 
    private RingInputStream input;

    private RingBuffer outputBuffer;
    private SnappyOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(SnappyCompressionStage.class);

    public SnappyCompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
        super(manager, inputRing, outputRing);

    	this.inputBuffer = inputRing;
    	this.outputBuffer = outputRing;
    }

    @Override
    public void startup() {
        input = new RingInputStream(inputBuffer);
        output = new SnappyOutputStream(new RingOutputStream(outputBuffer));
  
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
            
		} catch(IOException e) {
		  throw new RuntimeException("IOException in run()");
		}
	}

    @Override
    public void shutdown() {
        try {
            output.flush();
            output.close();
        } catch(IOException e) {
            throw new RuntimeException("IOException in shutdown()");
        }
    }
}
