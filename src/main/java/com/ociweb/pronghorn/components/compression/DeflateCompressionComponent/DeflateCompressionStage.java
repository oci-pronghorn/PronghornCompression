package com.ociweb.pronghorn.components.compression.DeflateCompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeflateCompressionStage extends PronghornStage {

    private RingBuffer inputBuffer; 
    private RingInputStream input;

    private RingBuffer outputBuffer;
    private DeflateCompressorOutputStream output;

    private int compressionLevel;
    private byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(DeflateCompressionStage.class);

    public DeflateCompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
    	this(manager, inputRing, outputRing, 1);
    }

	public DeflateCompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing, int compressionLevelIn) {
        
		super(manager, inputRing, outputRing);
		
		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;

		this.compressionLevel = compressionLevelIn;
	}

    @Override
	public void startup() {
        try {
            this.input = new RingInputStream(this.inputBuffer);

            DeflateParameters params = new DeflateParameters();
            params.setCompressionLevel(compressionLevel);

            this.output = new DeflateCompressorOutputStream(new RingOutputStream(this.outputBuffer), params);
            
        } catch(IOException e) {
            throw new RuntimeException("IOException in startup");
        }
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
            throw new RuntimeException("IOException in startup.");
		}
	}

    @Override
	public void shutdown() {

	    try {
		    output.flush();
            output.finish();
            output.close();
		} catch(IOException e) {
		    throw new RuntimeException("IOException in shutdown.");
		}
	}
}
