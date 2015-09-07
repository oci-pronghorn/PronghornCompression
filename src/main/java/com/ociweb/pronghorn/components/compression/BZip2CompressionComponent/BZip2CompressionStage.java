package com.ociweb.pronghorn.components.compression.BZip2CompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BZip2CompressionStage extends PronghornStage {

    private Pipe inputBuffer;
    private RingInputStream input;
    
    private Pipe outputBuffer;
    private RingOutputStream outputStream;
    private BZip2CompressorOutputStream output;
    
    private byte[] data;
    private int blockSize;

    private final Logger logger = LoggerFactory.getLogger(BZip2CompressionStage.class);


    public BZip2CompressionStage(GraphManager manager, Pipe inputBuffer, Pipe outputBuffer) {
    	this(manager, inputBuffer, outputBuffer, 9);
    }

	public BZip2CompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing, int blockSizeIn) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;

		this.blockSize = blockSizeIn;
	}

    @Override 
    public void startup() {
        super.startup();

        try {

            this.outputStream = new RingOutputStream(outputBuffer);
            this.output = new BZip2CompressorOutputStream(outputStream, blockSize);
            this.input = new RingInputStream(inputBuffer);
            this.data = new byte[4096];

        } catch(IOException e) {
            throw new RuntimeException("IOException in startup.");
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
                shutdown();
            }

        } catch(IOException e) {
            throw new RuntimeException("IOException in run.");
        }
	}

    @Override
    public void shutdown() {

        try {
            output.flush();
            output.finish();
            output.close();

            outputStream.close();
            
        } catch(IOException e) {
            throw new RuntimeException("IOException in shutdown.");
        }

        super.shutdown();
    }
}
