package com.ociweb.pronghorn.components.decompression.BZip2DecompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BZip2DecompressionStage extends PronghornStage {

    private Pipe inputBuffer; 
    private BZip2CompressorInputStream input;

    private Pipe outputBuffer;
    private RingOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(BZip2DecompressionStage.class);

    public BZip2DecompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;
    }

    @Override 
    public void startup() {
        super.startup();
        
        try {
            input = new BZip2CompressorInputStream(new RingInputStream(inputBuffer));
            output = new RingOutputStream(outputBuffer);
        } catch(IOException e) {
            throw new RuntimeException("IOException in startup()");
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
                //Position pill is still on the ring buffer and must be removed.
                if (Pipe.contentRemaining(inputBuffer)==Pipe.EOF_SIZE) {
                    int eofMsg = Pipe.takeMsgIdx(inputBuffer);
                    int eofLen = Pipe.takeValue(inputBuffer);
                    Pipe.releaseReads(inputBuffer);
                }
                shutdown();
            }
		}
		catch(IOException e) {
            throw new RuntimeException("IOException in run()");
		}
	}

	@Override
	public void shutdown() {
        output.close();
        super.shutdown();
    }

}
