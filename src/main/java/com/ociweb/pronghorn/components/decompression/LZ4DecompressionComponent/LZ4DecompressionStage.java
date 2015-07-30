package com.ociweb.pronghorn.components.decompression.LZ4DecompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// LZ4
import net.jpountz.lz4.LZ4BlockInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LZ4DecompressionStage extends PronghornStage {

    private RingBuffer inputBuffer; 
    private LZ4BlockInputStream input;

    private RingBuffer outputBuffer;
    private RingOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(LZ4DecompressionStage.class);

    public LZ4DecompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;
    }

    @Override
    public void startup() {
        input = new LZ4BlockInputStream(new RingInputStream(inputBuffer));
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
                //Position pill is still on the ring buffer and must be removed.
                if (RingBuffer.contentRemaining(inputBuffer)==RingBuffer.EOF_SIZE) {
                    int eofMsg = RingBuffer.takeMsgIdx(inputBuffer);
                    int eofLen = RingBuffer.takeValue(inputBuffer);
                    RingBuffer.releaseReads(inputBuffer);
                }
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
