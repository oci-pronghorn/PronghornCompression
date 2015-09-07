package com.ociweb.pronghorn.components.decompression.XZDecompressionComponent;

import java.io.IOException;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XZDecompressionStage extends PronghornStage {

    private Pipe inputBuffer; 
    private XZCompressorInputStream input;

    private Pipe outputBuffer;
    private RingOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(XZDecompressionStage.class);


    public XZDecompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;
    }

    @Override
    public void startup() {
        
        try {
            input = new XZCompressorInputStream(new RingInputStream(inputBuffer));
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
