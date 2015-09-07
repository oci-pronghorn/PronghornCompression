package com.ociweb.pronghorn.components.decompression.KanziDecompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// Kanzi
import kanzi.io.CompressedInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This stage is capable of uncompressing any stream compressed with 
// the KanziCompressionStage (any combination of codec & transform are supported)
public class KanziDecompressionStage extends PronghornStage {

    private Pipe inputBuffer; 
    private CompressedInputStream input;

    private Pipe outputBuffer;
    private RingOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(KanziDecompressionStage.class);


    public KanziDecompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;
    }

    @Override
    public void startup() {
        
        input = new CompressedInputStream(new RingInputStream(inputBuffer));
        output = new RingOutputStream(outputBuffer);        
    }
    
	@Override
	public void run() {
		
		try {
		 //   System.out.println("reading");
            int length = input.read(data);
           // System.out.println("read "+length);
            
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
