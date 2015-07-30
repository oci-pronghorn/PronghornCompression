package com.ociweb.pronghorn.components.compression.Pack200CompressionComponent;

import java.io.IOException;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.pack200.Pack200CompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class Pack200CompressionStage extends PronghornStage {

    private RingBuffer inputBuffer;
    private RingInputStream input;

    private RingBuffer outputBuffer;
    private Pack200CompressorOutputStream output;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(Pack200CompressionStage.class);
    
	public Pack200CompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
		super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;
	}

	@Override
	public void startup() {
		try {
			input = new RingInputStream(inputBuffer);
			output = new Pack200CompressorOutputStream(new RingOutputStream(outputBuffer));
		} catch(IOException e) {
			throw new RuntimeException("IOException in startup()");
		}

        super.startup();		
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
			throw new RuntimeException("IOException in run()");
		}
		
	}

	@Override
	public void shutdown() {
		try {
			output.flush();
			output.finish();
			output.close();
		} catch(IOException e) {
			throw new RuntimeException("IOException in shutdown()");
		}

		super.shutdown();
	}
}
