package com.ociweb.pronghorn.components.compression.LZ4CompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// LZ4
import net.jpountz.lz4.LZ4BlockOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LZ4CompressionStage extends PronghornStage {

    private RingBuffer inputBuffer;
    private RingInputStream input;

    private RingBuffer outputBuffer;
    private LZ4BlockOutputStream output;

    private int blockSize;
    private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(LZ4CompressionStage.class);


    public LZ4CompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
        this(manager, inputRing, outputRing, (int)1 << 16);
    }

    // WARNING: not all block sizes produce decompressable streams.
	public LZ4CompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing, int blockSize) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;

		this.blockSize = blockSize;
	}

    @Override
    public void startup() {
        input = new RingInputStream(inputBuffer);
        output = new LZ4BlockOutputStream(new RingOutputStream(outputBuffer), blockSize);

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
        try {

            output.flush();
            output.finish();
            output.close();

        } catch(IOException e) {
            throw new RuntimeException("IOException in shutdown()");
        }

    }
}
