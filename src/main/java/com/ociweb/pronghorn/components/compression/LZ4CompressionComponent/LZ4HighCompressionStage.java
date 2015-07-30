package com.ociweb.pronghorn.components.compression.LZ4CompressionComponent;

import java.io.IOException;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.ring.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// LZ4
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LZ4HighCompressionStage extends PronghornStage {

    private RingBuffer inputBuffer;
    private RingInputStream input;

    private RingBuffer outputBuffer;
    private LZ4BlockOutputStream output;
    private final byte[] data = new byte[4096];

    private int blockSize;
    private LZ4Compressor compressor;

    private final Logger logger = LoggerFactory.getLogger(LZ4HighCompressionStage.class);


    public LZ4HighCompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing) {
        this(manager, inputRing, outputRing, (int) 1 << 16, LZ4Factory.fastestInstance().highCompressor());
    }

    // WARNING: not all block sizes produce decompressable streams.
	public LZ4HighCompressionStage(GraphManager manager, RingBuffer inputRing, RingBuffer outputRing, int blockSize, LZ4Compressor compressor) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;

		this.blockSize = blockSize;
        this.compressor = compressor;
	}


    @Override
    public void startup() {
        input = new RingInputStream(inputBuffer);
        output = new LZ4BlockOutputStream(new RingOutputStream(outputBuffer), blockSize, compressor);
      
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
            throw new RuntimeException("RuntimeException in run()");
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
