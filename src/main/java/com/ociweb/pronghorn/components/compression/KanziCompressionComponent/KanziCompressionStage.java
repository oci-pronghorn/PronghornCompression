package com.ociweb.pronghorn.components.compression.KanziCompressionComponent;

import java.io.IOException;
import java.lang.RuntimeException;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

// PAQ
import kanzi.io.CompressedOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The KanziCompressionStage implements several compression compression options.
// The options are primarily controlled through the codec and transform variables.
// The following are valid values for codec: 
//      PAQ, FPAQ, ANS, HUFFMAN, RANGE, NONE
// 
// The following are valid values for transform:
//      BWT, BWTS, SNAPPY, LZ4, RLT, BWT+RANK, BWT+MTF, BWT+TIMESTAMP, NONE
//
// The following combinations cannot be decompressed correctly,
// and will be disallowed: BWT+RANK, NONE+SNAPPY
public class KanziCompressionStage extends PronghornStage {

    private Pipe inputBuffer;
    private RingInputStream input;

    private Pipe outputBuffer;
    private CompressedOutputStream output;
    private final byte[] data = new byte[4096];
    
    private String codec;
    private String transform;

    private final Logger logger = LoggerFactory.getLogger(KanziCompressionStage.class);

    public KanziCompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing, String codec, String transform) {
        super(manager, inputRing, outputRing);

    	this.inputBuffer = inputRing;
    	this.outputBuffer = outputRing;
        this.codec = codec;
        this.transform = transform;

        if(this.transform == "BWT+RANK") {
            throw new RuntimeException("BWT+RANK is not supported.");
        }

        if(this.codec == "NONE" && this.transform == "SNAPPY") {
            throw new RuntimeException("NONE+SNAPPY is not supported.");
        }
    }

    @Override 
    public void startup() {

        input = new RingInputStream(inputBuffer);
        output = new CompressedOutputStream(codec, transform, new RingOutputStream(outputBuffer));
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
            output.close();
        } catch(IOException e) {
            throw new RuntimeException("IOException in shutdown()");
        }
    }
}
