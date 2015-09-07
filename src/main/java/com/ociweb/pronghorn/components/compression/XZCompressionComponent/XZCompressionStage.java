package com.ociweb.pronghorn.components.compression.XZCompressionComponent;

import java.io.IOException;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class XZCompressionStage extends PronghornStage {

    private Pipe inputBuffer; 
    private RingInputStream input;

    private Pipe outputBuffer;
    private XZCompressorOutputStream output;

    private final byte[] data = new byte[4096];
    private int compressionLevel;

    private final Logger logger = LoggerFactory.getLogger(XZCompressionStage.class);


    public XZCompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing) {
        this(manager, inputRing, outputRing, 0);
    }

    /* 
		The compressionLevel 0-3 are fast presets with medium compression. 
		The compressionLevel 4-6 are fairly slow presets with high compression. 
		The default compressionLevel is 6.

		The compressionLevel 7-9 are like the preset 6 but use bigger 
		dictionaries and have higher compressor and decompressor memory requirements. 
		Unless the uncompressed size of the file exceeds 8 MiB, 16 MiB, or 32 MiB, it is waste of memory to use the presets 7, 8, or 9, respectively.
     */
	public XZCompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing, int compressionLevelIn) {
        super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;

		this.compressionLevel = compressionLevelIn;
	}

    @Override 
    public void startup() {
        try {

            input = new RingInputStream(inputBuffer);
            output = new XZCompressorOutputStream(new RingOutputStream(outputBuffer), compressionLevel);

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
