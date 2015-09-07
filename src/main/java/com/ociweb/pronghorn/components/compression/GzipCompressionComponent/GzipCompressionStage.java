package com.ociweb.pronghorn.components.compression.GzipCompressionComponent;

import java.io.IOException;

// Apache Commons Compression 
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingInputStream;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class GzipCompressionStage extends PronghornStage {

    private Pipe inputBuffer; 
    private RingInputStream input;

    private Pipe outputBuffer;
    private GzipCompressorOutputStream output;
    
    private byte[] data = new byte[4096];
    private int compressionLevel;

	private final Logger logger = LoggerFactory.getLogger(GzipCompressionStage.class);
    
	public GzipCompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing) {
		this(manager, inputRing, outputRing, 0);
	}

	public GzipCompressionStage(GraphManager manager, Pipe inputRing, Pipe outputRing, int compressionLevel) {
		super(manager, inputRing, outputRing);

		this.inputBuffer = inputRing;
		this.outputBuffer = outputRing;

		this.compressionLevel = compressionLevel;
	}

	@Override
	public void startup() {
		try {
			input = new RingInputStream(inputBuffer);

			GzipParameters params = new GzipParameters();
			params.setCompressionLevel(compressionLevel);

			output = new GzipCompressorOutputStream(new RingOutputStream(outputBuffer), params);

		} catch(IOException e) {
			throw new RuntimeException("IOException in startup");
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
