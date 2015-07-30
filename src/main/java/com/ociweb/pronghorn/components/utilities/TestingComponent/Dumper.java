package com.ociweb.pronghorn.components.utilities.TestingComponent;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dumper extends PronghornStage {

	private RingBuffer inputBuffer;
	private RingInputStream input;

	private ByteArrayOutputStream output;
	private final byte[] data = new byte[4096];

    private final Logger logger = LoggerFactory.getLogger(Dumper.class);


	public Dumper(GraphManager manager, RingBuffer inputBuffer) {
		super(manager, inputBuffer, NONE);

		this.inputBuffer = inputBuffer;
		this.output = new ByteArrayOutputStream();
	}

	@Override
	public void startup() {
		input = new RingInputStream(inputBuffer);
	}

	@Override
	public void run() {
	    
		int length = input.read(data);
	
		while(length > 0) {
			output.write(data, 0, length);
			length = input.read(data);
		}

		if(length < 0) {		    
			requestShutdown();
		}
	}

	@Override 
	public void shutdown() {
		try {
			output.flush();
			output.close();
		} catch(IOException e) {
		    logger.error("shutdown",e);
			throw new RuntimeException("IOException in shutdown()");
		}

	}

	public byte[] data() {
		return output.toByteArray();
	}
}
