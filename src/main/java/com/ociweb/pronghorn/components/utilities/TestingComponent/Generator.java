package com.ociweb.pronghorn.components.utilities.TestingComponent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.RingOutputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class Generator extends PronghornStage {

	private Pipe outputBuffer;
	private RingOutputStream outputStream; 
	private List<byte[]> bytes = new LinkedList<byte[]>();

	private int arrayCount = 0;
	private final int maxArrayCount;

    private final Logger logger = LoggerFactory.getLogger(Generator.class);
    

	public Generator(GraphManager manager, Pipe outputBuffer, int maxArrayCount) {
		super(manager, NONE, outputBuffer);

		this.outputBuffer = outputBuffer;
		this.maxArrayCount = maxArrayCount;
	}

	@Override
	public void startup() {
		outputStream = new RingOutputStream(outputBuffer);
	}

	@Override
	public void run() {

		if(++arrayCount > maxArrayCount) {
			requestShutdown();
			return;
		}

		byte[] data = createData();

		outputStream.write(data);
		write(data);
	}

	@Override
	public void shutdown() {
		try {
			outputStream.flush();
			outputStream.close();
		} catch(IOException e) {
			throw new RuntimeException("IOException in shutdown()");
		}
		
	}

	// return a copy of the data we generated.
	public byte[] data() {

		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		try {			

			for(byte[] data : bytes) {
				output.write(data);
			}

			output.flush();
			output.close();

		} catch(IOException e) {
			throw new RuntimeException("IOException in data()");
		}

		return output.toByteArray();
	}

	// return a copy of the data in the exact parts
	public List<byte[]> dataAsArray() {
		return bytes;
	}

	private byte[] createData() {

		int arraySize = (int)(Math.random() * 4096 + 1);
		byte[] data = new byte[arraySize];

		for(byte b : data) {
			b = (byte)(Math.random() * 255 + 1);
		}

		return data;
	}

	private void write(byte[] data) {
		bytes.add(data);
	}
}
