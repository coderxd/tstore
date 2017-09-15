package com.mxd.store.net.common;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class StoreMessage implements Serializable{
	
	private static final long serialVersionUID = -8014623096131067967L;

	private int requestCode;
	
	private byte[] data;
	
	public StoreMessage(int requestCode, byte[] data) {
		super();
		this.requestCode = requestCode;
		this.data = data;
		if(data==null){
			this.data = new byte[0];
		}
	}
	
	public StoreMessage(int requestCode, String json) throws IOException{
		super();
		this.requestCode = requestCode;
		if(json==null){
			this.data = new byte[0];
		}else{
			this.data = json.getBytes("ISO-8859-1");
		}
	}
	
	public StoreMessage(int opCode) {
		this(opCode,new byte[0]);
	}
	public int getRequestCode() {
		return requestCode;
	}
	public byte[] getData() {
		return data;
	}
	
	public ByteBuffer getBuffer(){
		ByteBuffer buffer = ByteBuffer.allocate(8+this.data.length);
		buffer.putInt(4+this.data.length);
		buffer.putInt(this.requestCode);
		buffer.put(this.data);
		buffer.flip();
		return buffer;
		
	}
	@Override
	public String toString() {
		return "StoreMessage [requestCode=" + requestCode +", data=" + Arrays.toString(data) + "]";
	}
}
