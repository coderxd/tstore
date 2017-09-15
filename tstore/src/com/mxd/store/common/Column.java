package com.mxd.store.common;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class Column implements Serializable{
	
	private static final long serialVersionUID = -5664455301299806072L;

	private String name;
	
	private int type;
	
	private int length;
	
	public Column() {
		super();
	}
	public static class Type{
		public static final int INT = 1;
		public static final int SHORT = 2;
		public static final int FLOAT = 3;
		public static final int DOUBLE = 4;
		public static final int LONG =5;
		public static final int STRING = 6;
	}

	public Column(String name, int type) {
		this(name, type, 0);
	}
	
	public Column(String name, int type, int length) {
		super();
		this.name = name;
		this.type = type;
		this.length = length;
		this.setType(this.type);
	}
	public String getName() {
		return name;
	}
	

	public void setName(String name) {
		this.name = name;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
		switch (this.type) {
		case Type.INT:
			this.length = 4;
			break;
		case Type.SHORT:
			this.length = 2;
			break;
		case Type.FLOAT:
			this.length = 4;
			break;
		case Type.DOUBLE:
			this.length = 8;
			break;
		case Type.LONG:
			this.length = 8;
			break;
		case Type.STRING:
			this.length = 4+(length > 0 ? length:256);
			break;
		}
	}

	public int getLength() {
		return length;
	}
	
	public void setLength(int length) {
		this.length = length;
	}

	public ByteBuffer getBuffer(){
		try {
			byte[] nameBytes = this.name.getBytes("ISO-8859-1");
			ByteBuffer buffer = ByteBuffer.allocate(12+nameBytes.length);
			buffer.putInt(nameBytes.length);
			buffer.put(nameBytes);
			buffer.putInt(this.type);
			buffer.putInt(this.length);
			buffer.flip();
			return buffer;
		} catch (UnsupportedEncodingException e) {
			return null;
		}
	}
	@Override
	public String toString() {
		return "Column [name=" + name + ", type=" + type + ", length=" + length + "]";
	}
	
}
