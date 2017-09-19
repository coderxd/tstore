package com.mxd.store.net.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DataCompress {
	
	public static byte[] compress(byte[] bytes){
		ByteArrayInputStream bais = null; 
		ByteArrayOutputStream baos = null;
		GZIPOutputStream gos = null;
		try {
			bais = new ByteArrayInputStream(bytes);
			baos = new ByteArrayOutputStream();
			gos = new GZIPOutputStream(baos);
			int count;  
		    byte data[] = new byte[1024];  
		    while ((count = bais.read(data, 0, 1024)) != -1) {  
		        gos.write(data, 0, count);  
		    }
		    gos.finish();
		    gos.flush();
		    return baos.toByteArray();
		} catch (IOException e) {
			return new byte[0];
		} finally{
			if(gos!=null){
				try {
					gos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(baos!=null){
				try {
					gos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(bais!=null){
				try {
					bais.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	public static byte[] uncompress(byte[] bytes){
		ByteArrayInputStream bais = null;
		GZIPInputStream gis = null;
		ByteArrayOutputStream baos = null;
		try {
			bais = new ByteArrayInputStream(bytes);
			baos = new ByteArrayOutputStream();
			gis = new GZIPInputStream(bais);
			int count;
			byte buffer[] = new byte[1024];
			while ((count = gis.read(buffer, 0, 1024)) != -1) {
				baos.write(buffer, 0, count);
			}
			bytes = baos.toByteArray();
			return bytes;
		} catch (IOException e1) {
			return new byte[0];
		} finally{
			if(gis!=null){
				try {
					gis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(baos!=null){
				try {
					baos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
