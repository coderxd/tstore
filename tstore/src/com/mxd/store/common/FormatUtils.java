package com.mxd.store.common;

public class FormatUtils {
	
	public static Long parseLong(Object value,Long defaultValue){
		try {
			return Long.parseLong(value.toString());
		} catch (Exception e) {
			return defaultValue;
		}
	}
	public static Short parseShort(Object value,Short defaultValue){
		try {
			return Short.parseShort(value.toString());
		} catch (Exception e) {
			return defaultValue;
		}
	}
	public static Double parseDouble(Object value,Double defaultValue){
		try {
			return Double.parseDouble(value.toString());
		} catch (Exception e) {
			return defaultValue;
		}
	}
	public static Float parseFloat(Object value,Float defaultValue){
		try {
			return Float.parseFloat(value.toString());
		} catch (Exception e) {
			return defaultValue;
		}
	}
	public static Integer parseInt(Object value,Integer defaultValue){
		try {
			return Integer.parseInt(value.toString());
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public static String parseString(Object value,String defaultValue){
		try {
			return value.toString();
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
