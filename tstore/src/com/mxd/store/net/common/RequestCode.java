package com.mxd.store.net.common;

public class RequestCode {
	
	/**
	 * 创建Store
	 */
	public static final int REQUEST_CREATE = 0x101;
	
	/**
	 * 删除Store
	 */
	public static final int REQUEST_DELETE = 0x102;
	
	
	/**
	 * 从Store中获取数据
	 */
	public static final int REQUEST_GET = 0x103;
	
	/**
	 * 从Store中获取数据数量
	 */
	public static final int REQUEST_GET_COUNT = 0x104;
	
	/**
	 * 往Store中插入数据
	 */
	public static final int REQUEST_PUT = 0x105;
	
	/**
	 * 往Store中插入数据（批量）
	 */
	public static final int REQUEST_PUTS = 0x106;
	
	
}
