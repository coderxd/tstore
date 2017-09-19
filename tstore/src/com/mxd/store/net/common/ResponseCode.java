package com.mxd.store.net.common;

public class ResponseCode {

	/**
	 * 系统错误
	 */
	public static final int ERROR_SYSTEM = 0x00;
	
	
	/**
	 * 操作成功
	 */
	public static final int RESPONSE_SUCCESS = 0x01;
	
	/**
	 * 未知命令
	 */
	public static final int ERROR_UNKNOW_COMMAND = 0x02;
	
	/**
	 * Store列表
	 */
	public static final int RESPONSE_STORE_LIST = 0x100;
	
	
	/**
	 * 创建Store失败,已存在
	 */
	public static final int ERROR_CREATE_STORE_EXISTS = 0x101;
	
	/**
	 * 命令解析失败
	 */
	public static final int RESPONSE_WRONG_FORMAT = 0x102;
	
	
	/**
	 * 从Store中获取数据
	 */
	public static final int RESPONSE_GET = 0x103;
	
	/**
	 * 从Store中获取数据数量
	 */
	public static final int RESPONSE_GET_COUNT = 0x104;
	
	/**
	 * Store不存在
	 */
	public static final int RESPONSE_STORE_NOTFOUND = 0x105;
	
	/**
	 * 往Store中插入数据（批量）
	 */
	public static final int RESPONSE_PUTS = 0x106;
}
