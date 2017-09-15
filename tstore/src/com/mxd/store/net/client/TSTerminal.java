package com.mxd.store.net.client;

import static com.mxd.store.net.common.RequestCode.*;
import static com.mxd.store.net.common.ResponseCode.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mxd.store.net.common.StoreMessage;
import com.mxd.store.net.common.StoreMessageDecoder;

public class TSTerminal {
	
	public static void main(String[] args) throws IOException {
		TSClient client = getTSClient(args);
		client.connect();
		Scanner scanner = new Scanner(System.in);
		System.out.print("> ");
		String command = scanner.nextLine();
		while(!"quit".equals(command)){
			int index = command.indexOf(" ");
			StoreMessage message = null;
			if(index>-1){
				String head = command.substring(0,index);
				String body = command.substring(index+1, command.length());
				message = parseParams(head, body);
			}else{
				message = parseParams(command, null);
			}
			if(message!=null){
				long begin = System.currentTimeMillis();
				try {
					StoreMessage storeMessage = client.send(message);
					printResponseMessage(storeMessage,System.currentTimeMillis()-begin);
				} catch (Exception e) {
					System.out.println("read error:"+e.getMessage());
				}
			}else{
				System.out.println("input params error");
			}
			System.out.print("> ");
			command = scanner.nextLine();
		}
		scanner.close();
		client.close();
	}
	
	private static TSClient getTSClient(String[] args){
		String host = "127.0.0.1";
		int port = 5124;
		int connectTimeout = 5000;
		int readTimeout = 10000;
		if(args!=null&&args.length>0){
			host = args[0].trim();
			if(args.length>1){
				port = Integer.parseInt(args[1].trim());
			}
			if(args.length>2){
				connectTimeout = Integer.parseInt(args[2].trim());
			}
			if(args.length>3){
				readTimeout = Integer.parseInt(args[2].trim());
			}
		}
		return new TSClient(host, port, connectTimeout, readTimeout);
	}
	
	private static void printResponseMessage(StoreMessage message,long readTime){
		switch (message.getRequestCode()) {
		case RESPONSE_SUCCESS:
			System.out.println("ok read cost time:"+readTime);
			break;
		case RESPONSE_CREATE_STORE_EXISTS:
			System.out.println("ERROR("+message.getRequestCode()+"):create store error,the store already exists");
			break;
		case RESPONSE_WRONG_FORMAT:
			System.out.println("ERROR("+message.getRequestCode()+"):wrong format");
			break;
		case RESPONSE_STORE_NOTFOUND:
			System.out.println("ERROR("+message.getRequestCode()+"):store not found");
			break;
		case RESPONSE_GET_COUNT:
			ByteBuffer buffer = ByteBuffer.wrap(message.getData());
			System.out.println("getCount:{count:"+buffer.getLong()+",query cost time:"+buffer.getInt()+",read cost time:"+readTime+"}");
			break;
		case RESPONSE_GET:
			printGetMessage(message,readTime);
			break;
		default:
			System.out.println("WARN("+message.getRequestCode()+"):unknow response message");
			break;
		}
	}
	
	private static void printGetMessage(StoreMessage message,long readTime){
		try {
			List<Map<String,Object>> list = new ArrayList<>();
			long[] result = StoreMessageDecoder.responseGet(message.getData(),list);
			if(result==null){
				System.out.println("parser result error");
			}else{
				boolean printHeader = true;
				int i = 0;
				for (Map<String, Object> map : list) {
					Set<String> keys = map.keySet();
					if(printHeader){
						System.out.print(" ");
						for(String key:keys){
							System.out.print("\t"+key);
						}
						printHeader = false;
						System.out.println();
					}
					System.out.print(++i);
					for (String key : keys) {
						System.out.print("\t"+map.get(key));
					}
					System.out.println();
				}
				System.out.println("get {count:"+result[0]+",query cost time:"+result[1]+",read cost time:"+readTime+"}");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("parser result error");
		}
	}
	private static StoreMessage parseParams(String command,String body){
		if("get".equals(command)){
			return parseGetCommand(body);
		}else if("getcount".equals(command)){
			return parseGetCountCommand(body);
		}else if("put".equals(command)){
			return parsePutCommand(body);
		}else if("create".equals(command)){
			return parseCreateCommand(body);
		}
		return null;
	}
	private static StoreMessage parseGetCommand(String body){
		try {
			String[] arr = body.split(" ");
			if(arr.length!=4){
				System.out.println("missing parameter");
				return null;
			}else{
				String storeName = arr[0].trim();
				long id = Long.parseLong(arr[1].trim());
				long minTimestamp = Long.parseLong(arr[2].trim());
				long maxTimestamp = Long.parseLong(arr[3].trim());
				return new StoreMessage(REQUEST_GET,String.format("[\"%s\",%d,%d,%d]", storeName,id,minTimestamp,maxTimestamp));
			}
		} catch (Exception e) {
			return null;
		}
	}
	private static StoreMessage parseGetCountCommand(String body){
		try {
			String[] arr = body.split(" ");
			if(arr.length!=4){
				System.out.println("missing parameter");
				return null;
			}else{
				String storeName = arr[0].trim();
				long id = Long.parseLong(arr[1].trim());
				long minTimestamp = Long.parseLong(arr[2].trim());
				long maxTimestamp = Long.parseLong(arr[3].trim());
				return new StoreMessage(REQUEST_GET_COUNT,String.format("[\"%s\",%d,%d,%d]", storeName,id,minTimestamp,maxTimestamp));
			}
		} catch (Exception e) {
			return null;
		}
	}
	private static StoreMessage parsePutCommand(String body){
		try {
			return new StoreMessage(REQUEST_PUT,new ObjectMapper().writeValueAsBytes(body.split(" ")));
		} catch (Exception e) {
			return null;
		}
	}
	private static StoreMessage parseCreateCommand(String body){
		try {
			return new StoreMessage(REQUEST_CREATE,body);
		} catch (Exception e) {
			return null;
		}
	}
}
