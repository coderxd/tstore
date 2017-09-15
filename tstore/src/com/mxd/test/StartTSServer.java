package com.mxd.test;

import com.mxd.store.net.server.TSServer;

public class StartTSServer {
	public static void main(String[] args) {
		new TSServer().start(5124);
	}
}
