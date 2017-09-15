package com.mxd.store.net.server.handler;
import java.nio.channels.SocketChannel;
import com.mxd.store.net.common.StoreMessage;
import static com.mxd.store.net.common.ResponseCode.*;

public class DefaultStoreMessageHandler extends ServerStoreMessageHandler{

	@Override
	public void onMessageReceived(SocketChannel channel, StoreMessage message) throws Exception {
		super.write(channel, new StoreMessage(ERROR_UNKNOW_COMMAND));
	}

}
