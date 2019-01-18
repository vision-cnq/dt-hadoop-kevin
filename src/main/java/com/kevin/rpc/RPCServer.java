package com.kevin.rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

/**
 * RPC服务端
 */
public class RPCServer implements Barty{
	
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Server server = new RPC.Builder(new Configuration())
			.setInstance(new RPCServer())
			.setBindAddress("localhost")
			.setPort(9527)
			.setProtocol(Barty.class)
			.build();
		server.start();

	}

	@Override
	public String sayHi(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
