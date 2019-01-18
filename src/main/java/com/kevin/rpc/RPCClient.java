package com.kevin.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * RPC客户端
 */
public class RPCClient {

	public static void main(String[] args) throws IOException {
		Barty proxy = RPC.getProxy(Barty.class, 10010,
				new InetSocketAddress("localhost", 9527), new Configuration());
		String sayHi = proxy.sayHi("tomcat");
		System.out.println(sayHi);
		
	}

}
