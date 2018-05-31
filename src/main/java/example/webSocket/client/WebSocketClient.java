package example.webSocket.client;

import java.net.URI;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public final class WebSocketClient {

	static final String URL = System.getProperty("url", "ws://127.0.0.1:7397/websocket");

	public static void main(String[] args) {
		send("111111111");
	}

	public static void send(String massage) {
		try {
			URI uri = new URI(URL);
			String scheme = uri.getScheme() == null ? "ws" : uri.getScheme();
			final String host = uri.getHost() == null ? "127.0.0.1" : uri.getHost();
			final int port;
			if (uri.getPort() == -1) {
				if ("ws".equalsIgnoreCase(scheme)) {
					port = 80;
				} else if ("wss".equalsIgnoreCase(scheme)) {
					port = 443;
				} else {
					port = -1;
				}
			} else {
				port = uri.getPort();
			}

			if (!"ws".equalsIgnoreCase(scheme) && !"wss".equalsIgnoreCase(scheme)) {
				System.err.println("Only WS(S) is supported.");
				return;
			}

			final boolean ssl = "wss".equalsIgnoreCase(scheme);
			final SslContext sslCtx;
			if (ssl) {
				sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
			} else {
				sslCtx = null;
			}

			EventLoopGroup group = new NioEventLoopGroup();
			try {
				final WebSocketClientHandler handler = new WebSocketClientHandler(WebSocketClientHandshakerFactory
						.newHandshaker(uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders()));

				Bootstrap b = new Bootstrap();
				b.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) {
						ChannelPipeline p = ch.pipeline();
						if (sslCtx != null) {
							p.addLast(sslCtx.newHandler(ch.alloc(), host, port));
						}
						p.addLast(new HttpClientCodec(), new HttpObjectAggregator(8192), handler);
					}
				});

				Channel ch = b.connect(uri.getHost(), port).sync().channel();
				handler.handshakeFuture().sync();

				if (massage == null) {
				} else if ("bye".equals(massage.toLowerCase())) {
					ch.writeAndFlush(new CloseWebSocketFrame());
					ch.closeFuture().sync();
				} else if ("ping".equals(massage.toLowerCase())) {
					WebSocketFrame frame = new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] { 8, 1, 8, 1 }));
					ch.writeAndFlush(frame);
				} else {
					WebSocketFrame frame = new TextWebSocketFrame(massage);
					ch.writeAndFlush(frame);
				}
			} finally {
				group.shutdownGracefully();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
