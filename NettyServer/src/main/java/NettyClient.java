import io.netty.channel.socket.SocketChannel;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

//ifconfig |grep inet

public class NettyClient {
        private static final String SERVER_HOST = "192.168.178.21"; // (1)
        public static final int PORT = 23500;
        public static final boolean KEEPALIVE = true;

        public static void main(String[] args) {
            EventLoopGroup workerGroup = new NioEventLoopGroup(); // (2)
            try {
                Bootstrap bootstrap = new Bootstrap(); // (3)
                bootstrap.group(workerGroup); // (4)
                bootstrap.channel(NioSocketChannel.class); // (5)
                bootstrap.option(ChannelOption.SO_KEEPALIVE, KEEPALIVE); // (6)
                bootstrap.handler(new ChannelInitializer<SocketChannel>() { // (7)


                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        System.out.println("Verbindung zum Server hergestellt!");
                    }
                });
                bootstrap.connect(SERVER_HOST, PORT).sync().channel().closeFuture().sync(); // (8)
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                workerGroup.shutdownGracefully(); // (9)
            }
        }
    }
