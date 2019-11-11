import io.netty.channel.socket.SocketChannel;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

//sudo lsof -i :23500
// kill -9 <PID>

public class NettyServer {
    public static final int PORT = 23500;
    public static final boolean KEEPALIVE = true;

        public static void main(String[] args) {
            EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
            EventLoopGroup workerGroup = new NioEventLoopGroup(); // (1)
            try {
                ServerBootstrap bootstrap = new ServerBootstrap(); // (2)
                bootstrap.group(bossGroup, workerGroup); // (3)
                bootstrap.channel(NioServerSocketChannel.class); // (4)
                bootstrap.childHandler(new ChannelInitializer<SocketChannel>() { // 5


                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        System.out.println("Ein Computer hat sich verbunden. IP: " + channel.remoteAddress()); // (6)
                    }
                });
                bootstrap.option(ChannelOption.SO_BACKLOG, 50); // (7)
                bootstrap.childOption(ChannelOption.SO_KEEPALIVE, KEEPALIVE); // (8)
                ChannelFuture future = bootstrap.bind(PORT).sync(); // (9)
                System.out.println("Server gestartet!");
                future.channel().closeFuture().sync(); // (10)
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                bossGroup.shutdownGracefully(); // (11)
                workerGroup.shutdownGracefully(); // (11)
            }
        }
    }

