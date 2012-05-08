package com.logicalpractice.selectorfun;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Simple Broadcast server.
 *
 * <p>Opens a ServerSocket on 2001.</p>
 *
 * <ol>
 * <li>And client that attaches will be sent a copy of the messages generated from the console.</li>
 * <li>Any input from the clients will be read and printed to the console line by line</li>
 * </ol>
 *
 * <p>The internal state of the connection is maintained with via an instance of ConnectionState attached to
 * each SelectionKey. Messages to broadcast are postted to the BlockingQueue and the Selector is woken up</p>
 */
public class BroadcastServer {

   private static final Charset UTF8 = Charset.forName("UTF-8");

   private final Logger logger = LoggerFactory.getLogger(BroadcastServer.class);

   private final Selector selector;

   private final BlockingQueue<byte[]> messages = new ArrayBlockingQueue<byte[]>(16);

   private volatile boolean stopping ;

   private Thread thread;

   public BroadcastServer() {
      try {
         selector = Selector.open();
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   private void go() throws IOException {

      ServerSocketChannel ssc = ServerSocketChannel.open();

      ssc.configureBlocking(false);
      ssc.bind(new InetSocketAddress(2001));

      SelectionKey serverKey = ssc.register(selector, SelectionKey.OP_ACCEPT );

      logger.info("listening on port 2001");
      while(!stopping){
         selector.select();

         Iterator<SelectionKey> it = selector.selectedKeys().iterator();
         while(it.hasNext()){
            SelectionKey selectionKey = it.next();
            it.remove();
            if( selectionKey.isAcceptable() ){
               SocketChannel newClient = ssc.accept();
               newClient.configureBlocking(false);
               newClient.register(selector, SelectionKey.OP_READ, new ConnectionState());
               logger.info("New client connected");
            }
            if( selectionKey.isValid() && selectionKey.isWritable() ){
//                    logger.info("Writable()");
               writeAttached(selectionKey);
            }
            if( selectionKey.isValid() && selectionKey.isReadable() ){
               readAttached(selectionKey);
            }
         }
         byte[] message ;
         while( (message = messages.poll()) != null) {
            for (SelectionKey selectionKey : selector.keys()) {
               if( selectionKey != serverKey ){
                  ConnectionState state = (ConnectionState) selectionKey.attachment();
                  state.writeQueue.add(ByteBuffer.wrap(message));
                  selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE); // switch on writes
               }
            }
         }
      }
      selector.close();
      ssc.close();
   }

    private void writeAttached(SelectionKey key) throws IOException {
        ConnectionState state = (ConnectionState) key.attachment();

        ByteBuffer buff = state.writeQueue.peek();


        if( buff == null ){
            // not interesting in writing anymore
            key.interestOps(key.interestOps() & ~ SelectionKey.OP_WRITE);
            return ;
        }

        if ( buff.hasRemaining() ){
            try {
                SocketChannel socket = (SocketChannel) key.channel();
                socket.write(buff);
            } catch (IOException e) {
                logger.info("IOException [{}] - disconnecting", e.getMessage());
                key.cancel();
                return ;
            }
        }
        if( buff.remaining() == 0 ){
            state.writeQueue.remove();
        }
    }

   private void readAttached(SelectionKey key) {
      ConnectionState state = (ConnectionState) key.attachment();

      SocketChannel socket = (SocketChannel) key.channel();

      ByteBuffer readBuffer = state.readBuffer;

      try {
         int read = socket.read(readBuffer);
         if( read == -1 ){
            logger.info("EOF - disconnecting");
            key.cancel();
            return ;
         }

         if( read == 0 ){
            return;
         }

         readBuffer.flip();
         int limit = readBuffer.limit();
         logger.info("Scanning {} bytes", limit);

         ByteBuffer command = ByteBuffer.allocate(limit);

         int endOfLastCommand = 0;
         for( int i = 0; i < limit; i ++ ){
            byte b = readBuffer.get();
            if( b == '\n' ){
               // end of command
               command.flip();
               logger.info("Complete message from client, [{}]", new String(command.array(),Charset.defaultCharset()));
               command.clear(); // put it back
               endOfLastCommand = i;
            } else {
               command.put(b);
            }
         }
         readBuffer.clear(); // have to clear cos we flipped it

         if( endOfLastCommand > 0 && endOfLastCommand != (limit - 1) ) {
            readBuffer.position(endOfLastCommand + 1);
            readBuffer.compact();
            readBuffer.position(limit - endOfLastCommand);
         }

      } catch (IOException e) {
         logger.info("IOException [{}] - disconnecting", e.getMessage());
         key.cancel();
      }
   }

    public synchronized void start(){
        if( this.thread != null ){
            throw new IllegalStateException("Already started");
        }

        this.thread = new Thread(){
            @Override
            public void run() {
                try {
                    go();
                } catch (Exception e) {
                    logger.error("BroadcastServer ending with exception", e);
                }
            }
        };
        thread.start();
    }

    public synchronized void stop() throws InterruptedException {
        stopping = true;
        selector.wakeup();
        this.thread.join();
    }

    public void sendMessage(String message) throws InterruptedException {
        byte [] bytes = (message + "\n").getBytes(UTF8);
        messages.put(bytes);
        selector.wakeup();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BroadcastServer server = new BroadcastServer();

        server.start();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while( ( line = br.readLine() ) != null ){
            switch(line){
                case "q":
                    server.stop();
                    System.out.println("Server stopped");
                    return;
                case "s":
                    server.printSummary();
                default:
                    try {
                        int count = Integer.parseInt(line);
                        for( int i = count; i > 0; i-- ){
                            server.sendMessage("message " + i);
                        }
                        System.out.println("Sent "+ count + " messages");
                    } catch (NumberFormatException e) {
                        System.out.println("Input number of messages");
                    }
            }
        }
    }

    private void printSummary() {
        System.out.println("number of clients : " + (selector.keys().size() - 1));
    }

    static final class ConnectionState {
        final Queue<ByteBuffer> writeQueue = new LinkedList<ByteBuffer>();
        ByteBuffer readBuffer = ByteBuffer.allocate(1024) ;
    }

}
