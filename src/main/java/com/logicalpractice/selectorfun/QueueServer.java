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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Queue based server.
 *
 * <p>opens port 2002</p>
 *
 */
public class QueueServer {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final int PORT = 2002;

    private final Logger logger = LoggerFactory.getLogger(QueueServer.class);

    private final Selector selector;

    private final BlockingQueue<Message> messages = new ArrayBlockingQueue<Message>(16);

    private volatile boolean stopping;

    private Thread thread;

    private AtomicLong queueNumbers = new AtomicLong(0);

    public QueueServer() {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void go() throws IOException {

        ServerSocketChannel ssc = ServerSocketChannel.open();

        ssc.configureBlocking(false);
        ssc.bind(new InetSocketAddress(PORT));

        SelectionKey serverKey = ssc.register(selector, SelectionKey.OP_ACCEPT);

        logger.info("listening on port " + PORT);
        while (!stopping) {
            selector.select();

            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey selectionKey = it.next();
                it.remove();
                if (selectionKey.isAcceptable()) {
                    SocketChannel newClient = ssc.accept();
                    newClient.configureBlocking(false);
                    ConnectionState connectionState = new ConnectionState(queueNumbers.incrementAndGet());
                    newClient.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, connectionState);
                    logger.info("New client connected - {}", connectionState.id);
                    connectionState.writeQueue.add(ByteBuffer.wrap((String.valueOf(connectionState.id) + "\n").getBytes(UTF8)));
                }
                if (selectionKey.isValid() && selectionKey.isWritable()) {
                    writeAttached(selectionKey);
                }
                if (selectionKey.isValid() && selectionKey.isReadable()) {
                    readAttached(selectionKey);
                }
            }
            Message message;
            while ((message = messages.poll()) != null) {
                boolean handled = false;
                for (SelectionKey selectionKey : selector.keys()) {
                    if (selectionKey != serverKey) {
                        ConnectionState state = (ConnectionState) selectionKey.attachment();
                        if( state.id == message.dest() ){
                            state.writeQueue.add(ByteBuffer.wrap(message.body()));
                            selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_WRITE); // switch on writes
                            handled = true;
                            break;
                        }
                    }
                }
                if( !handled ){
                    logger.info("unknown queue id {} - message dropped", message.dest());
                }
            }
        }
        selector.close();
        ssc.close();
    }

    private void writeAttached(SelectionKey key) throws IOException {
        ConnectionState state = (ConnectionState) key.attachment();

        ByteBuffer buff = state.writeQueue.peek();


        if (buff == null) {
            // not interesting in writing anymore
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            return;
        }

        if (buff.hasRemaining()) {
            try {
                SocketChannel socket = (SocketChannel) key.channel();
                socket.write(buff);
            } catch (IOException e) {
                logger.info("IOException [{}] - disconnecting", e.getMessage());
                key.cancel();
                return;
            }
        }
        if (buff.remaining() == 0) {
            state.writeQueue.remove();
        }
    }

    private void readAttached(SelectionKey key) {
        ConnectionState state = (ConnectionState) key.attachment();

        SocketChannel socket = (SocketChannel) key.channel();

        ByteBuffer readBuffer = state.readBuffer;

        try {
            int read = socket.read(readBuffer);
            if (read == -1) {
                logger.info("EOF - disconnecting");
                key.cancel();
                return;
            }

            if (read == 0) {
                return;
            }

            readBuffer.flip();
            int limit = readBuffer.limit();
            logger.info("Scanning {} bytes", limit);

            ByteBuffer command = ByteBuffer.allocate(limit);

            int endOfLastCommand = 0;
            for (int i = 0; i < limit; i++) {
                byte b = readBuffer.get();
                if (b == '\n') {
                    // end of command
                    command.flip();
                    logger.info("Complete message from client, [{}]", new String(command.array(), Charset.defaultCharset()));
                    command.clear(); // put it back
                    endOfLastCommand = i;
                } else {
                    command.put(b);
                }
            }
            readBuffer.clear(); // have to clear cos we flipped it

            if (endOfLastCommand > 0 && endOfLastCommand != (limit - 1)) {
                readBuffer.position(endOfLastCommand + 1);
                readBuffer.compact();
                readBuffer.position(limit - endOfLastCommand);
            }

        } catch (IOException e) {
            logger.info("IOException [{}] - disconnecting", e.getMessage());
            key.cancel();
        }
    }

    public synchronized void start() {
        if (this.thread != null) {
            throw new IllegalStateException("Already started");
        }

        this.thread = new Thread() {
            @Override
            public void run() {
                try {
                    go();
                } catch (Exception e) {
                    logger.error("QueueServer ending with exception", e);
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

    public void sendMessage(long id, String message) throws InterruptedException {
        byte[] bytes = (message + "\n").getBytes(UTF8);
        messages.put(new Message(id, bytes));
        selector.wakeup();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        QueueServer server = new QueueServer();

        server.start();

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        String line;
        while ((line = br.readLine()) != null) {
            switch (line) {
                case "q":
                    server.stop();
                    System.out.println("Server stopped");
                    return;
                case "s":
                    server.printSummary();
                default:
                    try {
                        String [] parts = line.split("\\s");
                        if( parts.length == 2 ){
                            int id = Integer.parseInt(parts[0]);
                            int count = Integer.parseInt(parts[1]);

                            for (int i = count; i > 0; i--) {
                                server.sendMessage(id, "message " + i);
                            }
                            System.out.println("Sent " + count + " messages to " + id);
                        } else {
                            System.out.println("Input number of {session} {count}");
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number input");
                    }
            }
        }
    }

    private void printSummary() {
        System.out.println("number of clients : " + (selector.keys().size() - 1));
    }

    static final class ConnectionState {
        final long id;
        final Queue<ByteBuffer> writeQueue = new LinkedList<ByteBuffer>();
        final ByteBuffer readBuffer = ByteBuffer.allocate(1024) ;

        ConnectionState(long id) {
            this.id = id;
        }

    }

    static final class Message {
        private final long dest;
        private final byte [] body ;

        Message(long dest, byte[] body) {
            this.dest = dest;
            this.body = body;
        }

        long dest(){ return dest; }
        byte [] body(){ return body ;}
    }
}
