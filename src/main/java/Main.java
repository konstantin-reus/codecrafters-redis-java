import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws IOException {
        int messages = 0;
        int cpuCores = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(cpuCores);

        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> {
                    try {
                        handleIncomingConnection(messages, clientSocket);
                    } catch (IOException e) {
                    }
                });
            }
        }
    }

    private static void handleIncomingConnection(int messages,
                                                 Socket clientSocket) throws IOException {
        InputStream in = clientSocket.getInputStream();
        byte[] bytes = new byte[1024];
        while (in.read(bytes) != -1) {
            System.out.println("[" + Thread.currentThread() + "] Received message #" + messages + ": " + new String(bytes, 0, bytes.length));
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            System.out.println("[" + Thread.currentThread() + "] Send PONG #" + messages);
            messages++;
        }
        clientSocket.close();
    }
}
