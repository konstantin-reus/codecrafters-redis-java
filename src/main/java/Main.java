import java.io.*;
import java.net.*;

public class Main {
    public static void main(String[] args) {
        int messages = 1;
        int incomingConnections = 0;

        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            serverSocket.setReuseAddress(true);
            handleIncomingConnection(messages, incomingConnections, serverSocket);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void handleIncomingConnection(int messages,
                                                 int incomingConnections,
                                                 ServerSocket serverSocket) {
        try (Socket clientSocket = serverSocket.accept()) {
            System.out.println("Got incoming connection #" + ++incomingConnections);
            InputStream in = clientSocket.getInputStream();
            byte[] bytes = new byte[1024];
            while (in.read(bytes) != -1) {
                System.out.println("Received message #" + messages + ": " + new String(bytes, 0, bytes.length));
                clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                System.out.println("Send PONG #" + messages);
                messages++;
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
}
