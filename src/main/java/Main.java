import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class Main {
    public static void main(String[] args) throws IOException {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(cpuCores);

        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> {
                    try {
                        handleIncomingConnection(clientSocket);
                    } catch (IOException e) {
                    }
                });
            }
        }
    }

    private static void handleIncomingConnection(Socket clientSocket) throws IOException {
        try {
            try (InputStream in = clientSocket.getInputStream()) {
                byte[] bytes = new byte[1024];
                String response;
                while (in.read(bytes) != -1) {
                    String receivedMessage = parseString(bytes);
                    if (receivedMessage.startsWith("*")) {
                        response = handleBulkCommand(receivedMessage);
                    } else {
                        response = handleSingleCommand(receivedMessage.substring(0, receivedMessage.indexOf('\r' )));
                    }
                    clientSocket.getOutputStream().write((response + "\r\n").getBytes());
                }
            }
        } finally {
            clientSocket.close();
        }
    }

    private static String handleSingleCommand(String receivedMessage) {
        System.out.println("[" + Thread.currentThread() + "] Received message: " + receivedMessage);
        if ("PING".equalsIgnoreCase(receivedMessage)) {
            return handlePing();
        } else {
            String response = "Unknown command: " + receivedMessage;
            System.out.println("[" + Thread.currentThread() + "] " + response);
            return response;
        }
    }

    private static String handleBulkCommand(String receivedMessage) {
        List<String> args = parseBulkMessageArguments(receivedMessage);
        return handleCommand(args);
    }

    private static String handleCommand(List<String> args) {
        String command = args.get(0);
        if ("ECHO".equalsIgnoreCase(command)) {
            return handleEcho(args.get(1));
        } else if ("PING".equalsIgnoreCase(command)) {
            return handlePing();
        } else {
            return handleUnknownCommand(command);
        }
    }

    private static String handleUnknownCommand(String command) {
        String response = "Unknown command: " + command;
        System.out.println("[" + Thread.currentThread() + "] " + response);
        return response;
    }

    private static String handlePing() {
        System.out.println("[" + Thread.currentThread() + "] Return +PONG");
        return "+PONG";
    }

    private static String handleEcho(String output) {
        System.out.println("[" + Thread.currentThread() + "] Return +" + output);
        return "+" + output;
    }

    private static String parseString(byte[] bytes) {
        return new String(bytes, 0, bytes.length);
    }

    private static List<String> parseBulkMessageArguments(String receivedMessage) {
        List<String> args = Arrays.stream(receivedMessage.split("\r\n"))
                .filter(arg -> !arg.trim().isEmpty() && !arg.startsWith("*") && !arg.startsWith("$")).collect(Collectors.toList());
        System.out.println("[" + Thread.currentThread() + "] Received bulk args: " + args);
        return args;
    }
}

