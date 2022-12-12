import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public class Main {
    private static final Map<String, String> storage = new ConcurrentHashMap<>();

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
                    List<String> args = parseArguments(receivedMessage);
                    response = handleCommand(args);
                    clientSocket.getOutputStream().write((response + "\r\n").getBytes());
                }
            }
        } finally {
            clientSocket.close();
        }
    }

    private static List<String> parseArguments(String receivedMessage) {
        List<String> args = Arrays.stream(receivedMessage.split("\r\n"))
                .filter(arg -> !arg.trim().isEmpty() && !arg.startsWith("*") && !arg.startsWith("$")).collect(Collectors.toList());
        System.out.println("[" + Thread.currentThread() + "] Received args: " + args);
        return args;
    }

    private static String handleCommand(List<String> args) {
        String command = args.get(0);
        System.out.println("[" + Thread.currentThread() + "] Received command: " + command);
        if ("ECHO".equalsIgnoreCase(command)) {
            return handleEcho(args.get(1));
        } else if ("PING".equalsIgnoreCase(command)) {
            return handlePing();
        } else if ("SET".equalsIgnoreCase(command)) {
            return handleSet(args.get(1), args.get(2));
        } else if ("GET".equalsIgnoreCase(command)) {
            return handleGet(args.get(1));
        } else {
            return handleUnknownCommand(command);
        }
    }

    private static String handleGet(String key) {
        String response = storage.getOrDefault(key, "-1");
        if (response.equals("-1")) {
            return "$-1";
        } else {
            return "$" + response.length() + "\r\n" + response + "\r\n";
        }
    }

    private static String handleSet(String key, String value) {
        storage.put(key, value);
        return "+OK";
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
}

