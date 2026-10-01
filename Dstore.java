
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

class Dstore {

    /** port to listen on **/
    private final int port;

    /** controller's port to talk to **/
    private final int cport;

    /** timeout in miliseconds **/
    private final int timeout;

    /** where to store the data locally **/
    private final String file_folder;



    public Dstore(int port, int cport, int timeout, String file_folder) {

        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.file_folder = file_folder;

        //delete all files in DStore
        Path filePath = Paths.get(file_folder);
        try {
            deleteFilesInPath(filePath);
        } catch (Exception e) {
            System.err.println("Error deleting all files in dstore " + port + " path " + e.getMessage());
        }
    }

    public static void deleteFilesInPath (Path filePath) {
        File filesList[] = filePath.toFile().listFiles();
        for(File file : filesList) {
            if (file.isFile()) {
                file.delete();
            }
        }
    }

    public static void main(String [] args) {
        int port, cport, timeout;
        String file_folder;
        Socket contollerSocket = null;
        ServerSocket dStoreSocket = null;

        try {
            port = Integer.parseInt(args[0]);
            cport = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            file_folder = (args[3]);
        } catch (Exception e) {
            System.err.println("Invalid argument(s): " + e.getMessage());
            System.exit(1);
            return;
        }

        new Dstore(port,cport,timeout,file_folder).start(contollerSocket,dStoreSocket);
    }

    public void start(Socket controllerSocket, ServerSocket dStoreSocket) {
        try {
            InetAddress address = InetAddress.getLocalHost();
            controllerSocket = new Socket(address, cport);
            dStoreSocket = new ServerSocket(port);

            PrintWriter sendToController = new PrintWriter(controllerSocket.getOutputStream(), true);
            sendToController.println(Protocol.JOIN_TOKEN + " " + port);

            //create a new thread for the controller to listen in on
            new Thread(new Dstore.ClientHandler(controllerSocket,sendToController)).start();

            //create new threads for every client that asks to communicate with DStore
            while (true) {
                try {
                    Socket client = dStoreSocket.accept();
                    new Thread(new Dstore.ClientHandler(client,sendToController)).start();
                } catch(Exception e) {
                    System.err.println("error: " + e);
                }
            }

        } catch (Exception e) {
            System.err.println("error: " + e);
        } finally {
            if (dStoreSocket != null) {
                try {
                    dStoreSocket.close();
                } catch (IOException e) {
                    System.err.println("error: " + e);
                }
            }
        }
    }
    private class ClientHandler implements Runnable {
        Socket client;
        PrintWriter sendToController;
        ClientHandler(Socket c, PrintWriter sToC) {
            client = c;
            sendToController = sToC;
        }
        public void run() {
            try {
                BufferedReader receiver = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter sender = new PrintWriter(client.getOutputStream(), true);

                String message;

                while((message = receiver.readLine()) != null) {
                    String command = message.split(" ")[0];
                    System.out.println("Received message: " + message + " From " + client);

                    // Handle any messages from the Controller or Client
                    switch(command) {
                        case Protocol.LIST_TOKEN -> {
                            StringBuilder file_list = new StringBuilder();
                            try {
                                Files.walk(Paths.get(file_folder)).filter(Files::isRegularFile).forEach(path -> file_list.append(" ").append(path.getFileName()));
                            } catch (IOException e) {
                                System.err.println("error while trying to read file_path for dstore " + port);
                            }
                            sendToController.println(Protocol.LIST_TOKEN + " " + file_list.toString());
                        }
                        case Protocol.REMOVE_TOKEN -> {
                            var requestParts = message.split(" ");
                            if (!(Controller.checkIfMalformed(command,requestParts))) {
                                String fileName = message.split(" ")[1];
                                Path filePath = Paths.get(file_folder,fileName);
                                if (Files.exists(filePath)) {
                                    try {
                                        Files.delete(filePath);
                                        sendToController.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                    } catch (IOException e) {
                                        System.err.println("failed to delete file " + fileName + " " + e.getMessage());
                                    }
                                } else {
                                    sendToController.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                }
                            }
                        }
                        case Protocol.REBALANCE_TOKEN -> {
                            handleRebalanceRequest(message);
                        }
                        case Protocol.STORE_TOKEN -> {
                            var requestParts = message.split(" ");
                            if (!(Controller.checkIfMalformed(command,requestParts))) {
                                String fileName = message.split(" ")[1];
                                int fileSize = Integer.parseInt(message.split(" ")[2]);
                                sender.println(Protocol.ACK_TOKEN);
                                client.setSoTimeout(timeout);
                                try {
                                    byte[] fileData = new byte[fileSize];
                                    client.getInputStream().readNBytes(fileData, 0, fileSize);
                                    Path filePath = Paths.get(file_folder,fileName);
                                    Files.createDirectories(filePath.getParent());
                                    Files.write(filePath, fileData);

                                    sendToController.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
                                } catch (Exception e) {
                                    System.err.println("Error storing file: " + e.getMessage());
                                }
                            }
                        }
                        case Protocol.LOAD_DATA_TOKEN -> {
                            var requestParts = message.split(" ");
                            if (!(Controller.checkIfMalformed(command,requestParts))) {
                                String filename = requestParts[1];
                                handleLoadRequest(filename,client.getOutputStream(),client);
                            }
                        }

                    }
                }
                client.close();
            } catch(SocketException e) {
                System.err.println("Connection dropped: " + e.getMessage());
            } catch(IOException e) {
                System.err.println("error: " + e);
            }
        }



        private void handleRebalanceRequest(String message) {

        }

        private void handleLoadRequest(String filename, OutputStream sender, Socket client) {
            try {
                Path filePath = Paths.get(file_folder, filename);
                byte[] fileData = Files.readAllBytes(filePath);
                sender.write(fileData);
            } catch (NoSuchFileException e) {
                System.err.println("File not found: " + filename);
                try {
                    //closes socket with client because it does not have requested file
                    client.close();
                } catch (IOException ex) {
                    System.err.println("Error closing socket: " + ex.getMessage());
                }
            } catch (IOException e) {
                System.err.println("Error loading file " + filename + " " + e.getMessage());
            }
        }
    }
}
