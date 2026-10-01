import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class Controller {

    static Index index;
    /** port to listen on **/
    private final int cport;

    /** keeps track of the number of STORE_ACK messages expected **/
    private Map<String,CountDownLatch> storeAckLatchAll;

    /** keeps track of the number of REMOVE_ACK messages expected **/
    private Map<String,CountDownLatch> removeAckLatchAll;

    private int counterForRebalance;

    /** replication factor
     * The Controller does not serve any client
     * request until at least R Dstores have joined the system
     * **/
    private final int R;

    /** timeout in miliseconds **/
    private final int timeout;

    /** how long to wait (in seconds) to start the next rebalance operation **/
    private final int rebalance_period;

    /** dstore Ports **/
    private final Set<Integer> dstorePorts;

    private final Map<Integer,Socket> dstoreSockets;

    private final Map<Integer, PrintWriter> dStoreOuts;

    private final Map<String,PrintWriter> fileNamesClientsStore;

    private final Map<String,PrintWriter> fileNamesClientsRemove;

    private final Map<String,ArrayList<Integer>> dStoresFiles;

    private final Map<Socket,Integer> clientsLoadCounter;

    HashMap<String,ArrayList<Integer>> rebalanceFilesToStore;


    public Controller(int cport, int R, int timeout, int rebalance_period) {

        index = new Index(new ArrayList<>(), new HashMap<>(), new HashMap<>());
        this.cport = cport;
        this.R = R;
        this.timeout = timeout;
        this.rebalance_period = rebalance_period;
        this.dstorePorts = new HashSet<>();
        this.dstoreSockets = new HashMap<>();
        this.dStoreOuts = new HashMap<>();
        this.fileNamesClientsStore = new HashMap<>();
        this.fileNamesClientsRemove = new HashMap<>();
        this.dStoresFiles = new HashMap<>();
        clientsLoadCounter = new HashMap<>();
        this.counterForRebalance = 0;
        this.rebalanceFilesToStore = new HashMap<>();
        this.storeAckLatchAll = new HashMap<>();
        this.removeAckLatchAll = new HashMap<>();
    }

    public static void main(String [] args) {
        int cport, R, timeout, rebalancePeriod;
        ServerSocket ss = null;

        try {
            cport = Integer.parseInt(args[0]);
            R = Integer.parseInt(args[1]);
            timeout = Integer.parseInt(args[2]);
            rebalancePeriod = Integer.parseInt(args[3]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid argument(s): " + e.getMessage());
            System.exit(1);
            return;
        }

        new Controller(cport,R,timeout,rebalancePeriod).start(ss);

    }

    public void start(ServerSocket ss) {
        try {
            ss = new ServerSocket(cport);
            System.out.println("Controller has started to listen on port " + cport);
            while (true) {
                try {
                    Socket client = ss.accept();
                    new Thread(new ClientHandler(client)).start();
                } catch(Exception e) {
                    System.err.println("error: " + e);
                }
            }
        } catch(Exception e) { System.err.println("error: " + e);
        } finally {
            if (ss != null)
                try {
                    ss.close();
                } catch (IOException e) {
                    System.err.println("error: " + e);
                }
        }
    }

    private synchronized boolean CheckEnoughDStores(PrintWriter out) {
        if (dstorePorts.size() >= R) {
            return true;
        } else {
            out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return false;
        }
    }

    public synchronized static boolean checkIfMalformed(String protocol, String[] message) {
        switch(protocol) {
            case Protocol.STORE_TOKEN -> {
                if (message.length != 3) {
                    System.out.println("STORE message is " + message.length + " length, not 3");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("STORE message filename is null");
                    return true;
                }
                try {
                    Integer.parseInt(message[2]);
                } catch (Exception e) {
                    System.out.println("STORE message filesize " + message[2] + " invalid");
                    return true;
                }
                return false;
            }
            case Protocol.LOAD_TOKEN ->  {
                if (message.length != 2) {
                    System.out.println("LOAD message is " + message.length + " length, not 2");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("LOAD message filename is null");
                    return true;
                }
                return false;
            }
            case Protocol.LOAD_DATA_TOKEN ->  {
                if (message.length != 2) {
                    System.out.println("LOAD_DATA message is " + message.length + " length, not 2");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("LOAD_DATA message filename is null");
                    return true;
                }
                return false;
            }
            case Protocol.STORE_ACK_TOKEN ->  {
                if (message.length != 2) {
                    System.out.println("STORE_ACK message is " + message.length + " length, not 2");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("STORE_ACK message filename is null");
                    return true;
                }
                return false;
            }
            case Protocol.REMOVE_TOKEN ->  {
                if (message.length != 2) {
                    System.out.println("REMOVE message is " + message.length + " length, not 2");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("REMOVE message filename is null");
                    return true;
                }
                return false;
            }
            case Protocol.REMOVE_ACK_TOKEN ->  {
                if (message.length != 2) {
                    System.out.println("REMOVE_ACK message is " + message.length + " length, not 2");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("REMOVE_ACK message filename is null");
                    return true;
                }
                return false;
            }
            case Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN ->  {
                if (message.length != 2) {
                    System.out.println("ERROR_FILE_DOES_NOT_EXIST message is " + message.length + " length, not 2");
                    return true;
                }
                if (message[1] == null) {
                    System.out.println("ERROR_FILE_DOES_NOT_EXIST message filename is null");
                    return true;
                }
                return false;
            }
            case Protocol.LIST_TOKEN ->  {
                if (message.length != 1) {
                    System.out.println("LIST message is " + message.length + " length, not 1");
                    return true;
                }
                return false;
            }
        }
        return false;
    }

private class ClientHandler implements Runnable {
        Socket client;
        int dstorePort;
        boolean isDstore = false;
        ClientHandler(Socket c) {
            client=c;
        }
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                int thisDStorePort = 0;
                String request;

                while((request = in.readLine()) != null) {
                    String command = request.split(" ")[0];
                    System.out.println("Received request: " + request + " From " + client);
                    if (!clientsLoadCounter.containsKey(client)) {
                        clientsLoadCounter.put(client,0);
                    }

                    switch (command) {
                        case Protocol.LIST_TOKEN -> {
                            clientsLoadCounter.put(client,0);
                            var requestParts = request.split(" ");
                            if (!isDstore) {
                                if (!(checkIfMalformed(command,requestParts))) {
                                    if(CheckEnoughDStores(out)) {
                                        handleListRequest(out);
                                    }
                                }
                            } else {
                                System.out.println("Dstore sent LIST request, sounds like some rebalancing stuff");
                                handleRebalanceList(requestParts,thisDStorePort);
                            }
                        }
                        case Protocol.JOIN_TOKEN -> {
                            isDstore = true;
                            thisDStorePort = Integer.parseInt(request.split(" ")[1]);
                            handleJoinRequest(out, request, client);
                            handleRebalanceOperation();
                        }
                        case Protocol.STORE_TOKEN -> {
                            clientsLoadCounter.put(client,0);
                            var requestParts = request.split(" ");
                            var filename = requestParts[1];
                            if (!(checkIfMalformed(command,requestParts))) {
                                if (CheckEnoughDStores(out)) {
                                    if (index.getFileStatus().containsKey(filename)) {
                                        out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                                    } else {
                                        fileNamesClientsStore.put(filename,out);
                                        handleStoreRequest(out,request,client);
                                    }
                                }
                            };
                        }
                        case Protocol.STORE_ACK_TOKEN -> {
                            clientsLoadCounter.put(client, 0);
                            System.out.println("Recieved a store ack!!");
                            var requestParts = request.split(" ");
                            var filename = requestParts[1];
                            if (!(checkIfMalformed(command,requestParts))) {
                                //if (Objects.equals(index.getFileStatus().get(filename), Index.s_storing)) {
                                    handleStoreComplete(requestParts,thisDStorePort,fileNamesClientsStore.get(filename));
                                //}
                            }
                        }
                        case Protocol.LOAD_TOKEN -> {
                            var requestParts = request.split(" ");
                            clientsLoadCounter.put(client,0);
                            if (!(checkIfMalformed(command,requestParts))) {
                                var filename = requestParts[1];
                                if (CheckEnoughDStores(out)) {
                                    if ((index.getFileStatus().containsKey(filename))) {
                                        if ((Objects.equals(index.getFileStatus().get(filename), Index.s_stored))) {
                                                handleLoadRequest(out,request,client);
                                        } else {
                                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                        }
                                    } else {
                                        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                    }
                                }
                            }
                        }
                        case Protocol.RELOAD_TOKEN -> {
                            handleReloadRequest(out,request,client);
                        }
                        case Protocol.REMOVE_TOKEN -> {
                            var requestParts = request.split(" ");
                            if (!(checkIfMalformed(command,requestParts))) {
                                var filename = requestParts[1];
                                if (CheckEnoughDStores(out)) {
                                    if (index.getFileStatus().containsKey(filename)) {
                                        if (!(Objects.equals(index.getFileStatus().get(filename), Index.s_removing))) {
                                            if (Objects.equals(index.getFileStatus().get(filename),Index.s_storing)) {
                                                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                            } else {
                                                //index.getFileStatus().put(filename,Index.s_removing);
                                                if (index.getFileNames().contains(filename)) {
                                                    fileNamesClientsRemove.put(filename,out);
                                                }
                                                handleRemoveRequest(out,request,client);
                                            }
                                        } else {
                                            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                        }
                                    } else {
                                        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                                    }
                                }
                            }
                        }
                        case Protocol.REMOVE_ACK_TOKEN, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN -> {
                            var requestParts = request.split(" ");
                            if ((!(checkIfMalformed(command,requestParts)))) {
                                String filename = requestParts[1];
                                if (Objects.equals(index.getFileStatus().get(filename), Index.s_removing)) {
                                    removeAckLatchAll.get(filename).countDown();
                                }
                            }
                        }

                    }
                }
                client.close();
            } catch(SocketException e) {
                System.err.println("Connection dropped: " + e.getMessage());
                // Remove the corresponding Dstore from the set of active Dstores
                dstorePorts.remove(dstorePort);
            } catch(IOException e) {
                System.err.println("error: " + e.getMessage());
            }
        }

    private synchronized void handleListRequest(PrintWriter out) {
        StringBuilder allFiles = new StringBuilder();
        if (!index.getFileNames().isEmpty()) {
            for (String file : index.getFileNames()) {
                if (Objects.equals(index.getFileStatus().get(file), Index.s_stored))
                    allFiles.append(" ").append(file);
            }
        }
        out.println("LIST" + allFiles);

    }

    private synchronized void handleStoreComplete(String[] request, Integer thisDStorePort, PrintWriter out) {
            String filename = request[1];
        ArrayList<Integer> dStoresWithFile = dStoresFiles.computeIfAbsent(filename, b -> new ArrayList<>());

        //Adds the dstore to the dstoreFiles map, key being value and adds dstore to value arraylist
        if (!dStoresWithFile.contains(thisDStorePort)) {
            dStoresWithFile.add(thisDStorePort);
        }
        //System.out.println("Store ack, the dstoresFiles looks like this: " + dStoresFiles.get(filename));

        if (Objects.equals(index.getFileStatus().get(filename), Index.s_storing)) {
            storeAckLatchAll.get(filename).countDown();
            //storeAckLatch.countDown();
            System.out.println("store ack latch is now " + storeAckLatchAll.get(filename).getCount() + " for file "  + filename);
        }
    }

     private synchronized void handleStoreRequest(PrintWriter out, String request, Socket client) {

            String[] requestsSplit = request.split(" ");
            String filename = requestsSplit[1];
            Integer filesize = Integer.parseInt(requestsSplit[2]);
            if (!index.getFileStatus().containsKey(filename)) {
                //updates index, "store in progress"
                index.getFileStatus().put(filename,Index.s_storing);
                System.out.println("index does not already contain filename " + filename);
                //fileNamesClientsStore.put(filename, out);

                //updates filesizes
                index.getFileSizes().put(filename,filesize);

                storeAckLatchAll.put(filename,new CountDownLatch(R));

                //selects R Dstores, their endpoints are port1, port2, ..., port R
                StringBuilder allPorts = new StringBuilder();
                var count = 0;
                for (int currentR : dstorePorts) {
                    if (count <= R ) {
                        allPorts.append(" ").append(currentR);
                        count++;
                    } else {
                        break;
                    }
                }
                String allPortsString = allPorts.toString();

                //controller -> client: STORE_TO port1 port2 .. portR
                out.println(Protocol.STORE_TO_TOKEN + allPortsString);

                Thread storeAckThread = new Thread(() -> {
                    try {
                        boolean awaitResult = storeAckLatchAll.get(filename).await(timeout, TimeUnit.MILLISECONDS);
                        if (!awaitResult) {
                            handleStoreTimeout(filename);
                        } else {
                            index.getFileNames().add(filename);
                            index.getFileStatus().put(filename,Index.s_stored);
                            System.out.println("store complete");
                            PrintWriter clientOut = fileNamesClientsStore.get(filename);
                            clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                        }
                    } catch (InterruptedException e) {
                        System.err.println(e.getMessage());
                    }
                });
                storeAckThread.start();
            } else {
                System.out.println("index already contain filename " + filename);
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            }
         }

    private synchronized void handleStoreTimeout(String filename) {
        System.out.println("store timeout happened, removing file from index " + filename);
        index.getFileStatus().remove(filename);
        index.getFileNames().remove(filename);
        index.getFileSizes().remove(filename);
        dStoresFiles.remove(filename);
    }

    private synchronized void handleJoinRequest(PrintWriter out, String request,Socket client) {
        dstorePort = Integer.parseInt(request.substring(5));
        dstorePorts.add(dstorePort);
        dstoreSockets.put(dstorePort,client);
        dStoreOuts.put(dstorePort,out);
        System.out.println("join from Dstore at port " + dstorePort);
        System.out.println("dstore that just joined: " + client.toString());

    }

    private synchronized void handleLoadRequest(PrintWriter out, String request, Socket client) {
        String filename = request.split(" ")[1];

        //if index contains file:
        if (index.getFileNames().contains(filename)) {
            int fileSize = index.getFileSizes().get(filename);
            int port = dStoresFiles.get(filename).get(clientsLoadCounter.get(client));
            out.println(Protocol.LOAD_FROM_TOKEN + " " + port + " " + fileSize);
        } else { //if not then file does not exist
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }
    }

    private void handleReloadRequest(PrintWriter out, String request, Socket client) {
            String filename = request.split(" ")[1];
            int counter = clientsLoadCounter.get(client);
            if (counter >= dStoresFiles.get(filename).size()-1) {
                out.println(Protocol.ERROR_LOAD_TOKEN);
            } else {
                if (CheckEnoughDStores(out)) {
                    clientsLoadCounter.put(client,counter+1);
                    handleLoadRequest(out,request,client);
                }
            }
    }

    private synchronized void handleRemoveRequest(PrintWriter out, String request, Socket client) {
        String[] requestsSplit = request.split(" ");
        String filename = requestsSplit[1];
        if (index.getFileStatus().containsKey(filename)) {
            System.out.println("index contains filename " + filename);

            //fileNamesClientsRemove.put(filename, out);

            //updates index, "remove in progress"
            index.getFileStatus().put(filename,Index.s_removing);

            //initializes removeAckLatch with replication factor R
            removeAckLatchAll.put(filename, new CountDownLatch(R));

            try {
                for (int currentR : dStoresFiles.get(filename)) {
                    try {
                        PrintWriter dStoreOut = dStoreOuts.get(currentR);
                        dStoreOut.println(Protocol.REMOVE_TOKEN + " " + filename);
                        System.out.println("send REMOVE to " + currentR);
                    } catch (Exception e) {
                        System.err.println("failed to send REMOVE " + filename + " to DStore " + currentR);
                    }
                }
            } catch (Exception e ){
                System.err.println("Somehow, that filename is not in the dstoresFiles list");
            }

            Thread removeAckThread = new Thread(() -> {
                try {
                    boolean awaitResult = removeAckLatchAll.get(filename).await(timeout,TimeUnit.MILLISECONDS);
                    if (!awaitResult) {
                        //do nothing, will remain as s_removing, rebalancing supposed to fix this
                    } else {
                         //All REMOVE_ACK messages received before timeout
                        index.getFileNames().remove(filename);
                        index.getFileStatus().remove(filename);
                        dStoresFiles.remove(filename);
                        PrintWriter clientOut = fileNamesClientsRemove.get(filename);
                        clientOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                    }
                } catch (InterruptedException e) {
                    System.err.println(e.getMessage());
                }
            });
            removeAckThread.start();

        } else {
            System.out.println("index does not contain filename " + filename);
            out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        }
    }

    private void handleRebalanceOperation() {
            rebalanceFilesToStore.clear();
            counterForRebalance = 0;
        //check if there are at least R detstores
        if (dstorePorts.size() < R) {
            return;
        }
            //for each DStore, send a LIST request to recieve list of files stored in that dstore.
            for (int dstorePort : dstorePorts) {
                try {
                    PrintWriter dStoreOut = dStoreOuts.get(dstorePort);
                    dStoreOut.println(Protocol.LIST_TOKEN);
                    System.out.println("sent LIST to DStore" + dstorePort);
                } catch (Exception e) {
                    System.err.println("failed to send LIST to dstore " + dstorePort);
                }
            }

    }

    private void handleRebalanceList(String[] request, Integer port) {

        ArrayList<String> allFiles = new ArrayList<>(Arrays.asList(request).subList(1, request.length));

        //update hashmap, of the ports that need files added too, and values being the files to add
        for (String file : index.getFileNames()) {
            if (Objects.equals(index.getFileStatus().get(file), Index.s_stored)) {
                //if the file is 'stored'

                //but the dstore that just sent the LIST doesn't have this file, then:
                if (!allFiles.contains(file)) {
                    if (rebalanceFilesToStore.containsKey(file)) {
                        rebalanceFilesToStore.get(file).add(port);
                    } else {
                        ArrayList<Integer> firstPort = new ArrayList<>();
                        firstPort.add(port);
                        rebalanceFilesToStore.put(file,firstPort);
                    }
                }
            }
        }

        counterForRebalance++;
        //if it has done this for the same amount of dstores that exist in connection,
        if (counterForRebalance >= dstorePorts.size()) {
            counterForRebalance = 0;

            StringBuilder files_to_send = new StringBuilder();

            var number_of_files_to_send = rebalanceFilesToStore.keySet().size();
            files_to_send.append(number_of_files_to_send);

            for (String filename : rebalanceFilesToStore.keySet()) {
                StringBuilder file_to_send_i = new StringBuilder(filename);
                file_to_send_i.append(" ").append(rebalanceFilesToStore.get(filename).size());
                for (Integer dport : rebalanceFilesToStore.get(filename)) {
                    file_to_send_i.append(" ").append(dport);
                }
                files_to_send.append(" ").append(file_to_send_i);
            }
            System.out.println("files_to_send is " + files_to_send.toString());





            // minimum and maximum replication factor
            double amountofDStores = dstorePorts.size();
            double minR = Math.floor(R * index.getFileNames().size() / amountofDStores);
            double maxR = Math.ceil(R * index.getFileNames().size() / amountofDStores);


        }
    }
    }
}

