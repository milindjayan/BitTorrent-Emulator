import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Formatter;
import java.util.logging.*;
import java.util.stream.Stream;

class Configuration {

    int numberOfPreferredNeighbors;
    int unchokingInterval;
    int optimisticUnchokingInterval;
    String fileName;
    int fileSize;
    int pieceSize;

    public Configuration() {
        this.numberOfPreferredNeighbors = 0;
        this.unchokingInterval = 0;
        this.optimisticUnchokingInterval = 0;
        this.fileSize = 0;
        this.pieceSize = 0;
    }

    public int getUnchokingInterval() {
        return unchokingInterval;
    }

    public int getOptimisticUnchokingInterval() {
        return optimisticUnchokingInterval;
    }

    public int getFileSize() {
        return this.fileSize;
    }

    public int getPieceSize() {
        return this.pieceSize;
    }

    public int getNumberOfPreferredNeighbors() {
        return numberOfPreferredNeighbors;
    }

    public int getTotalPieces() {
        int totalPieces = (int) Math.ceil((double) this.getFileSize() / this.getPieceSize());
        return totalPieces;
    }

    public void printConfigDetails() {
        System.out.println("-----------------Common.cfg-----------------");
        System.out.println("No. of preferred neighbors " + this.numberOfPreferredNeighbors);
        System.out.println("Unchoking Interval " + this.unchokingInterval);
        System.out.println("Optimistic Unchoking Interval " + this.optimisticUnchokingInterval);
        System.out.println("File Name " + this.fileName);
        System.out.println("File Size " + this.fileSize);
        System.out.println("Piece Size " + this.pieceSize);
    }

    public void printPeerDetails() {
        System.out.println("-----------------PeerInfo.cfg-----------------");
        for(int peerId : peerProcess.peerMap.keySet()){
            Peer peer = peerProcess.peerMap.get(peerId);
            System.out.println("PeerId: "+ peerId + " HostName: "+ peer.getHostName() + " Port: " + peer.getPort() + " hasFile: " + peer.getHasFile());
        }
    }
}

class MyLogger {

    Logger logger;
    FileHandler fileHandler;
    MyLogger(String peerId) throws IOException {
        logger = Logger.getLogger(peerProcess.class.getName());
        fileHandler = new FileHandler(".//"+ peerId +"//logs_"+ peerId+".log");
        fileHandler.setFormatter(new MyFormatter());
        logger.addHandler(fileHandler);
    }

    public void logInfo(String str){
        logger.log(new LogRecord(Level.INFO, str));
    }

    public void logError(String str){
        logger.log(new LogRecord(Level.SEVERE, str));
    }

    class MyFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
//            return record.getThreadID()+"::"+new Date(record.getMillis())+"::"+record.getMessage()+"\n";
            return new Date(record.getMillis())+" : "+record.getMessage()+"\n";
        }

    }
}

class MessageTypes {
    private ArrayList<String> messageTypes;

    public MessageTypes() {
        messageTypes = new ArrayList<>();
        messageTypes.add("CHOKE");
        messageTypes.add("UNCHOKE");
        messageTypes.add("INTERESTED");
        messageTypes.add("NOT_INTERESTED");
        messageTypes.add("HAVE");
        messageTypes.add("BITFIELD");
        messageTypes.add("REQUEST");
        messageTypes.add("PIECE");
    }

    public char getChokeIndex() {
        return (char)messageTypes.indexOf("CHOKE");
    }

    public char getUnchokeIndex() {
        return (char)messageTypes.indexOf("UNCHOKE");
    }

    public char getInterestedIndex() {
        return (char)messageTypes.indexOf("INTERESTED");
    }

    public char getNotInterestedIndex() {
        return (char)messageTypes.indexOf("NOT_INTERESTED");
    }

    public char getHaveIndex() {
        return (char)messageTypes.indexOf("HAVE");
    }
    public char getBitFieldIndex() {
        return (char)messageTypes.indexOf("BITFIELD");
    }
    public char getRequestIndex() {
        return (char)messageTypes.indexOf("REQUEST");
    }
    public char getPieceIndex() {
        return (char)messageTypes.indexOf("PIECE");
    }
}
class CommonConstants {

    private final static String THE_FILE = "thefile";
    private final static String COMMON_CFG_FILE_NAME = "Common.cfg";
    private final static String PEER_INFO_FILE_NAME = "PeerInfo.cfg";
    private final static String handShakeHeader = "P2PFILESHARINGPROJ";
    private final static String zeroPadding = "0000000000";
    private static String rootPath = System.getProperty("user.dir").concat("/");

    public static String getRootPath() {
        return rootPath;
    }

    public static String getHandShakeHeader() {
        return handShakeHeader;
    }

    public static String getZeroPadding() {
        return zeroPadding;
    }

    public static String getTheFileName() {
        return THE_FILE;
    }

    public static String getCommonConfigFileName() {
        return COMMON_CFG_FILE_NAME;
    }

    public static String getPeerInfoFileName() {
        return PEER_INFO_FILE_NAME;
    }
}

class Utils {

    public static int getRandomFilePiece(int[] currentPeerBitfield, int[] otherPeerBitfield, int size) {
        ArrayList<Integer> piecesRequired = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (currentPeerBitfield[i] == 0 && otherPeerBitfield[i] == 1) {
                piecesRequired.add(i);
            }
        }
        if (piecesRequired.size() <= 0) {
            return -1;
        } else {
            Random random = new Random();
            int index = Math.abs(random.nextInt() % piecesRequired.size());
            return piecesRequired.get(index);
        }
    }

    public static byte[] returnArrayCopy(byte[] original, int low, int high) {
        byte[] result = new byte[high - low];
        System.arraycopy(original, low, result, 0, Math.min(original.length - low, high - low));
        return result;
    }

    public static String extractString(byte[] byteArray, int startIndex, int endIndex) {
        int newLength = endIndex - startIndex + 1;
        //tbd - exception
        if (newLength <= 0 || endIndex >= byteArray.length) return "";
        byte[] copy = new byte[newLength];
        System.arraycopy(byteArray, startIndex, copy, 0, newLength);
        return new String(copy, StandardCharsets.UTF_8);
    }

    public static byte[] createHandshakePacket(int peerId) {
        byte[] hSPacket = new byte[32];

        byte[] headerInBytes = CommonConstants.getHandShakeHeader().getBytes();
        byte[] zerosInBytes = CommonConstants.getZeroPadding().getBytes();
        byte[] peerIdInBytes = ByteBuffer.allocate(4).put(String.valueOf(peerId).getBytes()).array();
        int index = 0;

        for (int i = 0; i < headerInBytes.length; i++) {
            hSPacket[index++] = headerInBytes[i];
        }

        for (int i = 0; i < zerosInBytes.length; i++) {
            hSPacket[index++] = zerosInBytes[i];
        }

        for (int i = 0; i < peerIdInBytes.length; i++) {
            hSPacket[index++] = peerIdInBytes[i];
        }

        return hSPacket;
    }

    public static boolean checkMissingPieces(int[] currentPeerBitfield, int[] otherPeerBitfield, int size) {
        for (int i = 0; i < size; i++) {
            if (currentPeerBitfield[i] == 0 && otherPeerBitfield[i] == 1) {
                return true;
            }
        }
        return false;
    }
}



class Peer {
    private int peerId;
    private String hostName;
    private int port;
    private int hasFile;
    private int[] bitfield;
    private int numberOfPieces;

    public Peer(int peerId, String hostName, int port, int hasFile) {
        this.peerId = peerId;
        this.hostName = hostName;
        this.port = port;
        this.hasFile = hasFile;
        this.numberOfPieces = 0;
    }

    public void setHasFile(int hasFile) {
        this.hasFile = hasFile;
    }

    public int getHasFile() {
        return this.hasFile;
    }

    public int getPeerId() {
        return this.peerId;
    }

    public void downloadComplete() {
        this.hasFile = 1;
    }

    public int getPiecesLength() {
        return this.bitfield.length;
    }

    public void setBitfield(int[] bitfield) {
        this.bitfield = bitfield;
    }

    public int getNoOfPiecesStored() {
        int no = 0;
        for (int i = 0; i < bitfield.length; i++) if (bitfield[i] == 1) no++;
        return no;
    }

    public void incrementNumOfPieces() {
        this.numberOfPieces++;
        if (this.numberOfPieces == this.bitfield.length) {
            this.hasFile = 1;
        }
    }

    public int getNumberOfPieces() {
        return this.numberOfPieces;
    }

    public int getPort() {
        return this.port;
    }


    public String getHostName() {
        return this.hostName;
    }

    public int[] getBitField() {
        return this.bitfield;
    }

    public void markBitOn(int i) {
        this.bitfield[i] = 1;
    }
}



public class peerProcess {
    static Configuration configuration;
    static int currentPeerId;
    static LinkedHashMap<Integer, Peer> peerMap;
    private static ConcurrentHashMap<Integer, PeerSocket> socketMap;
    static Peer currentPeer;
    static MessageTypes messageTypes;
    static byte[][] currentFilePieces;
    static int peersCompleted = 0;
    static File currentNodeDir;
    private static String theFileName;
    static MyLogger logger;

    private static class ParentThread extends Thread {
        private PeerSocket peerSocket;

        public ParentThread(PeerSocket peerSocket) {
            this.peerSocket = peerSocket;
        }

        public void printDownloadProgress() {
            double downloadedPercentage = ((currentPeer.getNumberOfPieces() * 100.0) / configuration.getTotalPieces());
            System.out.println(currentPeer.getNumberOfPieces() + "/" + configuration.getTotalPieces() + " downloaded: " + downloadedPercentage +"% ");
        }

        @Override
        public void run() {
            synchronized (this) {

                try {
                    DataInputStream inputStream = new DataInputStream(peerSocket.getSocket().getInputStream());
                    System.out.println("Sending bit field msg ... ");
                    peerSocket.sendBitFieldMsg();
                    while (peersCompleted < peerMap.size()) {
                        int size = inputStream.readInt();

                        byte[] formattedMessage = new byte[size - 1];
                        byte[] message = new byte[size];

                        double startTime = (System.nanoTime() / 100000000.0);
                        inputStream.readFully(message);
                        double endTime = (System.nanoTime() / 100000000.0);

                        char messageType = (char) message[0];
                        for (int i = 1; i < size; i++) {
                            formattedMessage[i-1] = message[i];
                        }

                        if (messageType == messageTypes.getBitFieldIndex()) {
                            int[] bitfield = new int[formattedMessage.length / 4];
                            int count = 0;
                            for (int i = 0; i < formattedMessage.length; i += 4) { //
                                byte[] temp = Utils.returnArrayCopy(formattedMessage, i, i + 4);
                                bitfield[count++] = ByteBuffer.wrap(temp).getInt();
                            }

                            Peer peer = peerMap.get(this.peerSocket.getPeerId());
                            peer.setBitfield(bitfield);
                            int currentPeerPieces = peer.getNoOfPiecesStored();

                            if (currentPeerPieces == currentPeer.getPiecesLength()) {
                                peer.setHasFile(1);
                                peersCompleted++;
                            }else {
                                peer.setHasFile(0);
                            }

                            boolean missingPieces = Utils.checkMissingPieces(currentPeer.getBitField(),
                                    peer.getBitField(), peer.getPiecesLength());

                            if (missingPieces==true) {
                                this.peerSocket.sendInterestedMessage(); //Should I change the names of these functions??
                            }
                            else {
                                this.peerSocket.sendNotInterestedMessage() ;
                            }

                        }

                        else if (messageType == messageTypes.getInterestedIndex()) {
                            peerSocket.setInterested(true);
                            logger.logInfo("Peer "+ currentPeer.getPeerId() +" received the ‘interested’ formattedMessage from "+ peerSocket.peerId);
                        }

                        else if (messageType == messageTypes.getNotInterestedIndex()) {
                            peerSocket.setInterested(false);
                            logger.logInfo("Peer "+ currentPeer.getPeerId() +" received the ‘not interested’ formattedMessage from "+ peerSocket.peerId);
                            if (!peerSocket.getIsChoked()) {
                                peerSocket.chokeConnection();
                                peerSocket.sendChokeMessage();
                            }
                        }

                        else if (messageType == messageTypes.getUnchokeIndex()) {
                            peerSocket.unChoke();
                            logger.logInfo("Peer " + currentPeer.getPeerId()+ " is unchoked by "+ peerSocket.peerId); //Is this right?
                            System.out.println(peerSocket.getPeerId() + " is unchoked");
                            Peer connectedPeerObject = peerMap.get(peerSocket.getPeerId());

                            int randomFilePiece = Utils.getRandomFilePiece(currentPeer.getBitField(),
                                    connectedPeerObject.getBitField(), connectedPeerObject.getPiecesLength());

                            if (randomFilePiece == -1) {
                                System.out.println("No more pieces required.");
                            }
                            else {
                                peerSocket.sendRequestMessage(randomFilePiece);
                            }

                        }
                        else if (messageType == messageTypes.getRequestIndex()) {
                            peerSocket.sendPieceMessage(ByteBuffer.wrap(formattedMessage).getInt());
                        }
                        else if (messageType == messageTypes.getPieceIndex()) {
                            int receivedPieceIndex = ByteBuffer.wrap(Utils.returnArrayCopy(formattedMessage, 0, 4)).getInt();
                            Peer neighborPeer = peerMap.get(peerSocket.getPeerId());
                            currentFilePieces[receivedPieceIndex] = new byte[formattedMessage.length - 4];
                            int index = 0;
                            for (int i = 4; i < formattedMessage.length; i++) {
                                byte[] currentFilePosition = currentFilePieces[receivedPieceIndex];
                                currentFilePosition[index++] = formattedMessage[i];
                            }
                            currentPeer.markBitOn(receivedPieceIndex);
                            currentPeer.incrementNumOfPieces();
                            if (!peerSocket.getIsChoked()) {
                                int pieceIndex = Utils.getRandomFilePiece(currentPeer.getBitField(),
                                        neighborPeer.getBitField(), neighborPeer.getPiecesLength());
                                if (pieceIndex != -1) {
                                    peerSocket.sendRequestMessage(pieceIndex);
                                }
                            }
                            double downSpeed = ((double) (formattedMessage.length + 5) / (endTime - startTime));
                            if (neighborPeer.getHasFile() == 1) {
                                peerSocket.setDownloadSpeed(-1);
                            } else {
                                peerSocket.setDownloadSpeed(downSpeed);
                            }
                            logger.logInfo("Peer "+ currentPeer.getPeerId() + "has downloaded the piece "+ receivedPieceIndex + " from "+ peerSocket.getPeerId()+".");

                            printDownloadProgress();
                            peerSocket.downloadCompleted(receivedPieceIndex);
                            for (int socketNodeId : socketMap.keySet()) {
                                PeerSocket peerSocket = socketMap.get(socketNodeId);
                                peerSocket.sendHaveMessage(receivedPieceIndex);
                            }

                        } else if (messageType == messageTypes.getHaveIndex()) {
                            int havePieceIndex = ByteBuffer.wrap(formattedMessage).getInt();
                            Peer peer = peerMap.get(peerSocket.getPeerId());
                            peer.markBitOn(havePieceIndex);
                            if (peer.getNoOfPiecesStored() == currentPeer.getPiecesLength()) {
                                peer.setHasFile(1);
                                peersCompleted++;
                            }


                            if (Utils.checkMissingPieces(currentPeer.getBitField(), peer.getBitField(), peer.getPiecesLength())) {
                                peerSocket.sendInterestedMessage();
                            } else {
                                peerSocket.sendNotInterestedMessage() ;
                            }
                            logger.logInfo("Peer " + currentPeer.getPeerId()+" received the ‘have’ formattedMessage from "+ peerSocket.getPeerId()+" for the piece " + havePieceIndex);

                        }
                        else if (messageType == messageTypes.getChokeIndex()) {
                            logger.logInfo("Peer "+ currentPeer.getPeerId() +" is choked by "+ peerSocket.peerId);
                            peerSocket.chokeConnection();
                        }
                    }

                    System.out.println("Finished executing");
                    Thread.sleep(6000);
                }
                catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class PeerSocket {

        private double downloadSpeed = 0;
        private boolean isOptimisticallyUnchoked = false;
        private Socket socket;
        private int peerId;
        private boolean isInterested = false;
        private boolean isChoked = true;

        public PeerSocket(Socket socket, int peerId) {
            this.socket = socket;
            this.peerId = peerId;
            (new ParentThread(this)).start();
        }

        public int getPeerId() {
            return this.peerId;
        }

        public double getDownloadSpeed() {
            return downloadSpeed;
        }

        public void setDownloadSpeed(double downloadSpeed) {
            this.downloadSpeed = downloadSpeed;
        }

        public boolean getIsChoked() {
            return this.isChoked;
        }

        public void chokeConnection() {
            this.isChoked = true;
        }

        public void optimisticallyUnchoke() {
            this.isOptimisticallyUnchoked = true;
        }

        public void optimisticallyChoke() {
            isOptimisticallyUnchoked = false;
        }

        public boolean isOptimisticallyUnchoked() {
            return isOptimisticallyUnchoked;
        }

        public void unChoke() {
            this.isChoked = false;
        }

        public boolean isInterested() {
            return this.isInterested;
        }

        public void setInterested(boolean interested) {
            isInterested = interested;
        }

        public Socket getSocket() {
            return this.socket;
        }

        public byte[] getFilePieces(int pInd, byte[] piece) {
            int index = 0;
            int pieceLength = piece.length;
            byte[] result = new byte[pieceLength+4];

            byte[] tempArray = ByteBuffer.allocate(4).putInt(pInd).array();
            int i = 0;
            while (i < tempArray.length) {
                result[index] = tempArray[i];
                i = i + 1;
                index = index + 1;
            }
            i = 0;
            while (i < pieceLength) {
                result[index] = piece[i];
                i = i + 1;
                index = index + 1;
            }
            byte[] returnPacket = null;


            try {
                int totalLength = pieceLength + 5;
                char type = messageTypes.getPieceIndex();
                returnPacket =  buildPacket(totalLength, type, result);

            } catch(CustomException e) {
                e.printStackTrace();
                System.exit(0);
            }

            return returnPacket;
        }

        public byte[] buildPacket(int length, char messageType, byte[] data) throws CustomException {

            if (messageType == messageTypes.getInterestedIndex() || messageType == messageTypes.getNotInterestedIndex() || messageType == messageTypes.getUnchokeIndex() || messageType == messageTypes.getChokeIndex()) {

                int index = 0;
                byte type = (byte) messageType;
                byte[] returnPacket = new byte[length + 4];
                byte[] header = ByteBuffer.allocate(4).putInt(length).array();
                int checkLength = header.length;
                int j = 0;
                while (j<checkLength) {
                    byte m = header[j];
                    returnPacket[index] = m;
                    index = index+1;
                    j = j + 1;
                }
                returnPacket[index] = type;
                return returnPacket;
            }
            else if (messageType == messageTypes.getBitFieldIndex() || messageType == messageTypes.getRequestIndex() || messageType == messageTypes.getPieceIndex() || messageType == messageTypes.getHaveIndex()) {

                int index = 0;
                byte msgType = (byte) messageType;
                byte[] resultPacket = new byte[length + 4];
                byte[] header = ByteBuffer.allocate(4).putInt(length).array();
                int checkLength = header.length;
                int checkLength1 = data.length;
                int i = 0;
                while(i<checkLength){
                    byte m = header[i];
                    resultPacket[index] = m;
                    index = index+1;
                    i = i + 1;
                }
                resultPacket[index++] = msgType;
                int j = 0;
                while(j<checkLength1){
                    byte m = data[j];
                    resultPacket[index] = m;
                    index = index + 1;
                    j = j + 1;
                }

                return resultPacket;
            }
            else {
                throw new CustomException("Invalid message type " + messageType);
            }
        }

        public void sendHaveMessage(int pieceIndex) {

            try {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();

                byte[] load = ByteBuffer.allocate(4).putInt(pieceIndex).array();
                byte[] haveMessage = null;

                try {
                    haveMessage = buildPacket(5, messageTypes.getHaveIndex(), load);
                } catch(CustomException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
                outputStream.write(haveMessage);
                outputStream.flush();

            }  catch (IOException exception) {
                System.out.println("Unable to send have.");
            }
        }

        public void sendRequestMessage(int index) {
            try {
                DataOutputStream outputStream  = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                byte[] load = ByteBuffer.allocate(4).putInt(index).array();
                byte[] requestMessage = null;
                try {
                    requestMessage = buildPacket(5, messageTypes.getRequestIndex(), load);
                } catch(CustomException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
                outputStream.write(requestMessage);
                outputStream.flush();

            }  catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void sendPieceMessage(int index) {

            try {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                outputStream.write(getFilePieces(index, currentFilePieces[index]));
                outputStream.flush();

            }  catch (IOException exception) {
                exception.printStackTrace();
            }
        }

        public void sendInterestedMessage() {
            try {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                byte[] interestedMessage;
                interestedMessage = buildPacket(1, messageTypes.getInterestedIndex(), null);
                outputStream.write(interestedMessage);
                outputStream.flush();
            } catch(CustomException | IOException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }

        public void sendBitFieldMsg() {
            try{
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                int[] bitField = currentPeer.getBitField();
                int totalLength = currentPeer.getBitField().length;
                int newMessageLength = (4 * totalLength)+ 1;
                byte[] load = new byte[newMessageLength - 1];
                int index = 0;
                for (int j=0;j < totalLength;j++) {
                    int ind = bitField[j];
                    byte[] numberByteArray = ByteBuffer.allocate(4).putInt(ind).array();
                    int checkLength = numberByteArray.length;
                    for (int k=0;k < checkLength;k++) {
                        byte oneByte = numberByteArray[k];
                        load[index] = oneByte;
                        index = index + 1;
                    }
                }

                byte[] bitMessage = buildPacket(newMessageLength, messageTypes.getBitFieldIndex(), load);

                outputStream.write(bitMessage);
                outputStream.flush();
            } catch(IOException | CustomException exception){
                exception.printStackTrace();
                System.exit(0);
            }
        }

        public void sendNotInterestedMessage() {
            try {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                byte[] iamNotInterestedMessage = buildPacket(1, messageTypes.getNotInterestedIndex(), null);

                outputStream.write(iamNotInterestedMessage);
                outputStream.flush();
            } catch (IOException | CustomException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }

        public void sendChokeMessage() {
            try {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                byte[] chokeMessage = buildPacket(1, messageTypes.getChokeIndex(), null);

                outputStream.write(chokeMessage);
                outputStream.flush();

            } catch (IOException | CustomException e) {
            e.printStackTrace();
            System.exit(0);
            }
        }

        public void sendUnChokeMessage() {

            try {
                DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
                outputStream.flush();
                byte[] unchokeMessage = buildPacket(1, messageTypes.getUnchokeIndex(), null);

                outputStream.write(unchokeMessage);
                outputStream.flush();

            } catch (IOException | CustomException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }

        private  void writeFile(String filePath, byte[] newFile){
            try {
                BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(filePath));
                outputStream.write(newFile);
                outputStream.close();
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        public void downloadCompleted(int fileIndex) {
            int totalNumberOfPieces = 0;
            int[] currentNodeBitFields = currentPeer.getBitField();
            int checkLength = currentNodeBitFields.length;
            int k = 0;
            while (k<checkLength) {
                if (currentNodeBitFields[k] == 1) {
                    totalNumberOfPieces = totalNumberOfPieces + 1;
                }
                k = k + 1;
            }


            if (totalNumberOfPieces == checkLength) {
                logger.logInfo("Peer " + currentPeer.getPeerId() + " has downloaded the complete file.");
                int index = 0;
                int fileSize = configuration.getFileSize();
                byte[] newFile = new byte[fileSize];
                int checkLength1 = currentFilePieces.length;
                for (int i=0;i < checkLength1;i++) {
                    int checkLength2 =  currentFilePieces[i].length;
                    for (int j=0; j < checkLength2;j++) {
                        byte currentByte = currentFilePieces[i][j];
                        newFile[index] = currentByte;
                        index = index + 1;
                    }
                }
                try {
                    String finalFilePath = CommonConstants.getRootPath() + "/" + currentPeerId + "/" + (theFileName);
                    writeFile(finalFilePath, newFile);
                    currentPeer.downloadComplete();
                    peersCompleted += 1;

                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else {
                int i = 0;
                int resultChunkSize = configuration.getPieceSize();
                byte[] newByteArray = new byte[resultChunkSize];
                for (int j=0; j < currentFilePieces[fileIndex].length;j++) {
                    byte currByte = currentFilePieces[fileIndex][j];
                    newByteArray[i++] = currByte;
                }

                try {
                    String finalFilePath = CommonConstants.getRootPath() + "/" + currentPeerId + "/" + ("piece_" + theFileName + "_" + fileIndex);
                    writeFile(finalFilePath, newByteArray);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static class Server implements Runnable {

        @Override
        public void run() {
            try {
                //Wait for new connections from all peers initialized after itself.
                ServerSocket server = new ServerSocket(currentPeer.getPort());
                byte[] handshakePacket = new byte[32];
                boolean newPeers = false;
                for(Integer peerID : peerMap.keySet()){
                    Peer tempPeer = peerMap.get(peerID);
                    if(newPeers){
                        Socket socket = server.accept();
                        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                        inputStream.readFully(handshakePacket);
                        logger.logInfo("Peer " + currentPeerId +" receives " + Utils.extractString(handshakePacket, 0, 31));
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        handshakePacket = Utils.createHandshakePacket(currentPeerId);
                        outputStream.write(handshakePacket);
                        outputStream.flush();

                        socketMap.put(peerID, new PeerSocket(socket, peerID));
                        logger.logInfo("Peer" + currentPeerId + " is connected from Peer" + peerID);
                    }

                    if (currentPeerId == peerID) newPeers = true;
                }
                //Completes after all TCP connections have been created.
                server.close();

            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }

    private static class Client implements Runnable {

        @Override
        public void run() {
            try {
                for (Integer neighborPeerId : peerMap.keySet()) {
                    if (neighborPeerId == currentPeerId) break;
                    Peer neighborPeer = peerMap.get(neighborPeerId);
                    Socket socket = new Socket(neighborPeer.getHostName(), neighborPeer.getPort());
                    logger.logInfo("Peer " + currentPeerId + " makes a connection to Peer "+neighborPeerId);
                    System.out.println("Client: " + neighborPeerId + " Socket created. Connecting to Server: " + neighborPeer.getHostName()
                            + " with " + neighborPeer.getPort());
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    byte[] handshakePacket = Utils.createHandshakePacket(currentPeerId);
                    outputStream.write(handshakePacket);
                    outputStream.flush();
                    logger.logInfo("Peer "+currentPeerId+ "sends handshake to "+neighborPeerId);
                    System.out.println("Client: " + neighborPeerId + " Handshake packet sent to Connecting to Server: "
                            + neighborPeer.getHostName() + " with " + neighborPeer.getPort());

                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    inputStream.readFully(handshakePacket);
                    String receivedHeader = Utils.extractString(handshakePacket, 0, 17);
                    String receivedPeerId = Utils.extractString(handshakePacket, 28, 31);

                    //Authenticating handshake
                    if(receivedHeader.equals(CommonConstants.getHandShakeHeader()) && Integer.parseInt(receivedPeerId) == neighborPeerId){
                        socketMap.put(neighborPeerId, new PeerSocket(socket, neighborPeerId));
                        logger.logInfo("Peer " + currentPeerId +" receives " + Utils.extractString(handshakePacket, 0, 31));
                        System.out.println("Client: " + neighborPeerId + " Handshake packet received from Server: " + neighborPeer.getHostName()
                                + " with " + neighborPeer.getPort() + " Packet = " + Utils.extractString(handshakePacket, 0, 31) + " appended to "
                                + " Updated connections " + socketMap.size() + "/" + (peerMap.size() - 1)
                                + " connections till now");
                    }else{
                        socket.close();
                    }
                }

            }
            catch (IOException exception) {

                exception.printStackTrace();

            }
        }
    }

    public static void initializeResources(String peerId) throws IOException {
        messageTypes = new MessageTypes();//for messages
        configuration = new Configuration();//common cfg data
        peerMap = new LinkedHashMap<>();//peer info cfg hash map
        socketMap = new ConcurrentHashMap<>();
        createDirectory(peerId);
        getTorrentDetails();
        readCommonConfig();
        configuration.printConfigDetails();
        readPeerConfig();
        configuration.printPeerDetails();
        logger = new MyLogger(peerId);
        socketMap = new ConcurrentHashMap<>();
    }

    public static void readPeerConfig() throws IOException {
        ArrayList<String> aList = parseFile(CommonConstants.getPeerInfoFileName());
        for(String line : aList){
            String[] words = line.split(" ");
            peerMap.put(Integer.valueOf(words[0]), new Peer(Integer.parseInt(words[0]), words[1], Integer.valueOf(words[2]), Integer.parseInt(words[3])));
        }
    }

    public static void readCommonConfig() throws IOException {
        ArrayList<String> aList = parseFile(CommonConstants.getCommonConfigFileName());
        configuration.numberOfPreferredNeighbors = Integer.parseInt(aList.get(0).split(" ")[1]);
        configuration.unchokingInterval = Integer.parseInt(aList.get(1).split(" ")[1]);
        configuration.optimisticUnchokingInterval = Integer.parseInt(aList.get(2).split(" ")[1]);
        configuration.fileName = aList.get(3).split(" ")[1];
        configuration.fileSize = Integer.parseInt(aList.get(4).split(" ")[1]);
        configuration.pieceSize = Integer.parseInt(aList.get(5).split(" ")[1]);
    }

    public static ArrayList<String> parseFile(String fileName) throws IOException {
        ArrayList<String> aList = new ArrayList<>();
        BufferedReader bReader = new BufferedReader(new FileReader(fileName));
        String line = bReader.readLine();
        while(line != null){
            aList.add(line);
            line = bReader.readLine();
        }
        bReader.close();
        return aList;
    }

    public static void createDirectory(String peerId) throws IOException {

        Path path = Paths.get(".//"+peerId);
        if(Files.exists(path)){
            clearDirectory(path);
            currentNodeDir = path.toFile();
        }else{
            currentNodeDir = Files.createDirectory(path).toFile();
        }
    }

    private static void clearDirectory(Path path) throws IOException {
        //Deleting every file.
        String fileName = CommonConstants.getTheFileName();
        Stream<Path> files = Files.list(path);
        for(Object obj : files.toArray()){
            Path file = (Path) obj;
            if(!file.getFileName().toString().equals(fileName)){
                Files.deleteIfExists(file);
            }
        }
        files.close();
    }

    public static void writeFilePieces() throws IOException {
        String filePath = CommonConstants.getRootPath()+ currentPeerId + "/" + CommonConstants.getTheFileName();
        BufferedInputStream file = new BufferedInputStream(new FileInputStream(filePath));
        int fileSize = configuration.getFileSize();
        int pieceSize = configuration.getPieceSize();
        byte[] byteArray = new byte[fileSize];
        file.read(byteArray);
        file.close();
        int pieceIndex = 0, cnt = 0;
        while(pieceIndex < fileSize) {
            if (pieceIndex + pieceSize <= fileSize)
                currentFilePieces[cnt] = Utils.returnArrayCopy(byteArray, pieceIndex, pieceIndex + pieceSize);
            else
                currentFilePieces[cnt] = Utils.returnArrayCopy(byteArray, pieceIndex, fileSize);
            currentPeer.incrementNumOfPieces();
            pieceIndex += pieceSize;
            cnt++;
        }
    }

    public static void divideIntoPieces() throws IOException {
        int pieceSize = configuration.getPieceSize();
        int fileSize = configuration.getFileSize();
        int noOfPieces = (int) Math.ceil((double)fileSize / pieceSize);
        currentFilePieces = new byte[noOfPieces][];

        int[] pieceMarker = new int[noOfPieces];
        Arrays.fill(pieceMarker, 1);

        if(currentPeer.getHasFile() == 1){
            peersCompleted++;
            Arrays.fill(pieceMarker, 1);
            currentPeer.setBitfield(pieceMarker);
            writeFilePieces();
        }else{
            Arrays.fill(pieceMarker, 0);
            currentPeer.setBitfield(pieceMarker);
        }
    }

    public static void getTorrentDetails() {
        theFileName = CommonConstants.getTheFileName();
    }
    public static void main(String[] args) throws IOException {

        currentPeerId = Integer.parseInt(args[0]);
        initializeResources(String.valueOf(currentPeerId));
        currentPeer = peerMap.get(currentPeerId);
        divideIntoPieces();

        new Thread(new Client()).start();
        new Thread(new Server()).start();
        new Thread(new UnchokedPeer()).start();
        new Thread(new OptimistcallyUnchokedPeer()).start();

    }
    static class CustomComparator implements Comparator<Integer> {
        Map<Integer, Double> map;
        public CustomComparator(Map<Integer, Double> map) {
            this.map = map;
        }
        public int compare(Integer a, Integer b) {
            if (map.get(a) >= map.get(b))return -1;
            else return 1;
        }
    }

    private static class OptimistcallyUnchokedPeer implements Runnable {

        public List<Integer> getConnectionIDs() {
            List<Integer> connectionIDs = new ArrayList<>();
            for (int i: socketMap.keySet()) {
                connectionIDs.add(i);
            }
            return connectionIDs;
        }

        public List<Integer> getInterestedConnections(List<Integer> connectionsList) {
            List<Integer> interestedPeers = new ArrayList<>();
            int i = 0;
            while(i< connectionsList.size()){
                int connection = connectionsList.get(i);
                if (socketMap.get(connection).isInterested()) {
                    interestedPeers.add(connection);
                }
                i++;
            }
            return interestedPeers;
        }
        @Override
        public void run() {
            while (peersCompleted < peerMap.size()) {
                List<Integer> interestedPeers = getInterestedConnections(getConnectionIDs());
                if (interestedPeers.size() > 0) {
                    Random random = new Random();
                    int someIndex = Math.abs(random.nextInt() % interestedPeers.size());

                    PeerSocket getConnection = socketMap.get(interestedPeers.get(someIndex));
                    getConnection.unChoke();
                    getConnection.sendUnChokeMessage();
                    getConnection.optimisticallyUnchoke();
                    logger.logInfo("Peer "+ currentPeerId +" has  the  optimistically  unchoked  neighbor  "+getConnection.getPeerId());
                    try {
                        Thread.sleep(configuration.getOptimisticUnchokingInterval() * 1000);
                        getConnection.optimisticallyChoke();

                    }
                    catch (Exception exception) {
                        System.out.println("Error");
                    }
                    finally {
                        System.out.println("Finished");
                    }
                }
            }
            try {
                System.out.println("Thread is sleeping Optimistic");
                Thread.sleep(5000);
            }
            catch (Exception e) {
                System.out.println("Error");
            }
            System.exit(0);
        }
    }

    private static class UnchokedPeer implements Runnable {

        public List<Integer> getConnectionIDs() {
            List<Integer> connectionIDs = new ArrayList<>();
            for (int i: socketMap.keySet()) {
                connectionIDs.add(i);
            }
            return connectionIDs;
        }


        public List<Integer> getInterestedConnections(List<Integer> connectionsList) {
            List<Integer> interestedPeers = new ArrayList<>();
            int i = 0;
            while(i< connectionsList.size()){
                int conn = connectionsList.get(i);
                PeerSocket peerSocket = socketMap.get(conn);
                if (peerSocket.isInterested())
                    interestedPeers.add(conn);
                i++;
            }
            return interestedPeers;
        }

        public List<Integer> getPeers_DownloadRate(List<Integer> connectionsList) {
            List<Integer> connInterested = new ArrayList<>();
            for (int peer : connectionsList) {
                PeerSocket connectionObject = socketMap.get(peer);
                if (connectionObject.isInterested() && connectionObject.getDownloadSpeed() >= 0)
                    connInterested.add(peer);
            }
            return connInterested;
        }

        @Override
        public void run() {

            while (peersCompleted < peerMap.size()) {
                List<Integer> conns = getConnectionIDs();
                if (currentPeer.getHasFile() == 1) {
                    List<Integer> interestedConnections = getInterestedConnections(conns);
                    if (interestedConnections.size() <= 0) {
                        System.out.println("No more peers are interested.");
                    }else {
                        if (interestedConnections.size() <= configuration.getNumberOfPreferredNeighbors()) {
                            int i = 0;
                            while(i< interestedConnections.size()){
                                int peer = interestedConnections.get(i);
                                PeerSocket tempConnection = socketMap.get(peer);
                                if (tempConnection.getIsChoked()==true) {
                                    tempConnection.unChoke();
                                    tempConnection.sendUnChokeMessage();
                                }
                                i++;
                            }
                            System.out.println("Peer "+ currentPeerId +" has the preferred neighbors " + interestedConnections);
                            logger.logInfo("Peer "+ currentPeerId +" has the preferred neighbors "+ interestedConnections);
                        }
                        else {
                            int[] prefNeighbors = new int[configuration.getNumberOfPreferredNeighbors()];
                            Random random = new Random();
                            int i = 0;
                            while(i< configuration.getNumberOfPreferredNeighbors()){
                                int someIndex = Math.abs(random.nextInt() % interestedConnections.size());
                                prefNeighbors[i] = interestedConnections.remove(someIndex);
                            }
                            i=0;
                            while(i< configuration.getNumberOfPreferredNeighbors()){
                                int tempId = prefNeighbors[i];
                                PeerSocket connectionObject = socketMap.get(tempId);
                                if (connectionObject.getIsChoked()) {
                                    connectionObject.unChoke();
                                    connectionObject.sendUnChokeMessage();
                                }
                                i++;
                            }
                            i=0;
                            while(i<interestedConnections.size()){
                                int peer = interestedConnections.get(i);
                                PeerSocket connectionObject = socketMap.get(peer);
                                boolean isChoked = connectionObject.getIsChoked(), isOpUnchoked = connectionObject.isOptimisticallyUnchoked();
                                if (!isChoked && !isOpUnchoked) {
                                    connectionObject.chokeConnection();
                                    connectionObject.sendChokeMessage();
                                }
                                i++;
                            }
                            System.out.println("Peer "+ currentPeerId +" has the preferred neighbors "+ prefNeighbors);
                            logger.logInfo("Peer "+ currentPeerId +" has the preferred neighbors "+ prefNeighbors );
                        }
                    }
                }
                else {
                    System.out.println("File does not exist in this peer.");
                    List<Integer> peersInterested = getPeers_DownloadRate(conns);
                    if (peersInterested.size() <= configuration.getNumberOfPreferredNeighbors()) {
                        int i=0;
                        while(i < peersInterested.size()) {
                            int peer = peersInterested.get(i);
                            PeerSocket connectionObject = socketMap.get(peer);
                            if (connectionObject.getIsChoked() == true) {
                                System.out.println("Sending chunks of existing file");
                                connectionObject.unChoke();
                                connectionObject.sendUnChokeMessage();
                            }
                            i++;
                        }
                    }
                    else {
                        int[] preferredNeighbors = new int[configuration.getNumberOfPreferredNeighbors()];
                        HashMap<Integer,Double> m = new HashMap<>();
                        CustomComparator comp =  new CustomComparator(m);
                        TreeMap<Integer,Double> sortedMap = new TreeMap<>(comp);
                        for(int i=0; i<peersInterested.size(); i++){
                            m.put(peersInterested.get(i), socketMap.get(peersInterested.get(i)).getDownloadSpeed());
                        }
                        sortedMap.putAll(m);
                        List<Integer> sortedPeers = new ArrayList<Integer>();
                        sortedPeers.addAll(sortedMap.keySet());
                        int i = 0;
                        while(i< configuration.getNumberOfPreferredNeighbors()){
                            int peer= sortedPeers.get(i);
                            preferredNeighbors[i]= peer;
                            PeerSocket neighborPeerSocket = socketMap.get(peer);
                            if(neighborPeerSocket.getIsChoked()){
                                neighborPeerSocket.unChoke();
                                neighborPeerSocket.sendUnChokeMessage();
                            }
                            peersInterested.remove(peer);
                            i++;
                        }
                        for(i=0;i<peersInterested.size();i++){
                            int peer = peersInterested.get(i);
                            PeerSocket tempConnection = socketMap.get(peer);
                            if( tempConnection.getIsChoked() == false && tempConnection.isOptimisticallyUnchoked() == false){
                                tempConnection.chokeConnection();
                                tempConnection.sendChokeMessage();
                            }
                        }
                        System.out.println("Preferred neighbours of node" + currentPeerId + " are " + preferredNeighbors);
                        logger.logInfo("Preferred neighbours of node" + currentPeerId + " are " + preferredNeighbors);
                    }

                }
                try {
                    Thread.sleep(configuration.getUnchokingInterval()*1000);
                }
                catch (Exception exception) {
                    exception.printStackTrace();
                }
            }
            System.exit(0);
        }
    }
}


class CustomException extends Exception {
    public CustomException(String s) {
        super(s);
    }
}