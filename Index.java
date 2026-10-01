import java.util.ArrayList;
import java.util.HashMap;

public class Index {

    public final static String s_stored = "STORED";
    public final static String s_storing = "STORING";
    public final static String s_removing = "REMOVE IN PROGRESS";
    public final static String s_removed = "REMOVED";

    private final ArrayList<String> fileNames;
    private final HashMap<String,String> fileStatus;
    private final HashMap<String,Integer> fileSizes;

    public Index(ArrayList<String> fileNames, HashMap<String, String> fileStatus, HashMap<String, Integer> fileSizes) {
        this.fileNames = fileNames;
        this.fileStatus = fileStatus;
        this.fileSizes = fileSizes;
    }

    public ArrayList<String> getFileNames() {
        return fileNames;
    }

    public HashMap<String, String> getFileStatus() {
        return fileStatus;
    }
    public HashMap<String, Integer> getFileSizes() {
        return fileSizes;
    }

    public void reset() {
        this.fileNames.clear();
        this.fileStatus.clear();
        this.fileSizes.clear();
    }

}
