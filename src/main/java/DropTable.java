import com.jcraft.jsch.SftpException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class DropTable {
    public void deleteFile(String tableName, String query) throws IOException, SftpException {
        readGDD.readgdd();
        HashMap<String, List<String>> gdd = GDD.getInstance();
        readMetaData readmetaData = new readMetaData();
        String dataPath;
        boolean isLocal;
        OutputStream or;
        BufferedWriter writer = null;
        InputStream ir = null;
        BufferedReader br = null;
        if(gdd.get("local").contains(tableName))
        {
            isLocal=true;
            dataPath = ConfigurationSetup.getDataPathLocal();
            List<String> tablesReferenced = readmetaData.checkIfTableIsReferenced(tableName,isLocal);
            if(tablesReferenced.size()==0) {
                gdd.get("local").remove(tableName);
                writeGDD.write(gdd);
//                DropTable dropTable = new DropTable();
//                dropTable.deleteFile(tableName, query);
            }
            else
            {
                System.out.println("Table cannot be dropped.");
                return;
            }
        }
        else if (gdd.get("remote").contains(tableName))
        {
            isLocal=false;
            dataPath = ConfigurationSetup.getDataPathRemote();
            List<String> tablesReferenced = readmetaData.checkIfTableIsReferenced(tableName, isLocal);
            if(tablesReferenced.size()==0)
            {
                DropTable dropTable = new DropTable();
                gdd.get("remote").remove(tableName);
                writeGDD.write(gdd);
                //dropTable.deleteFile(tableName,sql, ConfigurationSetup.getDataPathRemote());
            }
            else
            {
                System.out.println("Table cannot be dropped.");
                return;
            }
        }
        else{
            System.out.println("table does not exists");
            return;
        }

        File file = new File(dataPath+"\\"+tableName+".txt");
        String log = "";
        EventLog eventLog = new EventLog();
        SqlDump sqlDump = new SqlDump();
        boolean isDelete;
        if(ConfigurationSetup.getAutoCommit())
        {
            if(isLocal)
            {
                isDelete=file.delete();
            }
            else
            {
                isDelete = distributionHelper.deleteFile(tableName);
            }

            DeleteMetadata deleteMetadata = new DeleteMetadata();
            deleteMetadata.removeMetadata(tableName,dataPath,isLocal);
            if(isDelete) {
                System.out.println("Table is deleted");
                log += "drop^";
                log += tableName + "^";
                String timeStamp = new SimpleDateFormat("yyyy-MM-dd^HH:mm:ss").format(new Date());
                log += timeStamp;
                sqlDump.toDump(query, dataPath);
                eventLog.addLog(log, isLocal);
            }
        }
        else if(file.exists() && !ConfigurationSetup.getAutoCommit())
        {
            TransactionHelper.writeToTransactionFile(query,tableName);
        }
        else{
            System.out.println("Table does not exists");
        }
    }
}
