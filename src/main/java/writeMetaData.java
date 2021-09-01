import com.jcraft.jsch.SftpException;

import java.io.*;
import java.util.Set;

public class writeMetaData
{
    public static void writeMetaData(MetaData metaData, boolean isLocal)
    {
        String dataPath;
        File file;
        OutputStream or;
        BufferedWriter writer = null;
        InputStream ir = null;
        BufferedReader br = null;
        try {
            if (isLocal)
            {
                dataPath = ConfigurationSetup.getDataPathLocal();
                file = new File(dataPath + "/tableMetaData.txt");
                or = new FileOutputStream(file,true);
                writer = new BufferedWriter(new OutputStreamWriter(or));
            }
            else
            {
                dataPath = ConfigurationSetup.getDataPathRemote();
                file = new File(dataPath + "/tableMetaData.txt");
            }
            StringBuilder value= new StringBuilder(metaData.getTableName() + "^" + metaData.getNumberOfColumns());
            Set<String> keys=metaData.getColumns().keySet();
            for(String col:keys)
            {
                value.append("^").append(col).append("^").append(metaData.getColumns().get(col));
            }
            value.append("^PK^").append(metaData.getPrimaryKeyColumn()).append("^").append(metaData.getPrimaryKeyDataType());
            if(metaData.getForeignkeyColumn()!=null)
            {
                value.append("^FK^").append(metaData.getForeignkeyColumn()).append("^").append(metaData.getForeignkeyDataType()).append("^").append(metaData.getReferencedTable()).append("^").append(metaData.getReferencedColumnName());
            }
            if (isLocal) {
                writer.write(value.toString());
                writer.newLine();
                writer.close();
            }
            else
            {
                distributionHelper.writeMetadata(value);
            }
        } catch (IOException | SftpException e) {
            System.out.println("An error occurred.");
        }
    }
}
