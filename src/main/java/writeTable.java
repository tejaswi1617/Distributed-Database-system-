import com.jcraft.jsch.SftpException;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class writeTable
{
    public static void writeTable(String tableName, LinkedHashMap<String, ArrayList<String>> table, boolean isLocal)
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
                file = new File(dataPath+"/"+tableName+".txt");
                or = new FileOutputStream(file);
                writer = new BufferedWriter(new OutputStreamWriter(or));
            }
            else
            {
                dataPath = ConfigurationSetup.getDataPathRemote();
                file = new File(dataPath+"/"+tableName+".txt");
                distributionHelper.writeTableDataRemote(table,tableName);
                return;
            }


            for(String col:table.keySet())
            {
              StringBuilder value= new StringBuilder();
              ArrayList<String> columnValues=table.get(col);
              int lastIndex=0;
              for(String columnValue:columnValues)
              {
                  value.append(columnValue).append("^");
                  lastIndex+=columnValue.length()+1;
              }
              if(lastIndex==0)
              {
                  writer.write(String.valueOf(value));
                  writer.newLine();
                  return;
              }
              value.deleteCharAt(lastIndex-1);
                writer.write(String.valueOf(value));
                writer.newLine();

            }
            assert writer != null;
            writer.close();
        } catch (IOException | SftpException e) {
            System.out.println("An error occurred.");
        }
    }
}
