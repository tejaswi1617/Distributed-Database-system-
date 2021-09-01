import com.jcraft.jsch.SftpException;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class writeGDD
{
    public static void writegdd(String tableName, boolean isLocal) throws IOException, SftpException {

        HashMap<String, List<String>> gdd = GDD.getInstance();
        HashMap<String, List<String>> gddRemote = GDD.getInstance();

        if(isLocal)
        {
            gdd.get("local").add(tableName);
        }
        else
        {
            gdd.get("remote").add(tableName);
        }

        File gddFile = new File(ConfigurationSetup.getDataPathLocal()+"/gdd.txt");
        FileWriter fw = new FileWriter(gddFile);
        BufferedWriter writer = new BufferedWriter(fw);

        for(String location: gdd.keySet())
        {
            StringBuilder value = new StringBuilder();
            value.append(location).append("^");
            List<String> tables=gdd.get(location);
            int lastIndex=location.length()+1;
            for(String table:tables)
            {
                value.append(table).append("^");
                lastIndex+=table.length()+1;
            }
            if(tables.size()==0)
            {
                writer.write(String.valueOf(value));
                writer.newLine();
                continue;
            }
            value.deleteCharAt(lastIndex-1);
            writer.write(String.valueOf(value));
            writer.newLine();
        }
        writer.close();
        readGDD.readgdd();
        gddRemote=GDD.getInstance();
        gddRemote.put("temp",gdd.get("local"));
        gddRemote.put("local",gdd.get("remote"));
        gddRemote.put("remote",gdd.get("temp"));
        gddRemote.remove("temp");
        distributionHelper.writeGDD(gddRemote);
    }

    public static void write(HashMap<String, List<String>> gdd) throws IOException, SftpException {
        File gddFile = new File(ConfigurationSetup.getDataPathLocal()+"/gdd.txt");
        HashMap<String, List<String>> gddRemote;
        FileWriter fw = new FileWriter(gddFile);
        BufferedWriter writer = new BufferedWriter(fw);

        for(String location: gdd.keySet())
        {
            StringBuilder value = new StringBuilder();
            value.append(location).append("^");
            List<String> tables=gdd.get(location);
            int lastIndex=location.length()+1;
            for(String table:tables)
            {
                value.append(table).append("^");
                lastIndex+=table.length()+1;
            }
            if(tables.size()==0)
            {
                writer.write(String.valueOf(value));
                writer.newLine();
                continue;
            }
            value.deleteCharAt(lastIndex-1);
            writer.write(String.valueOf(value));
            writer.newLine();
        }
        writer.close();
        readGDD.readgdd();
        gddRemote=GDD.getInstance();
        gddRemote.put("temp",gdd.get("local"));
        gddRemote.put("local",gdd.get("remote"));
        gddRemote.put("remote",gdd.get("temp"));
        gddRemote.remove("temp");
        distributionHelper.writeGDD(gddRemote);
    }
}
