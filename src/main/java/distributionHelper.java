import com.jcraft.jsch.*;

import java.io.*;
import java.util.*;

public class distributionHelper {
    private static ChannelSftp sftpChannel;
    private static Session session;

    private static void createConnection() {
        try {
            JSch jsch = new JSch();
            String user = "tasneem_porbanderwala11";
            String host = "35.184.81.129";
            int port = 22;
            String privateKey = "C:\\Users\\Tasneem\\Desktop\\correct\\private-key.ppk";
            jsch.addIdentity(privateKey);
            session = jsch.getSession(user, host, port);
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.setInputStream(System.in);
            channel.setOutputStream(System.out);
            channel.connect();
            sftpChannel = (ChannelSftp) channel;
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    public static boolean createHelper(String tableName) {
        createConnection();
        try
        {
            sftpChannel.put(new ByteArrayInputStream("".getBytes()), ConfigurationSetup.getDataPathRemote()+"/"+tableName+".txt");
        }
        catch (SftpException e)
        {
            e.printStackTrace();
            disconnectSession();
            return false;
        }
        disconnectSession();
        return true;
    }

    public static void disconnectSession() {
        sftpChannel.exit();
        sftpChannel.disconnect();
        session.disconnect();
    }

    public static HashMap<String, MetaData> readRemoteMetadata() throws SftpException, IOException {
        createConnection();
        HashMap<String, MetaData> table = new HashMap<>();
        BufferedReader br;
        InputStream stream = sftpChannel.get(ConfigurationSetup.getDataPathRemote()+"/tableMetaData.txt");
        br = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ((line = br.readLine()) != null) {
            String[] lineParts = line.split("\\^");
            if (lineParts.length > 1) {
                MetaData m = new MetaData();

                m.setNumberOfColumns(Integer.parseInt(lineParts[1]));
                m.setTableName(lineParts[0]);
                LinkedHashMap<String, String> columns = new LinkedHashMap<>();
                for (int i = 2; i < lineParts.length; i = i + 2) {
                    if (lineParts[i].equals("PK")) {
                        i++;
                        m.setPrimaryKeyColumn(lineParts[i]);
                        m.setPrimaryKeyDataType(lineParts[i + 1]);
                        columns.put(lineParts[i], lineParts[i + 1]);
                    } else if (lineParts[i].equals("FK")) {
                        i++;
                        m.setForeignkeyColumn(lineParts[i]);
                        m.setForeignkeyDataType(lineParts[i + 1]);
                        m.setReferencedTable(lineParts[i + 2]);
                        m.setReferencedColumnName(lineParts[i + 3]);
                        columns.put(lineParts[i], lineParts[i + 1]);
                    } else {
                        columns.put(lineParts[i], lineParts[i + 1]);
                    }
                }
                m.setColumns(columns);
                table.put(lineParts[0], m);
            }
        }
        stream.close();
        disconnectSession();
        return table;
    }

    public static void writeMetadata(StringBuilder value) throws SftpException {
        createConnection();
        sftpChannel.put(new ByteArrayInputStream(value.toString().getBytes()), ConfigurationSetup.getDataPathRemote()+"/tableMetaData.txt", ChannelSftp.APPEND);
        sftpChannel.put(new ByteArrayInputStream("\n".getBytes()), ConfigurationSetup.getDataPathRemote()+"/tableMetaData.txt", ChannelSftp.APPEND);
        disconnectSession();
    }

    public static void writeGDD(HashMap<String, List<String>> gddRemote) throws SftpException, IOException {
        createConnection();

        File gddFileTemp = new File(ConfigurationSetup.getDataPathLocal()+"/gddtemp.txt");
        FileWriter fw = new FileWriter(gddFileTemp);
        BufferedWriter writer = new BufferedWriter(fw);

        for(String location: gddRemote.keySet())
        {
            StringBuilder value = new StringBuilder();
            value.append(location).append("^");
            List<String> tables=gddRemote.get(location);
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
        sftpChannel.put(gddFileTemp.getAbsolutePath(),ConfigurationSetup.getDataPathRemote()+"/gdd.txt",ChannelSftp.OVERWRITE);
        gddFileTemp.delete();
        disconnectSession();
    }

    public static void writeLog(String log) throws SftpException {
        createConnection();
        sftpChannel.put(new ByteArrayInputStream(log.getBytes()), ConfigurationSetup.getDataPathRemote()+"/eventlog.txt", ChannelSftp.APPEND);
        sftpChannel.put(new ByteArrayInputStream("\n".getBytes()), ConfigurationSetup.getDataPathRemote()+"/eventlog.txt", ChannelSftp.APPEND);
        disconnectSession();
    }

    public static void writeSQLDump(File file) throws SftpException {
        createConnection();
        sftpChannel.put(file.getAbsolutePath(),ConfigurationSetup.getDataPathRemote()+"/sqldump.txt",ChannelSftp.OVERWRITE);
        disconnectSession();
    }

    public static boolean checkTableFile(String tableName)
    {
        createConnection();
        try {
            sftpChannel.lstat(ConfigurationSetup.getDataPathRemote()+"/"+tableName+".txt");
        } catch (SftpException e) {
            System.out.println("check table file catch");
            e.printStackTrace();
            disconnectSession();
            return false;
        }
        disconnectSession();
        return true;
    }

    public static LinkedHashMap<String, ArrayList<String>> readTableDataRemote(String tableName, MetaData m) throws SftpException, IOException {
        createConnection();
        InputStream ir = sftpChannel.get(ConfigurationSetup.getDataPathRemote()+"/"+tableName+".txt");
        BufferedReader br = null;
        LinkedHashMap<String,ArrayList<String>> table=new LinkedHashMap<>();
        br = new BufferedReader(new InputStreamReader(ir));

        String line;
        LinkedHashMap<String,String> columns=m.getColumns();
        String[] columnNames= columns.keySet().toArray(new String[0]);
        int i=0;

        while((line=br.readLine())!=null)
        {
            if(!line.equals("\n")) {
                String[] lineParts = line.split("\\^");
                ArrayList<String> columnValues = new ArrayList<>(Arrays.asList(lineParts));
                table.put(columnNames[i], columnValues);
                i++;
            }
        }
        if(i==0)
        {
            for(int j=0;j<m.getNumberOfColumns();j++)
            {
                table.put(columnNames[j],new ArrayList<>());
            }
        }
        ir.close();
        disconnectSession();
        return table;
    }

    public static void writeTableDataRemote(LinkedHashMap<String, ArrayList<String>> table, String tableName) throws SftpException, IOException {
        createConnection();
        File tempfile = new File(ConfigurationSetup.getDataPathLocal()+"/"+tableName+".txt");
        OutputStream or = new FileOutputStream(tempfile);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(or));

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
                writer.close();
                sftpChannel.put(tempfile.getAbsolutePath(), ConfigurationSetup.getDataPathRemote()+"/"+tableName+".txt");
                tempfile.delete();
                disconnectSession();
                return;
            }
            value.deleteCharAt(lastIndex-1);
            writer.write(String.valueOf(value));
            writer.newLine();
        }
        writer.close();
        sftpChannel.put(tempfile.getAbsolutePath(), ConfigurationSetup.getDataPathRemote()+"/"+tableName+".txt");
        tempfile.delete();
        disconnectSession();
    }

    public static boolean deleteFile(String tableName)
    {
        createConnection();
        try {
            sftpChannel.rm(ConfigurationSetup.getDataPathRemote()+"/"+tableName+".txt");

        } catch (SftpException e) {
            e.printStackTrace();
            disconnectSession();
            return false;
        }
        disconnectSession();
        return true;
    }

    public static void deleteMetaData(File tempfile, String tablename) throws SftpException, IOException {
        createConnection();
        InputStream ir;
        BufferedReader br;
        FileWriter fileWriter;
        BufferedWriter writer;
        ir = sftpChannel.get(ConfigurationSetup.getDataPathRemote()+"/tableMetaData.txt");
        br = new BufferedReader(new InputStreamReader(ir));
        fileWriter = new FileWriter(tempfile, true);
        writer = new BufferedWriter(fileWriter);
        String line;
        while ((line = br.readLine()) != null) {
            String[] words = line.split("\\^");
            if (words[0].equals(tablename)) {
                continue;
            }
            writer.write(line);
            writer.newLine();
        }
        writer.close();
        fileWriter.close();
        br.close();
        sftpChannel.put(tempfile.getAbsolutePath(),ConfigurationSetup.getDataPathRemote()+"/tableMetaData.txt",ChannelSftp.OVERWRITE);
        tempfile.delete();
        disconnectSession();
    }
}
