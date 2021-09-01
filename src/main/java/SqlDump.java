import com.jcraft.jsch.SftpException;

import java.io.*;

public class SqlDump {
    public void toDump(String query, String dataPath) throws IOException {
        String tableName = null,querytype = null;
        String[] words = query.split(" ");
        querytype = words[0];
        if (querytype.equals("drop")) {
            tableName = words[2];
            tableName = tableName.substring(0,tableName.length()-1);
        }
        try{
            if(querytype.equals("drop"))
            {
                File file = new File(ConfigurationSetup.getDataPathLocal()+"/"+""+"sqldump.txt");
                File tempfile = new File(ConfigurationSetup.getDataPathLocal()+"/"+""+"tempDump.txt");
                BufferedReader br = new BufferedReader(new FileReader(file));
                FileWriter fileWriter = new FileWriter(tempfile, true);
                BufferedWriter writer = new BufferedWriter(fileWriter);
                String line = null;
                while((line = br.readLine())!=null) {
                    String[] wordsIndrop = line.split(" ");
                    if(wordsIndrop[0].equals("insert") && wordsIndrop[2].equals(tableName))
                    {
                        continue;
                    }
                    else if(wordsIndrop[1].equals("create") && wordsIndrop[3].equals(tableName)){
                        continue;
                    }
                    else if(wordsIndrop[0].equals("delete") && wordsIndrop[2].equals(tableName)){
                        continue;
                    }
                    else if (wordsIndrop[0].equals("update") && wordsIndrop[1].equals(tableName)){
                        continue;
                    }
                    writer.write(line);
                    writer.newLine();

                }
                writer.close();
                fileWriter.close();
                br.close();
                file.delete();
                tempfile.renameTo(file);
                distributionHelper.writeSQLDump(file);
            }
            else{
                File file = new File(ConfigurationSetup.getDataPathLocal()+"/"+""+"sqldump.txt");
                FileWriter fr = new FileWriter(file, true);
                BufferedWriter br = new BufferedWriter(fr);
                PrintWriter pr = new PrintWriter(br);
                pr.println(query);
                pr.close();
                br.close();
                fr.close();
                distributionHelper.writeSQLDump(file);
            }
        }catch (IOException | SftpException e)
        {
            e.printStackTrace();
        }
    }
}
