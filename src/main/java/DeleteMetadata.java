import com.jcraft.jsch.SftpException;

import java.io.*;

public class DeleteMetadata {
    public void removeMetadata(String tablename, String dataPath, boolean isLocal) {
        File file;
        File metafile;
        InputStream ir = null;
        BufferedReader br = null;
        FileWriter fileWriter = null;
        BufferedWriter writer = null;
        try {
            if (isLocal) {
                dataPath = ConfigurationSetup.getDataPathLocal();
                file = new File(dataPath + "/" + "" + "tempFile.txt");
                metafile = new File(dataPath + "/" + "" + "tableMetaData.txt");
                ir = new FileInputStream(metafile);
                br = new BufferedReader(new InputStreamReader(ir));
                fileWriter = new FileWriter(file, true);
                writer = new BufferedWriter(fileWriter);
            } else {
                file = new File(ConfigurationSetup.getDataPathLocal() + "/" + "" + "tempFile.txt");
                distributionHelper.deleteMetaData(file,tablename);
                return;
            }

            String line = null;
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
            if (isLocal)
            {
                metafile.delete();
                file.renameTo(metafile);
            }
        } catch (IOException | SftpException e) {
            e.printStackTrace();
        }
    }
}
