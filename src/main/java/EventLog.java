import com.jcraft.jsch.SftpException;

import java.io.*;

public class EventLog {
    public void addLog(String log, boolean isLocal) throws IOException, SftpException {

        String dataPath;
        File file;
        OutputStream or;
        BufferedWriter writer;
        PrintWriter pr;
        if (isLocal)
        {
            dataPath = ConfigurationSetup.getDataPathLocal();
            file = new File(dataPath + "/eventlog.txt");
            or = new FileOutputStream(file,true);
            writer = new BufferedWriter(new OutputStreamWriter(or));
            pr = new PrintWriter(writer);
            pr.println(log);
            pr.close();
            writer.close();
        }
        else
        {
            distributionHelper.writeLog(log);
        }
    }
}
