import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

import java.io.*;

public class ConfigurationSetup
{
    private static String dataPathLocal;
    private static String dataPathRemote;
    private static String rootPassword;
    private static boolean autoCommit;

    public static void setupConfiguration() throws IOException
    {
        String currentPath =System.getProperty("user.dir");
        File dataDirectory=new File(currentPath +"/data");
        //File dataDirectoryRemote=new File(currentPath +"/dataRemote");
        if(dataDirectory.mkdir())
        {
            System.out.println("data paths created successfully");
            dataPathLocal = dataDirectory.getAbsolutePath();
            //dataPathRemote = dataDirectoryRemote.getAbsolutePath();
            System.out.println("Enter root user password");
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(System.in));
            rootPassword=reader.readLine();
            createMetadataFile();
        }
    }

    private static void createMetadataFile() throws IOException
    {
        File metaDataFileTable = new File(dataPathLocal +"/tableMetaData.txt");
        //File metaDataFileTableRemote = new File(dataPathRemote +"/tableMetaData.txt");
        metaDataFileTable.createNewFile();
        //metaDataFileTableRemote.createNewFile();
        File gddFile = new File(dataPathLocal +"/gdd.txt");
        gddFile.createNewFile();
        File metaDataFileUser = new File(dataPathLocal +"/userMetaData.txt");
        metaDataFileUser.createNewFile();
        FileWriter writer = new FileWriter(dataPathLocal +"/userMetaData.txt");
        writer.write("username^password\n");
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        rootPassword = encoder.encode(rootPassword);
        writer.write("root^"+rootPassword+"\n");
        writer.close();
        writer = new FileWriter(dataPathLocal +"/gdd.txt");
        writer.write("local^");
        writer.write("\n");
        writer.write("remote^");
        writer.close();
    }

    public static String getDataPathLocal()
    {
        return System.getProperty("user.dir")+"/data";
    }

    public static String getDataPathRemote() { return "/home/tasneem_porbanderwala11/data";}

    public static void setAutoCommitToTrue()
    {
        System.out.println("Setting autocommit to True..");
        autoCommit=true;
    }

    public static void setAutoCommitToFalse()
    {
        System.out.println("Setting autocommit to False..");
        autoCommit=false;
    }

    public static boolean getAutoCommit()
    {
        return autoCommit;
    }
}
