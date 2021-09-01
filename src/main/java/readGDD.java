import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class readGDD {
    public static void readgdd() throws IOException {
        File file = new File(ConfigurationSetup.getDataPathLocal() + "/gdd.txt");
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        while ((line = br.readLine()) != null) {
            String[] lineParts = line.split("\\^");
            HashMap<String, List<String>> gdd = GDD.getInstance();
            if (lineParts[0].equalsIgnoreCase("local")) {
                //line = line.replace("local^", "");
                gdd.put("local", new ArrayList<>());
                if(lineParts.length>1)
                {
                    for(int i=1;i< lineParts.length;i++)
                    {
                        gdd.get("local").add(lineParts[i]);
                    }
                }
            }
            if (lineParts[0].equalsIgnoreCase("remote")) {
                gdd.put("remote", new ArrayList<>());
                if(lineParts.length>1)
                {
                    for(int i=1;i< lineParts.length;i++)
                    {
                        gdd.get("remote").add(lineParts[i]);
                    }
                }
            }
        }
    }
}
