import java.io.*;
import java.util.*;

public class readTable
{
    public boolean checkTableFile(boolean isLocal,String tableName)
    {
        if(isLocal)
        {
        File dir = new File(ConfigurationSetup.getDataPathLocal());
        String temp = tableName + ".txt";
        return new File(dir,temp).exists();
        }
        else
        {
            return distributionHelper.checkTableFile(tableName);
        }

    }

    public LinkedHashMap<String, ArrayList<String>> readTableData(String tableName, boolean isLocal) throws Exception
    {
        String dataPath;
        File file;
        InputStream ir = null;
        BufferedReader br = null;
        LinkedHashMap<String,ArrayList<String>> table=new LinkedHashMap<>();
        readMetaData rm=new readMetaData();
        MetaData m;
        HashMap<String, MetaData> metaData=rm.readmetaData(isLocal);
        if(metaData.containsKey(tableName))
        {
            m = metaData.get(tableName);
        }
        else
        {
            throw new Exception("Table does not exist.");
        }
        try
        {
            if (isLocal)
            {
                dataPath = ConfigurationSetup.getDataPathLocal();
                file = new File(dataPath+"/"+tableName+".txt");
                ir = new FileInputStream(file);
                br = new BufferedReader(new InputStreamReader(ir));
            }
            else
            {
                dataPath = ConfigurationSetup.getDataPathRemote();
                file = new File(dataPath+"/"+tableName+".txt");
                table=distributionHelper.readTableDataRemote(tableName,m);
                return table;
            }

            String line;
            LinkedHashMap<String,String> columns=m.getColumns();
            String[] columnNames= columns.keySet().toArray(new String[0]);
            int i=0;

            while((line=br.readLine())!=null)
            {

                String[] lineParts=line.split("\\^");
                ArrayList<String> columnValues= new ArrayList<>(Arrays.asList(lineParts));
                table.put(columnNames[i],columnValues);
                i++;
            }
            if(i==0)
            {
                for(int j=0;j<m.getNumberOfColumns();j++)
                {
                    table.put(columnNames[j],new ArrayList<>());
                }
            }
            ir.close();
            return table;
        }
        catch(IOException e) {
            e.printStackTrace();
        }
        System.out.println(table);
        return table;
    }
}
