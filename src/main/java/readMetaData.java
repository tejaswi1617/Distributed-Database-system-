import java.io.*;
import java.util.*;

public class readMetaData {
    public HashMap<String, MetaData> readmetaData(boolean isLocal) {
        String dataPath;
        File file;
        InputStream ir = null;
        BufferedReader br = null;
        HashMap<String, MetaData> table = new HashMap<>();
        try {
            if (isLocal)
            {
                dataPath = ConfigurationSetup.getDataPathLocal();
                file = new File(dataPath + "/tableMetaData.txt");
                ir = new FileInputStream(file);
                br = new BufferedReader(new InputStreamReader(ir));
            }
            else
            {
                table=distributionHelper.readRemoteMetadata();
                return table;
            }
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
            ir.close();
        } catch (IOException fileNotFoundException) {
            fileNotFoundException.printStackTrace();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return table;

    }

    public List<String> checkIfTableIsReferenced(String tablename, boolean isLocal) {
        boolean result = false;
        List<String> tableNames = new ArrayList<>();
        HashMap<String, MetaData> metadataHashMap = readmetaData(isLocal);
        for (MetaData metaData : metadataHashMap.values()) {
            if (metaData.getReferencedTable().equals(tablename)) {
                tableNames.add(metaData.getTableName());
            }
        }
        return tableNames;
    }
}
