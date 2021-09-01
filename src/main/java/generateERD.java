import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class generateERD {

    private static String local_dir="";
    private static String remote_dir="";
    private static File myObj=null;
    private static FileWriter myWriter = null;
    private readMetaData reader;
    private readTable readerTable;

    public generateERD()
    {
       local_dir=ConfigurationSetup.getDataPathLocal();
       remote_dir=ConfigurationSetup.getDataPathRemote();
        reader = new readMetaData();
        readerTable = new readTable();
    }

    public boolean generateERDOfDB(String url, String fileName)
    {
        boolean result = false;
        HashMap<String, MetaData> tablesInLocalDir =reader.readmetaData(true);
        HashMap<String, MetaData> tablesInRemoteDir =reader.readmetaData(false);
        if(tablesInLocalDir.size()>0)
        {
            try {
                myObj = new File(url+fileName);
                if (myObj.createNewFile()) {
                    System.out.println("File created: " + myObj.getName());
                    myWriter = new FileWriter(url+fileName);
                    if(tablesInLocalDir.size()>0){
                        result = createERDForTable(tablesInLocalDir,true);
                    }
                    if(result == true && tablesInRemoteDir.size()>0)
                    {
                        result = createERDForTable(tablesInRemoteDir,false);
                    }
                    myWriter.close();
                } else {
                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred while creating file please give correct output");
                e.printStackTrace();
                return result;
            }
        }

        return result;
    }

    private boolean createERDForTable(HashMap<String, MetaData> tables, boolean isLocal)
    {
        boolean result = false;
        try {
            for(String tableName : tables.keySet())
            {
                MetaData tableMetaData = reader.readmetaData(isLocal).get(tableName);
                LinkedHashMap<String, String> columns = tableMetaData.getColumns();
                List<String> othertableNames = reader.checkIfTableIsReferenced(tableName,isLocal);
                String primaryKeyColumnName =tableMetaData.getPrimaryKeyColumn();
                String foreignKeyColumnName = tableMetaData.getForeignkeyColumn();
                String getForeignKeyReferenceTableName=null;
                String getForeignKeyReferencecolumnName=null;

                if(foreignKeyColumnName != null)
                {
                    getForeignKeyReferenceTableName = tableMetaData.getReferencedTable();
                    getForeignKeyReferencecolumnName = tableMetaData.getReferencedColumnName();
                }
                if(columns.size()>0)
                {
                    myWriter.write("\n\n"+"TableName : "+tableName);
                    myWriter.write("\n"+"column_Name | column_DataType");
                    for(String columnName : columns.keySet())
                    {
                        myWriter.write("\n"+columnName+" | "+"["+columns.get(columnName)+"]");
                    }
                    myWriter.write("\n"+"Indexes:");
                    if(primaryKeyColumnName!=null){
                        myWriter.write("\n"+primaryKeyColumnName+"_CurrentTable_PK");
                    }
                    if(foreignKeyColumnName != null) {
                        myWriter.write("\n" + foreignKeyColumnName + "_" + "CurrentTable_FK" + "_" + getForeignKeyReferenceTableName + "_" + getForeignKeyReferencecolumnName);
                        myWriter.write("\n" + "Cardinality_" + getForeignKeyReferenceTableName + "_many-to-one");
                    }
                    for(int i=0;i<othertableNames.size();i++)
                    {
                        myWriter.write("\n"+"Cardinality_"+othertableNames.get(i)+"_one-to-many");
                    }
                    myWriter.write("\n************************************");
                }
                result = true;
            }

        } catch (Exception e) {
            result = false;
            e.printStackTrace();
        }
    return result;
    }
}
