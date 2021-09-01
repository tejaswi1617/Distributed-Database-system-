import java.text.SimpleDateFormat;
import java.util.*;

public class insertHelper
{
    private static String dataPath;
    private static String tableName;
    private insertParser insertparser;
    private String primaryKeyColumnName=null;
    private String foreignKeyColumnName =null;
    private String getForeignKeyReferenceTableName= null;
    private String getForeignKeyReferencecolumnName = null;
    private LinkedHashMap<String,ArrayList<String>> tableData;
    readTable readerTable;
    private boolean isLocal;
    public insertHelper()
    {
        tableName="";
        tableData = new LinkedHashMap<String,ArrayList<String>>();
        readerTable = new readTable();
        insertparser = new insertParser();
    }
    public boolean insertHelperClass(String insertquery) throws Exception {
        boolean result = false;
        readGDD.readgdd();
        HashMap<String, List<String>> gdd= GDD.getInstance();
        String log="";
        EventLog eventLog = new EventLog();
        SqlDump sqlDump = new SqlDump();

        if(insertquery.toLowerCase(Locale.ROOT).contains("values"))
        {
            String[] data = insertquery.toLowerCase().split("values");
            if(data.length == 2)
            {
                String[] getTableName = data[0].split(" ");
                if(getTableName.length==3){
                    tableName = getTableName[2].toLowerCase();
                }
                else{
                    System.out.println("Incomplete insert query. Please write correct query");
                    return result;
                }
                if(ConfigurationSetup.getAutoCommit() && TransactionHelper.checkfortablelocked(tableName)==true){
                    System.out.println("sorry"+tableName+" is used by another transaction");
                    return false;
                }
                if(gdd.get("local").contains(tableName)) {
                    dataPath = ConfigurationSetup.getDataPathLocal();
                    isLocal=true;
                }
                else
                {
                    dataPath=ConfigurationSetup.getDataPathRemote();
                    isLocal=false;

                }
                readMetaData reader = new readMetaData();
                MetaData tableMetaData = reader.readmetaData(isLocal).get(tableName);

                if ( reader.readmetaData(isLocal).containsKey(tableName))
                {
                    result = insertparser.insertQueryParserChecker(data,isLocal);

                    if(result == true)
                    {
                        tableData = readerTable.readTableData(tableName,isLocal);

                        primaryKeyColumnName = tableMetaData.getPrimaryKeyColumn();
                        foreignKeyColumnName = tableMetaData.getForeignkeyColumn();

                        if(foreignKeyColumnName != null)
                        {
                            getForeignKeyReferenceTableName = tableMetaData.getReferencedTable();
                            getForeignKeyReferencecolumnName = tableMetaData.getReferencedColumnName();
                        }
                        String values = data[1];
                        values = values.replace("(","");
                        values = values.replace(")","");
                        values = values.replace(";","");
                        values = values.replace("'","");
                        String[] colData = values.split(",");
                        result  = insertDataTrue(colData, isLocal);

                        if(result == true && ConfigurationSetup.getAutoCommit())
                        {
                            writeTable writeInFile = new writeTable();
                            writeTable.writeTable(tableName,tableData,isLocal);
                            //event log and sqldump
                            log+="insert^";
                            log+=tableName+"^";
                            for(int i=0;i<colData.length;i++)
                                log+=colData[i].toString()+"^";
                            String timeStamp = new SimpleDateFormat("yyyy-MM-dd^HH:mm:ss").format(new Date());
                            log+=timeStamp;
                            eventLog.addLog(log, isLocal);
                            sqlDump.toDump(insertquery,dataPath);
                        }
                        else if(result == true && !ConfigurationSetup.getAutoCommit())
                        {
                            TransactionHelper.writeToTransactionFile(insertquery,tableName);
                            System.out.println("data successfully inserted into "+tableName+" but transaction not yet commited");
                        }

                    }
                }
            }
        }
        if(result == false)
        {
            System.out.println("Invalid insert query. Please write correct query");
        }
        else
        {
            System.out.println("data successfully inserted into "+tableName);
        }
       return result;
    }
    private boolean insertDataTrue(String[] colData, boolean isLocal) throws Exception {
        int index = 0;
        for(String keys : tableData.keySet())
        {
            String value = colData[index].trim();


            if(primaryKeyColumnName.equals(keys) && primaryKeyColumnName != null)
            {
                if(primaryKeyColumnName.equals(keys)) {

                    if (tableData.get(keys).contains(value)) {
                        return false;
                    }
                }
            }
            if(foreignKeyColumnName != null && foreignKeyColumnName.equals(keys))
            {
                LinkedHashMap<String,ArrayList<String>> foreingKeyTableData = readerTable.readTableData(getForeignKeyReferenceTableName,isLocal);
                if(foreingKeyTableData.size()>0)
                {
                    if(!foreingKeyTableData.get(getForeignKeyReferencecolumnName).contains(value))
                    {
                        return false;
                    }
                }
            }
            tableData.get(keys).add(value);
            index++;
        }
        return true;
    }
}
