import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class CreateTable
{
    public void createTable(CreateQueryParse parser, boolean isLocal)
    {
        String dataPath;
        MetaData m = new MetaData();
        if(isLocal)
        {
            dataPath=ConfigurationSetup.getDataPathLocal();
        }
        else
        {
            dataPath=ConfigurationSetup.getDataPathRemote();
        }
        try{
            File table = new File(dataPath+"/"+parser.getTableName()+".txt");
            String log="";
            EventLog eventLog = new EventLog();
            SqlDump sqlDump = new SqlDump();
            if(!parser.getForeignKeyColumn().isEmpty()) {
                if (!checkForeignKey(parser,isLocal)) {
                    throw new Exception("Foreign key not valid");
                }
                else
                {
                    m.setForeignkeyColumn(parser.getForeignKeyColumn());
                    m.setForeignkeyDataType(parser.getForeignKeyDataType());
                    m.setReferencedTable(parser.getReferencedTable());
                    m.setReferencedColumnName(parser.getReferencedColumnName());
                }
            }
            if(!checkTableExists(parser.getTableName()))
            {
                if(ConfigurationSetup.getAutoCommit())
                {
                    boolean isCreated = false;
                    if(isLocal) {
                        isCreated = table.createNewFile();
                    }
                    else
                    {
                        isCreated = distributionHelper.createHelper(parser.getTableName());
                    }
                    if(isCreated)
                    {

                        System.out.println("Table Created");
                        m.setNumberOfColumns(parser.getColumns().size());
                        m.setColumns(parser.getColumns());
                        m.setTableName(parser.getTableName());
                        m.setPrimaryKeyColumn(parser.getPrimaryKeyColumn());
                        m.setPrimaryKeyDataType(parser.getPrimaryKeyDataType());
                        writeMetaData.writeMetaData(m, isLocal);
                        writeGDD.writegdd(m.getTableName(), isLocal);
                        //eventlog and sqldump
                        log += "create^";
                        log += parser.getTableName() + "^";
                        String timeStamp = new SimpleDateFormat("yyyy-MM-dd^HH:mm:ss").format(new Date());
                        log += timeStamp;
                        eventLog.addLog(log, isLocal);
                        sqlDump.toDump(parser.getQuery(), dataPath);
                    }
                }
                else
                {
                    TransactionHelper.writeToTransactionFile(parser.getQuery(),parser.getTableName());
                }
            }
            else{
                System.out.println("Table already exists");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private boolean checkTableExists(String tableName) throws IOException {
        boolean result=false;
        readGDD.readgdd();
        HashMap<String, List<String>> gdd=GDD.getInstance();
        for(List<String> tableNames : gdd.values())
        {
            if(tableNames.contains(tableName))
            {
                result=true;
            }
        }
        return result;
    }

    private boolean checkForeignKey(CreateQueryParse parse, boolean isLocal)
    {
        boolean result=false;
        readMetaData reader=new readMetaData();
            MetaData metaData = reader.readmetaData(isLocal).get(parse.getReferencedTable());
            if (metaData != null) {
                String primaryDataType = metaData.getPrimaryKeyDataType();
                if (primaryDataType.equals(parse.getForeignKeyDataType())) {
                    result = true;
                }
            }
        return result;
    }

}
