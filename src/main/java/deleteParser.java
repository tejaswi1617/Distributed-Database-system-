import java.text.SimpleDateFormat;
import java.util.*;

public class deleteParser
{
    private static String dataPath;
    String tableName;
    LinkedHashMap<String,ArrayList<String>> columns;
    LinkedHashMap<String, String> columntype;
    Map<String, ArrayList<String>> whereConditions;
    String primaryKeyColumnName;
    readMetaData reader;
    readTable readerTable;
    static HashMap<String, List<String>> gdd;
    boolean isLocal;

    public deleteParser()
    {
        tableName= null;
        primaryKeyColumnName=null;
        readerTable = new readTable();
        reader = new readMetaData();
        columns = new LinkedHashMap<String, ArrayList<String>>();
        columntype = new LinkedHashMap<String, String>();
        whereConditions = new HashMap<String,ArrayList<String>>();

    }

    public boolean deleteRecord(String query) throws Exception {
        boolean result = false;
        //dataPath = ConfigurationSetup.getDataPathLocal();
        String log="";
        String queryForlog =query;
        EventLog eventLog = new EventLog();
        SqlDump sqlDump = new SqlDump();
        readGDD.readgdd();
        gdd = GDD.getInstance();
        if(query.toLowerCase().contains("where") && query.toLowerCase().contains(";"))
        {
            query = query.replace(";", "");
            String[] splitString = query.toLowerCase().split("where");
            String[] databeforewhere = splitString[0].split(" ");
            if(databeforewhere.length==3)
            {
                tableName = databeforewhere[2].toLowerCase(Locale.ROOT);
            }
            else{
                System.out.println("Incompplete query. Please check you query statement");
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
            else if (gdd.get("remote").contains(tableName))
            {
                dataPath = ConfigurationSetup.getDataPathRemote();
                isLocal=false;
            }

            if ( reader.readmetaData(isLocal).containsKey(tableName))
            {
                columns = readerTable.readTableData(tableName,isLocal);
                result = checkbeforeWhere(databeforewhere);

                if(result == true)
                {
                    result = storeWhereCondition(splitString[1]);

                    if(result == true)
                    {
                        //readMetaData reader = new readMetaData();
                        MetaData tableMetaData = reader.readmetaData(isLocal).get(tableName);
                        primaryKeyColumnName = tableMetaData.getPrimaryKeyColumn();
                        columntype = tableMetaData.getColumns();
                        result = deleteData();
                    }
                }
            }

        }
        else if(query.toLowerCase().contains(";"))
        {
            query = query.replace(";", "");
            String[] splitString = query.split(" ");
            if(splitString.length!=3)
            {
                System.out.println("Invalid delete syntax");
                return false;
            }


            tableName = splitString[2].toLowerCase(Locale.ROOT);
            if(ConfigurationSetup.getAutoCommit() && TransactionHelper.checkfortablelocked(tableName)==true){
                System.out.println("sorry"+tableName+" is used by another transaction");
                return false;
            }
            if(gdd.get("local").contains(tableName)) {
                dataPath = ConfigurationSetup.getDataPathLocal();
                isLocal=true;
            }
            else if (gdd.get("remote").contains(tableName))
            {
                dataPath = ConfigurationSetup.getDataPathRemote();
                isLocal=false;
            }
            if ( reader.readmetaData(isLocal).containsKey(tableName))
            {
                result = checkbeforeWhere(splitString);
                if(result == true)
                {
                    columns = readerTable.readTableData(tableName,isLocal);
                    if(columns.size()>0)
                    {
                        result = deleteDataOfTable();
                    }

                }
            }

        }
        if(result == true && ConfigurationSetup.getAutoCommit())
        {
            writeTable writeInFile = new writeTable();
            writeTable.writeTable(tableName,columns,isLocal);
            //eventlog and sqldump
            log+="delete^";
            log+=tableName+"^";
            log+=columns.toString()+"^";
            String timeStamp = new SimpleDateFormat("yyyy-MM-dd^HH:mm:ss").format(new Date());
            log+=timeStamp;
            eventLog.addLog(log, isLocal);
            sqlDump.toDump(queryForlog,dataPath);
        }
        if(result == false)
        {
            System.out.println("Sorry unable to delete data from table. Please check your query");
        }
        else if(result == true && !ConfigurationSetup.getAutoCommit())
        {

            TransactionHelper.writeToTransactionFile(queryForlog,tableName);

            System.out.println("data successfully deleted from"+tableName+" but transaction not yet commited");

        }
        else
        {
            System.out.println("data successfully deleted from"+tableName);
        }
        return result;
    }

    private boolean deleteDataOfTable() {

        for(String keys:columns.keySet())
        {
            columns.get(keys).clear();
        }
        return true;
    }

    private boolean deleteData()
    {
        boolean result = false;
        int flag=0;
        Iterator<String> iterator = columns.keySet().iterator();

        String key = null;
        if(iterator.hasNext())
        {
            key = iterator.next();
        }

        for(int i=columns.get(key).size()-1;i>=0;i--)
        {
            flag = 0;
            for(String keys : columns.keySet())
            {
                if(whereConditions.containsKey(keys))
                {
                    String value = whereConditions.get(keys).get(0);
                    String operator = whereConditions.get(keys).get(1);
                    if(operator.equals("=")) {

                        if(columns.get(keys).get(i).equals(value) == false )
                        {
                            flag=1;
                            break;
                        }
                    }
                    else if(operator.equals("<"))
                    {
                        if(columntype.get(keys).equals("int"))
                        {
                            int tableValue = Integer.parseInt(columns.get(keys).get(i));
                            int updatevalue = Integer.parseInt(value);
                            if(tableValue<updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                        else if(columntype.get(keys).equals("float"))
                        {
                            float tableValue = Float.parseFloat(columns.get(keys).get(i));
                            float updatevalue = Float.parseFloat(value);
                            if(tableValue<updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }

                    }
                    else if(operator.equals(">"))
                    {
                        if(columntype.get(keys).equals("int"))
                        {
                            int tableValue = Integer.parseInt(columns.get(keys).get(i));
                            int updatevalue = Integer.parseInt(value);
                            if(tableValue>updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                        else if(columntype.get(keys).equals("float"))
                        {
                            float tableValue = Float.parseFloat(columns.get(keys).get(i));
                            float updatevalue = Float.parseFloat(value);
                            if(tableValue>updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                    }
                    else if(operator.equals("<=")) {
                        if(columntype.get(keys).equals("int"))
                        {
                            int tableValue = Integer.parseInt(columns.get(keys).get(i));
                            int updatevalue = Integer.parseInt(value);
                            if(tableValue<=updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                        else if(columntype.get(keys).equals("float"))
                        {
                            float tableValue = Float.parseFloat(columns.get(keys).get(i));
                            float updatevalue = Float.parseFloat(value);
                            if(tableValue<=updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                    }
                    else if(operator.equals(">="))
                    {
                        if(columntype.get(keys).equals("int"))
                        {
                            int tableValue = Integer.parseInt(columns.get(keys).get(i));
                            int updatevalue = Integer.parseInt(value);
                            if(tableValue>=updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                        else if(columntype.get(keys).equals("float"))
                        {
                            float tableValue = Float.parseFloat(columns.get(keys).get(i));
                            float updatevalue = Float.parseFloat(value);
                            if(tableValue>=updatevalue) {
                                flag=0;
                            }
                            else
                            {
                                flag=1;
                                break;
                            }
                        }
                    }

                }
            }

            if(flag==0)
            {
                result = deleteElementFromTable(i);
            }
        }
        return result;
    }

    private boolean deleteElementFromTable(int i)
    {
        List<String> tableNames = reader.checkIfTableIsReferenced(tableName,isLocal);

        if(tableNames.size()>0)
        {
            for(int j = 0;j<tableNames.size();j++)
            {
                MetaData tableMetaData = reader.readmetaData(isLocal).get(tableNames.get(j));
                String foreingKeyColumn=tableMetaData.getForeignkeyColumn();
                try {
                    LinkedHashMap<String,ArrayList<String>> otherTableColumns =readerTable.readTableData(tableNames.get(j),isLocal);
                    if(otherTableColumns.containsKey(foreingKeyColumn) && otherTableColumns.get(foreingKeyColumn).contains(columns.get(primaryKeyColumnName).get(i)))
                    {
                        return false;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
        }
        for(String keys : columns.keySet())
        {
            columns.get(keys).remove(i);
        }
        return true;
    }

    private boolean checkbeforeWhere(String[] Data) {
        if(Data[0].equals("delete"))
        {
            if (Data[1].equals("from"))
            {
                return true;
            }
        }
        return false;
    }

    private boolean storeWhereCondition(String string) {
        String[] colNameWithValue;
        boolean result = false;
        String[] splitCol;
        String[] splitCols = new String[1];
        splitCols[0]= string;
        splitCol = splitCols;

        for(int i=0;i<splitCol.length;i++)
        {

            splitCol[i] = splitCol[i].trim();
            String sing = null;
            if(splitCol[i].contains(">="))
            {
                colNameWithValue = splitCol[i].split(">=");
                sing = ">=";
            }
            else if (splitCol[i].contains("<="))
            {
                colNameWithValue = splitCol[i].split("<=");
                sing = "<=";

            }
            else if (splitCol[i].contains("<"))
            {
                colNameWithValue = splitCol[i].split("<");
                sing = "<";

            }
            else if (splitCol[i].contains(">"))
            {
                colNameWithValue = splitCol[i].split(">");
                sing = ">";
            }
            else if (splitCol[i].contains("="))
            {
                colNameWithValue = splitCol[i].split("=");
                sing = "=";
            }
            else
            {
                System.out.println("invalid where clause condition");
                break;
            }
            if(colNameWithValue.length == 2)
            {
                result = addColumns(colNameWithValue,sing);

         }
            if(result == false)
            {
                break;
            }
        }
        return result;
    }

    private boolean addColumns(String[] colNameWithValue, String sing)
    {
        boolean result = true;
        colNameWithValue[0] = colNameWithValue[0].trim();
        if(columns.containsKey(colNameWithValue[0]))
        {
            String key = colNameWithValue[0];
            key = key.trim();
            String element = colNameWithValue[1].replace("'", "");
            element = element.trim();
            whereConditions.put(key,new ArrayList<String>());
            whereConditions.get(key).add(element);
            whereConditions.get(key).add(sing);

        }
        else
        {
            result = false;
        }
        return result;
    }

}

