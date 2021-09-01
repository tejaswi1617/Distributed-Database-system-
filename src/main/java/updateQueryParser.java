import java.text.SimpleDateFormat;
import java.util.*;

public class updateQueryParser
{
    private static String dataPath;
    String tableName;
    LinkedHashMap<String, ArrayList<String>> columns;
    LinkedHashMap<String, String> columntype;
    Map<String,String> colWithValue;
    Map<String, ArrayList<String>> colWithValueInWhereClause;
    List<String> tableNames;
    readTable readerTable;
    MetaData tableMetaData;

    String primaryKeyColumnName;
    String foreingKeyColumn;
    String foriengKeyReferenceTableName;
    String foriengKeyReferenceTableColumn;
    readMetaData reader;
    private static boolean isLocal;
    static HashMap<String, List<String>> gdd;

    public updateQueryParser()
    {
        tableName= null;
        primaryKeyColumnName=null;
        foreingKeyColumn=null;
        foriengKeyReferenceTableName=null;
        foriengKeyReferenceTableColumn=null;
        readerTable = new readTable();
        reader = new readMetaData();
        tableNames = new ArrayList<String>();
        columns = new LinkedHashMap<String, ArrayList<String>>();
        columntype = new LinkedHashMap<String, String>();
        colWithValue = new HashMap<String,String>();
        colWithValueInWhereClause = new HashMap<String,ArrayList<String>>();
    }

    public boolean updateTable(String query) throws Exception {
        boolean result = false;
        //dataPath = ConfigurationSetup.getDataPathLocal();


        String log="";
        EventLog eventLog = new EventLog();
        SqlDump sqlDump = new SqlDump();
        readGDD.readgdd();
        gdd = GDD.getInstance();
        if(query.contains("where"))
        {
            String[] wordsInquery = query.toLowerCase().split("where");
            if(wordsInquery.length == 2 && wordsInquery[0].contains("set"))
            {
                String[] wordsbeforeWhere = wordsInquery[0].split("set");
                if(wordsbeforeWhere.length == 2) {
                    String[] data = wordsbeforeWhere[0].split(" ");
                    if(data.length==2){
                        tableName = data[1].toLowerCase();
                    }
                    else{
                        System.out.println("Invalid update query. Please check you query statement");
                        return false;
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
                        tableMetaData = reader.readmetaData(isLocal).get(tableName);
                        columns = readerTable.readTableData(tableName,isLocal);
                        tableNames = reader.checkIfTableIsReferenced(tableName,isLocal);
                        primaryKeyColumnName= tableMetaData.getPrimaryKeyColumn() ;
                        foreingKeyColumn=tableMetaData.getForeignkeyColumn();
                        if(foreingKeyColumn != null)
                        {
                            foriengKeyReferenceTableName=tableMetaData.getReferencedTable();
                            foriengKeyReferenceTableColumn=tableMetaData.getReferencedColumnName();
                        }

                        if(data[0].toLowerCase().equals("update"))
                        {
                            result = validateAfterWhereWord(wordsInquery[1]);
                            if(result == true)
                            {
                                result = checkAndAddUpdateClumns(wordsbeforeWhere[1]);
                                if(result == true)
                                {
                                    result = updateTable();
                                    if(result == true && ConfigurationSetup.getAutoCommit())
                                    {
                                        writeTable writeInFile = new writeTable();
                                        writeTable.writeTable(tableName,columns,isLocal);
                                        //eventlog and sqlDump
                                        log+="update^";
                                        log+=tableName+"^";
                                        log+=columns.toString()+"^";
                                        String timeStamp = new SimpleDateFormat("yyyy-MM-dd^HH:mm:ss").format(new Date());
                                        log+=timeStamp;
                                        eventLog.addLog(log, isLocal);
                                        sqlDump.toDump(query,dataPath);
                                        System.out.println("Updated successfully");

                                    }
                                    else if(result == true && !ConfigurationSetup.getAutoCommit())
                                    {

                                        TransactionHelper.writeToTransactionFile(query,tableName);
                                        System.out.println("data successfully updated in "+tableName+" but transaction not yet commited");
                                        return result;

                                    }
                                }
                            }
                        }
                    }
                }

            }
        }
        if(result == false){
            System.out.println("Sorry unable to perform update on table. Please write correct query");
        }
        else{
            System.out.println("data successfully updated in "+tableName);

        }
        return result;
    }



    private boolean updateTable() {
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
                if(colWithValueInWhereClause.containsKey(keys))
                {
                    String value = colWithValueInWhereClause.get(keys).get(0);
                    String operator = colWithValueInWhereClause.get(keys).get(1);
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
                result = updateTable(i);
                if(result == false)
                {
                    break;
                }
            }
        }
        return result;
    }

    private boolean updateTable(int i) {

        for(String key: colWithValue.keySet())
        {
            String element = colWithValue.get(key);
            String currentKey = columns.get(primaryKeyColumnName).get(i);
            if(primaryKeyColumnName.trim() != null && primaryKeyColumnName.trim().equals(key))
            {
                if(columns.get(key).contains(element) == true)
                {
                    return false;
                }
                for(int j=0;j<tableNames.size();j++)
                {
                    LinkedHashMap<String, ArrayList<String>> currentTableColumn = null;
                    try {
                        currentTableColumn = readerTable.readTableData(tableNames.get(j),isLocal);
                        tableMetaData = reader.readmetaData(isLocal).get(tableNames.get(j));
                        String foreingKeyColumnofTable=tableMetaData.getForeignkeyColumn();
                        if(currentTableColumn.get(foreingKeyColumnofTable).contains(currentKey))
                        {
                            return false;
                        }
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        return false;
                    }
                }
            }
            if(foreingKeyColumn != null && foreingKeyColumn.equals(key)) {
                try {
                    LinkedHashMap<String, ArrayList<String>> foreingKeyTableData  = readerTable.readTableData(foriengKeyReferenceTableName,isLocal);
                    if (foreingKeyTableData.size() > 0) {
                        if (!foreingKeyTableData.get(foriengKeyReferenceTableColumn).contains(element)) {
                            return false;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            columns.get(key).set(i, element);
        }
        return true;
    }

    private boolean checkAndAddUpdateClumns(String string) {
        boolean result = false;

        readMetaData reader = new readMetaData();
        MetaData tableMetaData = reader.readmetaData(isLocal).get(tableName);
        columntype = tableMetaData.getColumns();

        String[] splitCol = string.split(",");
        String[] colNameWithValue;
        for(int i=0;i<splitCol.length;i++)
        {
            colNameWithValue = splitCol[i].split("=");
            if(colNameWithValue.length == 2)
            {
                result = checkForDataType(colNameWithValue);
                if(result == true)
                {
                    colNameWithValue[0] = colNameWithValue[0].trim();
                    if(colWithValue.containsKey(colNameWithValue[0]) == false)
                    {
                        if(columns.containsKey(colNameWithValue[0])) {
                            String element = colNameWithValue[1].replace("'", "");
                            element = element.trim();
                            colWithValue.put(colNameWithValue[0],element);
                        }
                        else
                        {
                            result = false;
                        }
                    }
                    else
                    {
                        result = false;
                    }
                }
            }
            else
            {
                break;
            }
            if(result == false)
            {
                break;
            }
        }
        return result;
    }

    private boolean checkForDataType(String[] colNameWithValue)
    {
        boolean result = false;
        String key =colNameWithValue[0].trim();
        if(columntype.containsKey(key))
        {
            if(columntype.get(key).equals("int"))
            {
                result = validateInt(colNameWithValue[1].trim());
            }
            else if(columntype.get(key).equals("float"))
            {
                result = validateFloat(colNameWithValue[1].trim());
            }
            else if(columntype.get(key).equals("text")) {
                result = validateTextValue(colNameWithValue[1].trim());
            }
        }
        return result;
    }
    private boolean validateTextValue(String columnValue)
    {
        columnValue = columnValue.trim();
        String charAtZero = Character.toString(columnValue.charAt(0));
        String chartAtLast =Character.toString(columnValue.charAt(columnValue.length()-1));

        if(charAtZero.equals("'") && chartAtLast.equals("'"))
        {
            return true;
        }
        return false;
    }

    private boolean validateInt(String columnValue)
    {
        if(columnValue.matches("[0-9]+") == true)
        {
            return true;
        }
        return false;
    }

    private boolean validateFloat(String columnValue)
    {
        if(columnValue.matches("^\\d*\\.?\\d+|\\d+\\.\\d*$") == true)
        {
            return true;
        }
        return false;
    }

    private boolean validateBeforeSetWord(String wordsbeforeWhere)
    {
        String[] words = wordsbeforeWhere.split(" ");
        if(words.length == 2)
        {
            if(words[0].equalsIgnoreCase("update"))
            {
                return true;

            }
        }
        return false;

    }

    private boolean validateAfterWhereWord(String string)
    {
        String[] colNameWithValue;
        boolean result = false;
        if(string.contains(";"))
        {
            string = string.replace(";","");
            String[] splitCol;
            if(string.contains("and") == true )
            {
                String[] splitCols = string.split("and");
                splitCol = splitCols;
            }
            else
            {
                String[] splitCols = new String[1];
                splitCols[0]= string;
                splitCol = splitCols;
            }
            for(int i=0;i<splitCol.length;i++)
            {
                splitCol[i] = splitCol[i].replaceAll("'", "");
                splitCol[i] = splitCol[i].trim();
                if(splitCol[i].contains(">="))
                {
                    colNameWithValue = splitCol[i].split(">=");
                    String sing = ">=";
                    result = addColumns(colNameWithValue,sing);
                }
                else if (splitCol[i].contains("<="))
                {
                    colNameWithValue = splitCol[i].split("<=");
                    String sing = "<=";
                    result = addColumns(colNameWithValue,sing);
                }
                else if (splitCol[i].contains("<"))
                {
                    colNameWithValue = splitCol[i].split("<");
                    String sing = "<";
                    result = addColumns(colNameWithValue,sing);
                }
                else if (splitCol[i].contains(">"))
                {
                    colNameWithValue = splitCol[i].split(">");
                    String sing = ">";
                    result = addColumns(colNameWithValue,sing);
                }
                else if (splitCol[i].contains("="))
                {
                    colNameWithValue = splitCol[i].split("=");
                    String sing = "=";
                    result = addColumns(colNameWithValue,sing);
                }
                else
                {
                    System.out.println("invalid where clause condition");
                }
            }

        }
        return result;
    }

    private boolean addColumns(String[] colNameWithValue, String sing) {
        boolean result = true;
        colNameWithValue[0] = colNameWithValue[0].trim();
        if(colWithValue.containsKey(colNameWithValue[0]) == false && colNameWithValue.length == 2)
        {
            if(columns.containsKey(colNameWithValue[0])) {
                String key = colNameWithValue[0];
                key = key.trim();
                String element = colNameWithValue[1].replace("'", "");
                element = element.trim();
                colWithValueInWhereClause.put(key,new ArrayList<String>());
                colWithValueInWhereClause.get(key).add(element);
                colWithValueInWhereClause.get(key).add(sing);
            }
            else
            {
                result = false;
            }
        }
        else
        {
            result = false;
        }
        return result;
    }
}
