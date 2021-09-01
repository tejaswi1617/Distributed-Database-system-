import java.util.Arrays;
import java.util.LinkedHashMap;

//====space between tablename and opening bracket required====
public class CreateQueryParse
{
    private String tableName;
    private String query;
    private LinkedHashMap<String,String> columns=new LinkedHashMap<>();
    private String primaryKeyColumn="";
    private boolean isLocal;

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setColumns(LinkedHashMap<String, String> columns) {
        this.columns = columns;
    }

    public void setPrimaryKeyColumn(String primaryKeyColumn) {
        this.primaryKeyColumn = primaryKeyColumn;
    }

    public void setPrimaryKeyDataType(String primaryKeyDataType) {
        this.primaryKeyDataType = primaryKeyDataType;
    }

    public void setForeignKeyColumn(String foreignKeyColumn) {
        this.foreignKeyColumn = foreignKeyColumn;
    }

    public void setForeignKeyDataType(String foreignKeyDataType) {
        this.foreignKeyDataType = foreignKeyDataType;
    }

    public void setReferencedColumnName(String referencedColumnName) {
        this.referencedColumnName = referencedColumnName;
    }

    public void setReferencedTable(String referencedTable) {
        this.referencedTable = referencedTable;
    }

    public String getPrimaryKeyDataType() {
        return primaryKeyDataType;
    }

    private String primaryKeyDataType="";

    public String getForeignKeyColumn() {
        return foreignKeyColumn;
    }

    private String foreignKeyColumn="";

    public String getForeignKeyDataType() {
        return foreignKeyDataType;
    }

    private String foreignKeyDataType="";
    public String getReferencedColumnName() {
        return referencedColumnName;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    private String referencedColumnName ="";
    private String referencedTable="";

    public boolean isValid(String sql) throws Exception {
        boolean result=false;
        sql=sql.toLowerCase();
        query = sql;
        String[] array=sql.split(" ");
        if(array.length>3)
        {
            if(array[0].equals("local"))
            {
                isLocal = true;
                sql=sql.replace("local","");
            }
            else if(array[0].equals("remote"))
            {
                isLocal = false;
                sql=sql.replace("remote","");
            }
            else
            {
                throw new Exception("Specify location of creation");
            }
            sql=sql.trim();
            array=sql.split(" ");
            if(array[0].equals("create")&&array[1].equals("table"))
            {
                if(!array[2].contains("("))
                {
                    tableName=array[2];
                    sql=sql.replace("create","");
                    sql=sql.replace("table","");
                    sql=sql.replace(tableName,"");
                    sql=sql.trim();
                    if(!sql.isEmpty()) {
                        char openBracket = sql.charAt(0);

                        int indexOfClose = sql.lastIndexOf(')');
                        if (openBracket == '(' && indexOfClose + 2 == sql.length() && sql.charAt(sql.length() - 1) == ';') {
                            StringBuilder string = new StringBuilder(sql);
                            string.deleteCharAt(indexOfClose);
                            string.deleteCharAt(string.length() - 1);

                            string.deleteCharAt(0);

                            sql = string.toString();

                            String[] cols = sql.split(",");

                            int i = 0;
                            for (; i < cols.length; i++) {
                                cols[i] = cols[i].trim();
                                String[] column = cols[i].split(" ");
                                if (column.length == 2) {
                                    if (isValidDatatype(column[1])) {
                                        columns.put(column[0], column[1]);
                                    } else {
                                        throw new Exception("Invalid datatype");
                                    }
                                } else if (column.length == 4) {
                                    if (column[2].equals("primary") && column[3].equals("key")) {

                                        if (!isValidDatatype(column[1])) {
                                            throw new Exception("Invalid datatype");
                                        } else {
                                            primaryKeyColumn = column[0];
                                            primaryKeyDataType = column[1];
                                            columns.put(column[0], column[1]);
                                        }
                                    }
                                } else if (column.length == 7) {
                                    if (column[2].equals("foreign") && column[3].equals("key") && column[4].equals("references")) {
                                        if (!isValidDatatype(column[1])) {
                                            throw new Exception("Invalid datatype for foreign key");
                                        } else {
                                            foreignKeyColumn = column[0];
                                            foreignKeyDataType = column[1];
                                            referencedTable = column[5];
                                            referencedColumnName = column[6];
                                            columns.put(column[0], column[1]);
                                        }
                                    }
                                }
                                else
                                {
                                    throw new Exception("Invalid Syntax");
                                }
                            }
                            if (i == cols.length && !primaryKeyColumn.isEmpty()) {
                                result = true;
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    private boolean isValidDatatype(String datatype)
    {
        boolean result=false;
        if(datatype.equals("int")||datatype.equals("text")||datatype.equals("float"))
        {
            result=true;
        }
        return result;
    }

    public String getTableName()
    {
        return tableName;
    }

    public LinkedHashMap<String, String> getColumns()
    {
        return columns;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public String getQuery(){
        return query;
    }

    public void createTable(String sql) throws Exception
    {
        try
        {
            if(isValid(sql))
            {
                CreateTable execution = new CreateTable();
                execution.createTable(this, isLocal);
            }
            else
            {
                System.out.println("Invalid query");
            }
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

}
