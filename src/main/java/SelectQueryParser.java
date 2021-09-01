//===CONSTRAINT: PUT SPACE BETWEEN OPERATOR AND OPERAND====
public class SelectQueryParser
{
    String sql;
    private String tableName;
    private String[] columns;
    private boolean isAll;
    private String whereColumn;
    private String whereOperation;
    private String whereValue;
    private String originalSQL;

    public SelectQueryParser()
    {
        sql="";
        columns=new String[]{};
        isAll=false;
        whereColumn="";
        whereOperation="";
        whereValue="";
    }

    public boolean isValid(String sql)
    {
        originalSQL = sql;
        boolean result=false;
        sql=sql.toLowerCase();
        String[] array=sql.split(" ");
        if(sql.contains("where"))
        {
            if(array.length>=8)
            {
                if (array[0].equalsIgnoreCase("select"))
                {
                    if (array[1].contains(","))
                    {
                        columns = array[1].split(",");
                    }
                    else if (!array[1].contains("*"))
                    {
                        columns = new String[]{array[1]};
                    }
                    else if (array[1].contains("*"))
                    {
                        isAll = true;
                    }
                    if (array[2].equals("from"))
                    {
                        if (!array[3].isEmpty() && !array[3].equals(";"))
                        {
                            tableName = array[3].replace(";", "");
                            if(array[4].equals("where")&&!array[7].equals(";")&&!array[7].isEmpty()
                                    &&array[7].contains(";")&&!array[6].isEmpty()&&!array[5].isEmpty())
                            {
                                whereColumn=array[5];
                                whereOperation=array[6];
                                whereValue=array[7].replace(";","");
                                result = true;
                            }

                        }
                    }
                }
            }
        }
        else {
            if (array.length >= 3) {
                if (array[0].equalsIgnoreCase("select")) {
                    if (array[1].contains(","))
                    {
                        columns = array[1].split(",");
                    }
                    else if (!array[1].contains("*"))
                    {
                        columns = new String[]{array[1]};
                    }
                    else if (array[1].contains("*"))
                    {
                        isAll = true;
                    }
                    if (array[2].equals("from"))
                    {
                        if (!array[3].isEmpty() && !array[3].equals(";") && array[3].contains(";")) {
                            tableName = array[3].replace(";", "");
                            result = true;
                        }
                    }
                }
            }
        }
        return result;
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumns() {
        return columns;
    }

    public String getWhereColumn() {
        return whereColumn;
    }

    public String getWhereOperation() {
        return whereOperation;
    }



    public String getWhereValue() {
        return whereValue;
    }

    public String getOriginalSql(){return originalSQL;}
    public boolean isAll() {
        return isAll;
    }
}
