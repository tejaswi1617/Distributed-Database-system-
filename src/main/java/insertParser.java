import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;

public class insertParser
{
    public boolean insertQueryParserChecker(String[] data, boolean isLocal) throws Exception {
        boolean result;
        result = insertqueryparser(data, isLocal);
        return result;
    }

    private boolean insertqueryparser(String[] data, boolean isLocal)
    {
        boolean result = false;
        String[] dataBeforeValue = data[0].split(" ");
        if(dataBeforeValue[0].equalsIgnoreCase("insert"))
        {
            if (dataBeforeValue[1].equalsIgnoreCase("into"))
            {
                String tableName = dataBeforeValue[2].trim();
                String query = data[1];

                   if (query.contains("(") && query.contains(")") && query.contains(";"))
                   {

                       query = query.replace("(", "");
                       query = query.replace(")", "");
                       query = query.replace(";", "");
                       result = checkData(query);

                       if(result == true)
                       {
                            String[] columnValues =  query.split(",");
                            result = checkForColumnDataType(tableName,columnValues,isLocal);

                       }
                   }
            }
        }
        return result;
    }

    private boolean checkForColumnDataType(String tableName, String[] columnValues, boolean isLocal)
    {
        boolean result=false;
        readMetaData reader = new readMetaData();
        MetaData tableMetaData = reader.readmetaData(isLocal).get(tableName);
        LinkedHashMap<String, String> columnsWithtype = tableMetaData.getColumns();
        int numberOfColumns = tableMetaData.getNumberOfColumns();
        if(numberOfColumns == columnValues.length)
        {
            int index = 0;
            for(String key : columnsWithtype.keySet())
            {
                String getDataType =   columnsWithtype.get(key);
                if(getDataType.equals("int"))
                {
                    result = validateNumber(columnValues[index].trim());
                }
                else if(getDataType.equals("float"))
                {
                    result = validateFloat(columnValues[index].trim());
                }
                else if(getDataType.equals("text"))
                {
                    result = validateTextValue(columnValues[index].trim());
                }
                else
                    {
                    result = true;
                }
                if(result == false)
                {
                    break;
                }
                index++;

            }

        }
        return result;
    }

    private boolean checkData(String string)
    {
        if(string.contains("(") && string.contains(")") && string.contains(";"))
        {
            return false;
        }
        return true;
    }

    private boolean validateTextValue(String columnValue)
    {
        columnValue = columnValue.trim();
        String charAtZero = Character.toString(columnValue.charAt(0));
        String chartAtLast =Character.toString(columnValue.charAt(columnValue.length()-1));

        if(charAtZero.equals("'")&& chartAtLast.equals("'"))
        {
            return true;
        }
        return false;
    }

    private boolean validateNumber(String columnValue)
    {
        if(columnValue.matches("[0-9]+") == true)
        {
            return true;
        }
        return false;
    }

    private boolean validateFloat(String columnValue)
    {
        if(columnValue.matches("^\\d*\\.?\\d+|\\d+\\.\\d*$"))
        {
            return true;
        }
        return false;
    }
}
