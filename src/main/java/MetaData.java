import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class MetaData
{
    String tableName;
    int numberOfColumns;
    LinkedHashMap<String, String> columns;
    String primaryKeyColumn;
    String primaryKeyDataType;
    String foreignkeyColumn;
    String foreignkeyDataType;
    private String referencedColumnName ="";
    private String referencedTable="";

    public String getForeignkeyColumn() {
        return foreignkeyColumn;
    }

    public void setForeignkeyColumn(String foreignkeyColumn) {
        this.foreignkeyColumn = foreignkeyColumn;
    }

    public String getReferencedColumnName() {
        return referencedColumnName;
    }

    public void setReferencedColumnName(String referencedColumnName) {
        this.referencedColumnName = referencedColumnName;
    }

    public String getReferencedTable() {
        return referencedTable;
    }

    public void setReferencedTable(String referencedTable) {
        this.referencedTable = referencedTable;
    }

    public void setPrimaryKeyDataType(String primaryKeyDataType) {
        this.primaryKeyDataType = primaryKeyDataType;
    }

    public void setForeignkeyDataType(String foreignkeyDataType) {
        this.foreignkeyDataType = foreignkeyDataType;
    }

    public String getPrimaryKeyDataType() {
        return primaryKeyDataType;
    }

    public String getForeignkeyDataType() {
        return foreignkeyDataType;
    }
    public void setColumns(LinkedHashMap<String, String> columns) {
        this.columns = columns;
    }

    public LinkedHashMap<String, String> getColumns() {
        return columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getNumberOfColumns() {
        return numberOfColumns;
    }

    public void setNumberOfColumns(int numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public void setPrimaryKeyColumn(String primaryKeyColumn)
    {
        this.primaryKeyColumn=primaryKeyColumn;
    }
}
