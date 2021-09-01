import com.jcraft.jsch.SftpException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public class DropTableParse {
    private String tableName;

    public boolean isValid(String sql) {
        boolean result = false;
        sql = sql.toLowerCase();
        String[] array = sql.split(" ");
        if (array[0].equals("drop") && array[1].equals("table") && sql.charAt(sql.length() - 1) == ';') {
            tableName = array[2].substring(0,array[2].length()-1);
            result = true;
        }
        else {
            System.out.println("incorrect query");
        }
        return result;
    }

    public String getTableName() {
        return tableName;
    }


    public boolean dropTable(String sql) throws IOException, SftpException {
        boolean result = false;
        result = isValid(sql);
        if(result)
        {
            if(ConfigurationSetup.getAutoCommit() && TransactionHelper.checkfortablelocked(getTableName())==true){
                System.out.println("sorry "+tableName+" is used by another transaction");
                return false;
            }
            else if(!ConfigurationSetup.getAutoCommit())
            {
                TransactionHelper.writeToTransactionFile(sql,tableName);
                return true;
            }
            DropTable dropTable = new DropTable();
            dropTable.deleteFile(getTableName(), sql);
            result = true;
        }
        return result;
    }
}