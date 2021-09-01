import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SelectQueryParserTest
{
    @Test
    public void isValidAllColumns_True()
    {
        String sql="Select * from TABLE1;";
        SelectQueryParser parse=new SelectQueryParser();
        boolean result=parse.isValid(sql);
        Assertions.assertTrue(result);
    }

    @Test
    public void isValidSpecificColumn_True()
    {
        String sql="Select column1,column2 from table1;";
        SelectQueryParser parse=new SelectQueryParser();
        boolean result=parse.isValid(sql);
        Assertions.assertTrue(result);
        Assertions.assertEquals("table1",parse.getTableName());
        String[] columns=parse.getColumns();
        Assertions.assertEquals("column1",columns[0]);
        Assertions.assertEquals("column2",columns[1]);
    }

    @Test
    public void isValidWithWhere_True()
    {
        String sql="Select * from TABLE1 where column1 = 1;";
        SelectQueryParser parse=new SelectQueryParser();
        boolean result=parse.isValid(sql);
        Assertions.assertTrue(result);
        Assertions.assertEquals("table1",parse.getTableName());
        Assertions.assertEquals("column1",parse.getWhereColumn());
        Assertions.assertEquals("1",parse.getWhereValue());
        Assertions.assertEquals("=",parse.getWhereOperation());
    }
}
