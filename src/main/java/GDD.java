import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class GDD {

    private static HashMap<String, List<String>> gdd;

    public HashMap<String, List<String>> getGdd() {
        return gdd;
    }

    public void setGdd(HashMap<String, List<String>> gdd) {
        GDD.gdd = gdd;
    }

    private GDD()
    {

    }

    public static HashMap<String, List<String>> getInstance() {
        if(gdd==null)
        {
            gdd=new HashMap<>();
        }
        return gdd;
    }
}
