package com.ayscom.minetur.FrankXDRReload.FrankUtilities;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FrankUtils.class);

    public static BasicDBObject setInterval( BasicDBList xdrs ) {
        LOG.info("Inicio setInterval:");
        LOG.info("XDRS para buscar el intevalo size:"+xdrs.size()+" contenido:"+xdrs.toString());

        BasicDBObject time = new BasicDBObject();
        Date end;
        Date start;
        DBObject dbObt;
        DBObject dbObt2 = (DBObject) (xdrs.get(0));
        int count = 0;


        start = (Date) dbObt2.get("start_time");
        end = (Date) dbObt2.get("end_time");

        LOG.info("Start time  para inicial la busqueda "+start.toString());
        LOG.info("End time  para inicial la busqueda "+end.toString());

        while (count < xdrs.size()) {
            dbObt = (DBObject) (xdrs.get(count));
            Date date_start = (Date) dbObt.get("start_time");
            Date date_end = (Date) dbObt.get("end_time");

            if (start.compareTo(date_start) == 1) {
                start = date_start;
            }
            if (end.compareTo(date_end) == -1) {
                end = date_end;
            }
            ++count;
        }
        LOG.info("Fuera del loop "+end.toString());
        time.put("end", end);
        time.put("start", start);
        LOG.info("carga a time");

        LOG.info("Finalizo setInterval:");
        LOG.info("Resultados start: "+time.get("start")+" end:"+time.get("end"));

        return time;
    }

}
