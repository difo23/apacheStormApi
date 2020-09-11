package com.ayscom.minetur;

import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.utils.MongoDBConnector;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Christian on 3/7/15.
 */
public class XDR implements Cloneable {

    private static final String ID_FIELD = "_id";
    private static final String MSISDN_FIELD = "msisdn";
    private static final String IMEI_FIELD = "imei";
    private static final String IMSI_FIELD = "imsi";
    private static final String START_TIME_FIELD = "start_time";
    private static final String END_TIME_FIELD = "end_time";
    private static final String MANDATORY_FIELD = "mandatory";
    private static final String XDR_RAW_FIELD = "xdr_raw";

    private ObjectId _id = null;
    private Date _start_time = null;
    private Date _end_time = null;
    private Long _msisdn = null;
    private Long _imei = null;
    private Long _imsi = null;
    private Boolean _mandatory = false;
    private Document _xdr_raw = null;
    private final String _format= "dd/MM/yyyy HH:mm:ss";

    public XDR(Document xdr_raw, String msisdn_field, String imei_field, String imsi_field,
               String start_time_field, String end_time_field) {

        if ((start_time_field != null) && (end_time_field != null)) {
            _id = (ObjectId) xdr_raw.get( "_id" );
            _start_time = setDate((String) xdr_raw.get(start_time_field), _format);
            _end_time = setDate((String) xdr_raw.get(end_time_field), _format);

            if (msisdn_field != null) {
                Object msisdn = xdr_raw.get(msisdn_field);
                if (msisdn instanceof Number)
                    _msisdn = ((Number)msisdn).longValue();
            }

            if (imei_field != null) {
                Object imei = xdr_raw.get(imei_field);
                if (imei instanceof Number)
                    _imei = ((Number)imei).longValue();
            }

            if (imsi_field != null) {
                Object imsi = xdr_raw.get(imsi_field);
                if (imsi instanceof Number)
                    _imsi = ((Number)imsi).longValue();
            }

            _xdr_raw = xdr_raw;
        }
    }

    public XDR(Document xdr_raw, Long msisdn, Long imei, Long imsi,
               String start_time, String end_time) {

        if ((start_time != null) && (end_time != null)) {
            _id = (ObjectId) xdr_raw.get( "_id" );
            _start_time = setDate(start_time, _format);
            _end_time = setDate(end_time, _format);
            _msisdn = msisdn;
            _imei = imei;
            _imsi = imsi;
            _xdr_raw = xdr_raw;
        }
    }

    public XDR(Document xdr_rich) {
        _id = (ObjectId) xdr_rich.get(ID_FIELD);
        _msisdn = (Long) xdr_rich.get(MSISDN_FIELD);
        _imei = (Long) xdr_rich.get(IMEI_FIELD);
        _imsi = (Long) xdr_rich.get(IMSI_FIELD);
        _start_time = setDate((String) xdr_rich.get(START_TIME_FIELD), _format);
        _end_time = setDate((String) xdr_rich.get(END_TIME_FIELD), _format);
        _xdr_raw = (Document) xdr_rich.get(XDR_RAW_FIELD);

    }

    public ObjectId getId() {
        return _id;
    }

    public void setId(ObjectId id) {
        _id = id;
    }

    public Date getStartTime() {
        return _start_time;
    }

    public void setStartTime(Date start_time) {
        _start_time = start_time;
    }

    public Date getEndTime() {
        return _end_time;
    }

    public void setEndTime(Date end_time) {
        _end_time = end_time;
    }

    public Long getMSISDN() {
        return _msisdn;
    }

    public void setMSISDN(Long msisdn) {
        _msisdn = msisdn;
    }

    public Long getIMEI() {
        return _imei;
    }

    public void setIMEI(Long imei) {
        _imei = imei;
    }

    public Long getIMSI() {
        return _imsi;
    }

    public void setMandatory(Boolean mandatory) {
        _mandatory = mandatory;
    }

    public Boolean getMandatory() {
        return _mandatory;
    }

    public void setIMSI(Long imsi) {
        _imsi = imsi;
    }

    public Document getXDR_Raw() {
        return _xdr_raw;
    }

    public void setXDRRaw(Document xdr_raw) {
        _xdr_raw = xdr_raw;
    }

    public Document toBSON() {
        Document document = new Document(ID_FIELD, _id)
                .append(MSISDN_FIELD, _msisdn)
                .append(IMEI_FIELD, _imei)
                .append(IMSI_FIELD, _imsi)
                .append(START_TIME_FIELD, _start_time)
                .append(END_TIME_FIELD, _end_time)
                .append(MANDATORY_FIELD, _mandatory)
                .append(XDR_RAW_FIELD, _xdr_raw);
        return document;
    }

    public String toJSON() {
        return this.toBSON().toString();
    }

    private Date setDate(String date, String format){
        Date _date = null;

        if(date!= null){
            SimpleDateFormat dateFormat = new SimpleDateFormat(format);

            try {
                _date = dateFormat.parse(date);
            } catch (ParseException ex) {
                ex.printStackTrace();
            }
        }
        return _date;
    }

    public XDR clone() {
        try {
            return (XDR)super.clone();
        } catch (CloneNotSupportedException e) {
            return this;
        }
    }
}
