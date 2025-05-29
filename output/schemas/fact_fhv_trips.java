// ORM class for table 'fact_fhv_trips'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 03 18:46:29 MSK 2025
// For connector: org.apache.sqoop.manager.PostgresqlManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.DelimiterSet;
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.RecordParser;
import org.apache.sqoop.lib.BooleanParser;
import org.apache.sqoop.lib.BlobRef;
import org.apache.sqoop.lib.ClobRef;
import org.apache.sqoop.lib.LargeObjectLoader;
import org.apache.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class fact_fhv_trips extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("hvfhs_license_num", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.hvfhs_license_num = (String)value;
      }
    });
    setters.put("dispatching_base_num", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.dispatching_base_num = (String)value;
      }
    });
    setters.put("originating_base_num", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.originating_base_num = (String)value;
      }
    });
    setters.put("request_datetime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.request_datetime = (java.sql.Timestamp)value;
      }
    });
    setters.put("on_scene_datetime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.on_scene_datetime = (java.sql.Timestamp)value;
      }
    });
    setters.put("pickup_datetime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.pickup_datetime = (java.sql.Timestamp)value;
      }
    });
    setters.put("dropoff_datetime", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.dropoff_datetime = (java.sql.Timestamp)value;
      }
    });
    setters.put("pu_location_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.pu_location_id = (Integer)value;
      }
    });
    setters.put("do_location_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.do_location_id = (Integer)value;
      }
    });
    setters.put("trip_miles", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.trip_miles = (Double)value;
      }
    });
    setters.put("trip_time", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.trip_time = (Long)value;
      }
    });
    setters.put("base_passenger_fare", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.base_passenger_fare = (Double)value;
      }
    });
    setters.put("tolls", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.tolls = (Double)value;
      }
    });
    setters.put("bcf", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.bcf = (Double)value;
      }
    });
    setters.put("sales_tax", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.sales_tax = (Double)value;
      }
    });
    setters.put("congestion_surcharge", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.congestion_surcharge = (Double)value;
      }
    });
    setters.put("airport_fee", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.airport_fee = (Double)value;
      }
    });
    setters.put("tips", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.tips = (Double)value;
      }
    });
    setters.put("driver_pay", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.driver_pay = (Double)value;
      }
    });
    setters.put("shared_request_flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.shared_request_flag = (String)value;
      }
    });
    setters.put("shared_match_flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.shared_match_flag = (String)value;
      }
    });
    setters.put("access_a_ride_flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.access_a_ride_flag = (String)value;
      }
    });
    setters.put("wav_request_flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.wav_request_flag = (String)value;
      }
    });
    setters.put("wav_match_flag", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.wav_match_flag = (String)value;
      }
    });
    setters.put("__index_level_0__", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        fact_fhv_trips.this.___index_level_0__ = (Long)value;
      }
    });
  }
  public fact_fhv_trips() {
    init0();
  }
  private String hvfhs_license_num;
  public String get_hvfhs_license_num() {
    return hvfhs_license_num;
  }
  public void set_hvfhs_license_num(String hvfhs_license_num) {
    this.hvfhs_license_num = hvfhs_license_num;
  }
  public fact_fhv_trips with_hvfhs_license_num(String hvfhs_license_num) {
    this.hvfhs_license_num = hvfhs_license_num;
    return this;
  }
  private String dispatching_base_num;
  public String get_dispatching_base_num() {
    return dispatching_base_num;
  }
  public void set_dispatching_base_num(String dispatching_base_num) {
    this.dispatching_base_num = dispatching_base_num;
  }
  public fact_fhv_trips with_dispatching_base_num(String dispatching_base_num) {
    this.dispatching_base_num = dispatching_base_num;
    return this;
  }
  private String originating_base_num;
  public String get_originating_base_num() {
    return originating_base_num;
  }
  public void set_originating_base_num(String originating_base_num) {
    this.originating_base_num = originating_base_num;
  }
  public fact_fhv_trips with_originating_base_num(String originating_base_num) {
    this.originating_base_num = originating_base_num;
    return this;
  }
  private java.sql.Timestamp request_datetime;
  public java.sql.Timestamp get_request_datetime() {
    return request_datetime;
  }
  public void set_request_datetime(java.sql.Timestamp request_datetime) {
    this.request_datetime = request_datetime;
  }
  public fact_fhv_trips with_request_datetime(java.sql.Timestamp request_datetime) {
    this.request_datetime = request_datetime;
    return this;
  }
  private java.sql.Timestamp on_scene_datetime;
  public java.sql.Timestamp get_on_scene_datetime() {
    return on_scene_datetime;
  }
  public void set_on_scene_datetime(java.sql.Timestamp on_scene_datetime) {
    this.on_scene_datetime = on_scene_datetime;
  }
  public fact_fhv_trips with_on_scene_datetime(java.sql.Timestamp on_scene_datetime) {
    this.on_scene_datetime = on_scene_datetime;
    return this;
  }
  private java.sql.Timestamp pickup_datetime;
  public java.sql.Timestamp get_pickup_datetime() {
    return pickup_datetime;
  }
  public void set_pickup_datetime(java.sql.Timestamp pickup_datetime) {
    this.pickup_datetime = pickup_datetime;
  }
  public fact_fhv_trips with_pickup_datetime(java.sql.Timestamp pickup_datetime) {
    this.pickup_datetime = pickup_datetime;
    return this;
  }
  private java.sql.Timestamp dropoff_datetime;
  public java.sql.Timestamp get_dropoff_datetime() {
    return dropoff_datetime;
  }
  public void set_dropoff_datetime(java.sql.Timestamp dropoff_datetime) {
    this.dropoff_datetime = dropoff_datetime;
  }
  public fact_fhv_trips with_dropoff_datetime(java.sql.Timestamp dropoff_datetime) {
    this.dropoff_datetime = dropoff_datetime;
    return this;
  }
  private Integer pu_location_id;
  public Integer get_pu_location_id() {
    return pu_location_id;
  }
  public void set_pu_location_id(Integer pu_location_id) {
    this.pu_location_id = pu_location_id;
  }
  public fact_fhv_trips with_pu_location_id(Integer pu_location_id) {
    this.pu_location_id = pu_location_id;
    return this;
  }
  private Integer do_location_id;
  public Integer get_do_location_id() {
    return do_location_id;
  }
  public void set_do_location_id(Integer do_location_id) {
    this.do_location_id = do_location_id;
  }
  public fact_fhv_trips with_do_location_id(Integer do_location_id) {
    this.do_location_id = do_location_id;
    return this;
  }
  private Double trip_miles;
  public Double get_trip_miles() {
    return trip_miles;
  }
  public void set_trip_miles(Double trip_miles) {
    this.trip_miles = trip_miles;
  }
  public fact_fhv_trips with_trip_miles(Double trip_miles) {
    this.trip_miles = trip_miles;
    return this;
  }
  private Long trip_time;
  public Long get_trip_time() {
    return trip_time;
  }
  public void set_trip_time(Long trip_time) {
    this.trip_time = trip_time;
  }
  public fact_fhv_trips with_trip_time(Long trip_time) {
    this.trip_time = trip_time;
    return this;
  }
  private Double base_passenger_fare;
  public Double get_base_passenger_fare() {
    return base_passenger_fare;
  }
  public void set_base_passenger_fare(Double base_passenger_fare) {
    this.base_passenger_fare = base_passenger_fare;
  }
  public fact_fhv_trips with_base_passenger_fare(Double base_passenger_fare) {
    this.base_passenger_fare = base_passenger_fare;
    return this;
  }
  private Double tolls;
  public Double get_tolls() {
    return tolls;
  }
  public void set_tolls(Double tolls) {
    this.tolls = tolls;
  }
  public fact_fhv_trips with_tolls(Double tolls) {
    this.tolls = tolls;
    return this;
  }
  private Double bcf;
  public Double get_bcf() {
    return bcf;
  }
  public void set_bcf(Double bcf) {
    this.bcf = bcf;
  }
  public fact_fhv_trips with_bcf(Double bcf) {
    this.bcf = bcf;
    return this;
  }
  private Double sales_tax;
  public Double get_sales_tax() {
    return sales_tax;
  }
  public void set_sales_tax(Double sales_tax) {
    this.sales_tax = sales_tax;
  }
  public fact_fhv_trips with_sales_tax(Double sales_tax) {
    this.sales_tax = sales_tax;
    return this;
  }
  private Double congestion_surcharge;
  public Double get_congestion_surcharge() {
    return congestion_surcharge;
  }
  public void set_congestion_surcharge(Double congestion_surcharge) {
    this.congestion_surcharge = congestion_surcharge;
  }
  public fact_fhv_trips with_congestion_surcharge(Double congestion_surcharge) {
    this.congestion_surcharge = congestion_surcharge;
    return this;
  }
  private Double airport_fee;
  public Double get_airport_fee() {
    return airport_fee;
  }
  public void set_airport_fee(Double airport_fee) {
    this.airport_fee = airport_fee;
  }
  public fact_fhv_trips with_airport_fee(Double airport_fee) {
    this.airport_fee = airport_fee;
    return this;
  }
  private Double tips;
  public Double get_tips() {
    return tips;
  }
  public void set_tips(Double tips) {
    this.tips = tips;
  }
  public fact_fhv_trips with_tips(Double tips) {
    this.tips = tips;
    return this;
  }
  private Double driver_pay;
  public Double get_driver_pay() {
    return driver_pay;
  }
  public void set_driver_pay(Double driver_pay) {
    this.driver_pay = driver_pay;
  }
  public fact_fhv_trips with_driver_pay(Double driver_pay) {
    this.driver_pay = driver_pay;
    return this;
  }
  private String shared_request_flag;
  public String get_shared_request_flag() {
    return shared_request_flag;
  }
  public void set_shared_request_flag(String shared_request_flag) {
    this.shared_request_flag = shared_request_flag;
  }
  public fact_fhv_trips with_shared_request_flag(String shared_request_flag) {
    this.shared_request_flag = shared_request_flag;
    return this;
  }
  private String shared_match_flag;
  public String get_shared_match_flag() {
    return shared_match_flag;
  }
  public void set_shared_match_flag(String shared_match_flag) {
    this.shared_match_flag = shared_match_flag;
  }
  public fact_fhv_trips with_shared_match_flag(String shared_match_flag) {
    this.shared_match_flag = shared_match_flag;
    return this;
  }
  private String access_a_ride_flag;
  public String get_access_a_ride_flag() {
    return access_a_ride_flag;
  }
  public void set_access_a_ride_flag(String access_a_ride_flag) {
    this.access_a_ride_flag = access_a_ride_flag;
  }
  public fact_fhv_trips with_access_a_ride_flag(String access_a_ride_flag) {
    this.access_a_ride_flag = access_a_ride_flag;
    return this;
  }
  private String wav_request_flag;
  public String get_wav_request_flag() {
    return wav_request_flag;
  }
  public void set_wav_request_flag(String wav_request_flag) {
    this.wav_request_flag = wav_request_flag;
  }
  public fact_fhv_trips with_wav_request_flag(String wav_request_flag) {
    this.wav_request_flag = wav_request_flag;
    return this;
  }
  private String wav_match_flag;
  public String get_wav_match_flag() {
    return wav_match_flag;
  }
  public void set_wav_match_flag(String wav_match_flag) {
    this.wav_match_flag = wav_match_flag;
  }
  public fact_fhv_trips with_wav_match_flag(String wav_match_flag) {
    this.wav_match_flag = wav_match_flag;
    return this;
  }
  private Long ___index_level_0__;
  public Long get____index_level_0__() {
    return ___index_level_0__;
  }
  public void set____index_level_0__(Long ___index_level_0__) {
    this.___index_level_0__ = ___index_level_0__;
  }
  public fact_fhv_trips with____index_level_0__(Long ___index_level_0__) {
    this.___index_level_0__ = ___index_level_0__;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof fact_fhv_trips)) {
      return false;
    }
    fact_fhv_trips that = (fact_fhv_trips) o;
    boolean equal = true;
    equal = equal && (this.hvfhs_license_num == null ? that.hvfhs_license_num == null : this.hvfhs_license_num.equals(that.hvfhs_license_num));
    equal = equal && (this.dispatching_base_num == null ? that.dispatching_base_num == null : this.dispatching_base_num.equals(that.dispatching_base_num));
    equal = equal && (this.originating_base_num == null ? that.originating_base_num == null : this.originating_base_num.equals(that.originating_base_num));
    equal = equal && (this.request_datetime == null ? that.request_datetime == null : this.request_datetime.equals(that.request_datetime));
    equal = equal && (this.on_scene_datetime == null ? that.on_scene_datetime == null : this.on_scene_datetime.equals(that.on_scene_datetime));
    equal = equal && (this.pickup_datetime == null ? that.pickup_datetime == null : this.pickup_datetime.equals(that.pickup_datetime));
    equal = equal && (this.dropoff_datetime == null ? that.dropoff_datetime == null : this.dropoff_datetime.equals(that.dropoff_datetime));
    equal = equal && (this.pu_location_id == null ? that.pu_location_id == null : this.pu_location_id.equals(that.pu_location_id));
    equal = equal && (this.do_location_id == null ? that.do_location_id == null : this.do_location_id.equals(that.do_location_id));
    equal = equal && (this.trip_miles == null ? that.trip_miles == null : this.trip_miles.equals(that.trip_miles));
    equal = equal && (this.trip_time == null ? that.trip_time == null : this.trip_time.equals(that.trip_time));
    equal = equal && (this.base_passenger_fare == null ? that.base_passenger_fare == null : this.base_passenger_fare.equals(that.base_passenger_fare));
    equal = equal && (this.tolls == null ? that.tolls == null : this.tolls.equals(that.tolls));
    equal = equal && (this.bcf == null ? that.bcf == null : this.bcf.equals(that.bcf));
    equal = equal && (this.sales_tax == null ? that.sales_tax == null : this.sales_tax.equals(that.sales_tax));
    equal = equal && (this.congestion_surcharge == null ? that.congestion_surcharge == null : this.congestion_surcharge.equals(that.congestion_surcharge));
    equal = equal && (this.airport_fee == null ? that.airport_fee == null : this.airport_fee.equals(that.airport_fee));
    equal = equal && (this.tips == null ? that.tips == null : this.tips.equals(that.tips));
    equal = equal && (this.driver_pay == null ? that.driver_pay == null : this.driver_pay.equals(that.driver_pay));
    equal = equal && (this.shared_request_flag == null ? that.shared_request_flag == null : this.shared_request_flag.equals(that.shared_request_flag));
    equal = equal && (this.shared_match_flag == null ? that.shared_match_flag == null : this.shared_match_flag.equals(that.shared_match_flag));
    equal = equal && (this.access_a_ride_flag == null ? that.access_a_ride_flag == null : this.access_a_ride_flag.equals(that.access_a_ride_flag));
    equal = equal && (this.wav_request_flag == null ? that.wav_request_flag == null : this.wav_request_flag.equals(that.wav_request_flag));
    equal = equal && (this.wav_match_flag == null ? that.wav_match_flag == null : this.wav_match_flag.equals(that.wav_match_flag));
    equal = equal && (this.___index_level_0__ == null ? that.___index_level_0__ == null : this.___index_level_0__.equals(that.___index_level_0__));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof fact_fhv_trips)) {
      return false;
    }
    fact_fhv_trips that = (fact_fhv_trips) o;
    boolean equal = true;
    equal = equal && (this.hvfhs_license_num == null ? that.hvfhs_license_num == null : this.hvfhs_license_num.equals(that.hvfhs_license_num));
    equal = equal && (this.dispatching_base_num == null ? that.dispatching_base_num == null : this.dispatching_base_num.equals(that.dispatching_base_num));
    equal = equal && (this.originating_base_num == null ? that.originating_base_num == null : this.originating_base_num.equals(that.originating_base_num));
    equal = equal && (this.request_datetime == null ? that.request_datetime == null : this.request_datetime.equals(that.request_datetime));
    equal = equal && (this.on_scene_datetime == null ? that.on_scene_datetime == null : this.on_scene_datetime.equals(that.on_scene_datetime));
    equal = equal && (this.pickup_datetime == null ? that.pickup_datetime == null : this.pickup_datetime.equals(that.pickup_datetime));
    equal = equal && (this.dropoff_datetime == null ? that.dropoff_datetime == null : this.dropoff_datetime.equals(that.dropoff_datetime));
    equal = equal && (this.pu_location_id == null ? that.pu_location_id == null : this.pu_location_id.equals(that.pu_location_id));
    equal = equal && (this.do_location_id == null ? that.do_location_id == null : this.do_location_id.equals(that.do_location_id));
    equal = equal && (this.trip_miles == null ? that.trip_miles == null : this.trip_miles.equals(that.trip_miles));
    equal = equal && (this.trip_time == null ? that.trip_time == null : this.trip_time.equals(that.trip_time));
    equal = equal && (this.base_passenger_fare == null ? that.base_passenger_fare == null : this.base_passenger_fare.equals(that.base_passenger_fare));
    equal = equal && (this.tolls == null ? that.tolls == null : this.tolls.equals(that.tolls));
    equal = equal && (this.bcf == null ? that.bcf == null : this.bcf.equals(that.bcf));
    equal = equal && (this.sales_tax == null ? that.sales_tax == null : this.sales_tax.equals(that.sales_tax));
    equal = equal && (this.congestion_surcharge == null ? that.congestion_surcharge == null : this.congestion_surcharge.equals(that.congestion_surcharge));
    equal = equal && (this.airport_fee == null ? that.airport_fee == null : this.airport_fee.equals(that.airport_fee));
    equal = equal && (this.tips == null ? that.tips == null : this.tips.equals(that.tips));
    equal = equal && (this.driver_pay == null ? that.driver_pay == null : this.driver_pay.equals(that.driver_pay));
    equal = equal && (this.shared_request_flag == null ? that.shared_request_flag == null : this.shared_request_flag.equals(that.shared_request_flag));
    equal = equal && (this.shared_match_flag == null ? that.shared_match_flag == null : this.shared_match_flag.equals(that.shared_match_flag));
    equal = equal && (this.access_a_ride_flag == null ? that.access_a_ride_flag == null : this.access_a_ride_flag.equals(that.access_a_ride_flag));
    equal = equal && (this.wav_request_flag == null ? that.wav_request_flag == null : this.wav_request_flag.equals(that.wav_request_flag));
    equal = equal && (this.wav_match_flag == null ? that.wav_match_flag == null : this.wav_match_flag.equals(that.wav_match_flag));
    equal = equal && (this.___index_level_0__ == null ? that.___index_level_0__ == null : this.___index_level_0__.equals(that.___index_level_0__));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.hvfhs_license_num = JdbcWritableBridge.readString(1, __dbResults);
    this.dispatching_base_num = JdbcWritableBridge.readString(2, __dbResults);
    this.originating_base_num = JdbcWritableBridge.readString(3, __dbResults);
    this.request_datetime = JdbcWritableBridge.readTimestamp(4, __dbResults);
    this.on_scene_datetime = JdbcWritableBridge.readTimestamp(5, __dbResults);
    this.pickup_datetime = JdbcWritableBridge.readTimestamp(6, __dbResults);
    this.dropoff_datetime = JdbcWritableBridge.readTimestamp(7, __dbResults);
    this.pu_location_id = JdbcWritableBridge.readInteger(8, __dbResults);
    this.do_location_id = JdbcWritableBridge.readInteger(9, __dbResults);
    this.trip_miles = JdbcWritableBridge.readDouble(10, __dbResults);
    this.trip_time = JdbcWritableBridge.readLong(11, __dbResults);
    this.base_passenger_fare = JdbcWritableBridge.readDouble(12, __dbResults);
    this.tolls = JdbcWritableBridge.readDouble(13, __dbResults);
    this.bcf = JdbcWritableBridge.readDouble(14, __dbResults);
    this.sales_tax = JdbcWritableBridge.readDouble(15, __dbResults);
    this.congestion_surcharge = JdbcWritableBridge.readDouble(16, __dbResults);
    this.airport_fee = JdbcWritableBridge.readDouble(17, __dbResults);
    this.tips = JdbcWritableBridge.readDouble(18, __dbResults);
    this.driver_pay = JdbcWritableBridge.readDouble(19, __dbResults);
    this.shared_request_flag = JdbcWritableBridge.readString(20, __dbResults);
    this.shared_match_flag = JdbcWritableBridge.readString(21, __dbResults);
    this.access_a_ride_flag = JdbcWritableBridge.readString(22, __dbResults);
    this.wav_request_flag = JdbcWritableBridge.readString(23, __dbResults);
    this.wav_match_flag = JdbcWritableBridge.readString(24, __dbResults);
    this.___index_level_0__ = JdbcWritableBridge.readLong(25, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.hvfhs_license_num = JdbcWritableBridge.readString(1, __dbResults);
    this.dispatching_base_num = JdbcWritableBridge.readString(2, __dbResults);
    this.originating_base_num = JdbcWritableBridge.readString(3, __dbResults);
    this.request_datetime = JdbcWritableBridge.readTimestamp(4, __dbResults);
    this.on_scene_datetime = JdbcWritableBridge.readTimestamp(5, __dbResults);
    this.pickup_datetime = JdbcWritableBridge.readTimestamp(6, __dbResults);
    this.dropoff_datetime = JdbcWritableBridge.readTimestamp(7, __dbResults);
    this.pu_location_id = JdbcWritableBridge.readInteger(8, __dbResults);
    this.do_location_id = JdbcWritableBridge.readInteger(9, __dbResults);
    this.trip_miles = JdbcWritableBridge.readDouble(10, __dbResults);
    this.trip_time = JdbcWritableBridge.readLong(11, __dbResults);
    this.base_passenger_fare = JdbcWritableBridge.readDouble(12, __dbResults);
    this.tolls = JdbcWritableBridge.readDouble(13, __dbResults);
    this.bcf = JdbcWritableBridge.readDouble(14, __dbResults);
    this.sales_tax = JdbcWritableBridge.readDouble(15, __dbResults);
    this.congestion_surcharge = JdbcWritableBridge.readDouble(16, __dbResults);
    this.airport_fee = JdbcWritableBridge.readDouble(17, __dbResults);
    this.tips = JdbcWritableBridge.readDouble(18, __dbResults);
    this.driver_pay = JdbcWritableBridge.readDouble(19, __dbResults);
    this.shared_request_flag = JdbcWritableBridge.readString(20, __dbResults);
    this.shared_match_flag = JdbcWritableBridge.readString(21, __dbResults);
    this.access_a_ride_flag = JdbcWritableBridge.readString(22, __dbResults);
    this.wav_request_flag = JdbcWritableBridge.readString(23, __dbResults);
    this.wav_match_flag = JdbcWritableBridge.readString(24, __dbResults);
    this.___index_level_0__ = JdbcWritableBridge.readLong(25, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(hvfhs_license_num, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dispatching_base_num, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(originating_base_num, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(request_datetime, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(on_scene_datetime, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(pickup_datetime, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(dropoff_datetime, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeInteger(pu_location_id, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(do_location_id, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(trip_miles, 10 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeLong(trip_time, 11 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeDouble(base_passenger_fare, 12 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tolls, 13 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(bcf, 14 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(sales_tax, 15 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(congestion_surcharge, 16 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(airport_fee, 17 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tips, 18 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(driver_pay, 19 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(shared_request_flag, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(shared_match_flag, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(access_a_ride_flag, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(wav_request_flag, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(wav_match_flag, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(___index_level_0__, 25 + __off, -5, __dbStmt);
    return 25;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(hvfhs_license_num, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(dispatching_base_num, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(originating_base_num, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(request_datetime, 4 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(on_scene_datetime, 5 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(pickup_datetime, 6 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(dropoff_datetime, 7 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeInteger(pu_location_id, 8 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeInteger(do_location_id, 9 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeDouble(trip_miles, 10 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeLong(trip_time, 11 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeDouble(base_passenger_fare, 12 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tolls, 13 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(bcf, 14 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(sales_tax, 15 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(congestion_surcharge, 16 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(airport_fee, 17 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(tips, 18 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeDouble(driver_pay, 19 + __off, 8, __dbStmt);
    JdbcWritableBridge.writeString(shared_request_flag, 20 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(shared_match_flag, 21 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(access_a_ride_flag, 22 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(wav_request_flag, 23 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(wav_match_flag, 24 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeLong(___index_level_0__, 25 + __off, -5, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.hvfhs_license_num = null;
    } else {
    this.hvfhs_license_num = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dispatching_base_num = null;
    } else {
    this.dispatching_base_num = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.originating_base_num = null;
    } else {
    this.originating_base_num = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.request_datetime = null;
    } else {
    this.request_datetime = new Timestamp(__dataIn.readLong());
    this.request_datetime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.on_scene_datetime = null;
    } else {
    this.on_scene_datetime = new Timestamp(__dataIn.readLong());
    this.on_scene_datetime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.pickup_datetime = null;
    } else {
    this.pickup_datetime = new Timestamp(__dataIn.readLong());
    this.pickup_datetime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.dropoff_datetime = null;
    } else {
    this.dropoff_datetime = new Timestamp(__dataIn.readLong());
    this.dropoff_datetime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.pu_location_id = null;
    } else {
    this.pu_location_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.do_location_id = null;
    } else {
    this.do_location_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.trip_miles = null;
    } else {
    this.trip_miles = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.trip_time = null;
    } else {
    this.trip_time = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.base_passenger_fare = null;
    } else {
    this.base_passenger_fare = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.tolls = null;
    } else {
    this.tolls = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.bcf = null;
    } else {
    this.bcf = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.sales_tax = null;
    } else {
    this.sales_tax = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.congestion_surcharge = null;
    } else {
    this.congestion_surcharge = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.airport_fee = null;
    } else {
    this.airport_fee = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.tips = null;
    } else {
    this.tips = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.driver_pay = null;
    } else {
    this.driver_pay = Double.valueOf(__dataIn.readDouble());
    }
    if (__dataIn.readBoolean()) { 
        this.shared_request_flag = null;
    } else {
    this.shared_request_flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.shared_match_flag = null;
    } else {
    this.shared_match_flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.access_a_ride_flag = null;
    } else {
    this.access_a_ride_flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wav_request_flag = null;
    } else {
    this.wav_request_flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.wav_match_flag = null;
    } else {
    this.wav_match_flag = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.___index_level_0__ = null;
    } else {
    this.___index_level_0__ = Long.valueOf(__dataIn.readLong());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.hvfhs_license_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, hvfhs_license_num);
    }
    if (null == this.dispatching_base_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dispatching_base_num);
    }
    if (null == this.originating_base_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, originating_base_num);
    }
    if (null == this.request_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.request_datetime.getTime());
    __dataOut.writeInt(this.request_datetime.getNanos());
    }
    if (null == this.on_scene_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.on_scene_datetime.getTime());
    __dataOut.writeInt(this.on_scene_datetime.getNanos());
    }
    if (null == this.pickup_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.pickup_datetime.getTime());
    __dataOut.writeInt(this.pickup_datetime.getNanos());
    }
    if (null == this.dropoff_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.dropoff_datetime.getTime());
    __dataOut.writeInt(this.dropoff_datetime.getNanos());
    }
    if (null == this.pu_location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.pu_location_id);
    }
    if (null == this.do_location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.do_location_id);
    }
    if (null == this.trip_miles) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.trip_miles);
    }
    if (null == this.trip_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.trip_time);
    }
    if (null == this.base_passenger_fare) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.base_passenger_fare);
    }
    if (null == this.tolls) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tolls);
    }
    if (null == this.bcf) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.bcf);
    }
    if (null == this.sales_tax) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.sales_tax);
    }
    if (null == this.congestion_surcharge) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.congestion_surcharge);
    }
    if (null == this.airport_fee) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.airport_fee);
    }
    if (null == this.tips) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tips);
    }
    if (null == this.driver_pay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.driver_pay);
    }
    if (null == this.shared_request_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, shared_request_flag);
    }
    if (null == this.shared_match_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, shared_match_flag);
    }
    if (null == this.access_a_ride_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, access_a_ride_flag);
    }
    if (null == this.wav_request_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wav_request_flag);
    }
    if (null == this.wav_match_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wav_match_flag);
    }
    if (null == this.___index_level_0__) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.___index_level_0__);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.hvfhs_license_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, hvfhs_license_num);
    }
    if (null == this.dispatching_base_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, dispatching_base_num);
    }
    if (null == this.originating_base_num) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, originating_base_num);
    }
    if (null == this.request_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.request_datetime.getTime());
    __dataOut.writeInt(this.request_datetime.getNanos());
    }
    if (null == this.on_scene_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.on_scene_datetime.getTime());
    __dataOut.writeInt(this.on_scene_datetime.getNanos());
    }
    if (null == this.pickup_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.pickup_datetime.getTime());
    __dataOut.writeInt(this.pickup_datetime.getNanos());
    }
    if (null == this.dropoff_datetime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.dropoff_datetime.getTime());
    __dataOut.writeInt(this.dropoff_datetime.getNanos());
    }
    if (null == this.pu_location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.pu_location_id);
    }
    if (null == this.do_location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.do_location_id);
    }
    if (null == this.trip_miles) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.trip_miles);
    }
    if (null == this.trip_time) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.trip_time);
    }
    if (null == this.base_passenger_fare) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.base_passenger_fare);
    }
    if (null == this.tolls) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tolls);
    }
    if (null == this.bcf) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.bcf);
    }
    if (null == this.sales_tax) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.sales_tax);
    }
    if (null == this.congestion_surcharge) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.congestion_surcharge);
    }
    if (null == this.airport_fee) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.airport_fee);
    }
    if (null == this.tips) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.tips);
    }
    if (null == this.driver_pay) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeDouble(this.driver_pay);
    }
    if (null == this.shared_request_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, shared_request_flag);
    }
    if (null == this.shared_match_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, shared_match_flag);
    }
    if (null == this.access_a_ride_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, access_a_ride_flag);
    }
    if (null == this.wav_request_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wav_request_flag);
    }
    if (null == this.wav_match_flag) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, wav_match_flag);
    }
    if (null == this.___index_level_0__) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.___index_level_0__);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(hvfhs_license_num==null?"null":hvfhs_license_num, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dispatching_base_num==null?"null":dispatching_base_num, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(originating_base_num==null?"null":originating_base_num, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(request_datetime==null?"null":"" + request_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(on_scene_datetime==null?"null":"" + on_scene_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pickup_datetime==null?"null":"" + pickup_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dropoff_datetime==null?"null":"" + dropoff_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pu_location_id==null?"null":"" + pu_location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(do_location_id==null?"null":"" + do_location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(trip_miles==null?"null":"" + trip_miles, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(trip_time==null?"null":"" + trip_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(base_passenger_fare==null?"null":"" + base_passenger_fare, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tolls==null?"null":"" + tolls, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bcf==null?"null":"" + bcf, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sales_tax==null?"null":"" + sales_tax, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(congestion_surcharge==null?"null":"" + congestion_surcharge, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_fee==null?"null":"" + airport_fee, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tips==null?"null":"" + tips, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(driver_pay==null?"null":"" + driver_pay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(shared_request_flag==null?"null":shared_request_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(shared_match_flag==null?"null":shared_match_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(access_a_ride_flag==null?"null":access_a_ride_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wav_request_flag==null?"null":wav_request_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wav_match_flag==null?"null":wav_match_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(___index_level_0__==null?"null":"" + ___index_level_0__, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(hvfhs_license_num==null?"null":hvfhs_license_num, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dispatching_base_num==null?"null":dispatching_base_num, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(originating_base_num==null?"null":originating_base_num, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(request_datetime==null?"null":"" + request_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(on_scene_datetime==null?"null":"" + on_scene_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pickup_datetime==null?"null":"" + pickup_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dropoff_datetime==null?"null":"" + dropoff_datetime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(pu_location_id==null?"null":"" + pu_location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(do_location_id==null?"null":"" + do_location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(trip_miles==null?"null":"" + trip_miles, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(trip_time==null?"null":"" + trip_time, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(base_passenger_fare==null?"null":"" + base_passenger_fare, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tolls==null?"null":"" + tolls, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(bcf==null?"null":"" + bcf, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sales_tax==null?"null":"" + sales_tax, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(congestion_surcharge==null?"null":"" + congestion_surcharge, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(airport_fee==null?"null":"" + airport_fee, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(tips==null?"null":"" + tips, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(driver_pay==null?"null":"" + driver_pay, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(shared_request_flag==null?"null":shared_request_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(shared_match_flag==null?"null":shared_match_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(access_a_ride_flag==null?"null":access_a_ride_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wav_request_flag==null?"null":wav_request_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(wav_match_flag==null?"null":wav_match_flag, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(___index_level_0__==null?"null":"" + ___index_level_0__, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.hvfhs_license_num = null; } else {
      this.hvfhs_license_num = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dispatching_base_num = null; } else {
      this.dispatching_base_num = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.originating_base_num = null; } else {
      this.originating_base_num = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.request_datetime = null; } else {
      this.request_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.on_scene_datetime = null; } else {
      this.on_scene_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pickup_datetime = null; } else {
      this.pickup_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dropoff_datetime = null; } else {
      this.dropoff_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pu_location_id = null; } else {
      this.pu_location_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.do_location_id = null; } else {
      this.do_location_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_miles = null; } else {
      this.trip_miles = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_time = null; } else {
      this.trip_time = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.base_passenger_fare = null; } else {
      this.base_passenger_fare = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tolls = null; } else {
      this.tolls = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bcf = null; } else {
      this.bcf = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.sales_tax = null; } else {
      this.sales_tax = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.congestion_surcharge = null; } else {
      this.congestion_surcharge = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.airport_fee = null; } else {
      this.airport_fee = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tips = null; } else {
      this.tips = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.driver_pay = null; } else {
      this.driver_pay = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.shared_request_flag = null; } else {
      this.shared_request_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.shared_match_flag = null; } else {
      this.shared_match_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.access_a_ride_flag = null; } else {
      this.access_a_ride_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wav_request_flag = null; } else {
      this.wav_request_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wav_match_flag = null; } else {
      this.wav_match_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.___index_level_0__ = null; } else {
      this.___index_level_0__ = Long.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.hvfhs_license_num = null; } else {
      this.hvfhs_license_num = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.dispatching_base_num = null; } else {
      this.dispatching_base_num = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.originating_base_num = null; } else {
      this.originating_base_num = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.request_datetime = null; } else {
      this.request_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.on_scene_datetime = null; } else {
      this.on_scene_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pickup_datetime = null; } else {
      this.pickup_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dropoff_datetime = null; } else {
      this.dropoff_datetime = java.sql.Timestamp.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.pu_location_id = null; } else {
      this.pu_location_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.do_location_id = null; } else {
      this.do_location_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_miles = null; } else {
      this.trip_miles = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.trip_time = null; } else {
      this.trip_time = Long.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.base_passenger_fare = null; } else {
      this.base_passenger_fare = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tolls = null; } else {
      this.tolls = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.bcf = null; } else {
      this.bcf = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.sales_tax = null; } else {
      this.sales_tax = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.congestion_surcharge = null; } else {
      this.congestion_surcharge = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.airport_fee = null; } else {
      this.airport_fee = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.tips = null; } else {
      this.tips = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.driver_pay = null; } else {
      this.driver_pay = Double.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.shared_request_flag = null; } else {
      this.shared_request_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.shared_match_flag = null; } else {
      this.shared_match_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.access_a_ride_flag = null; } else {
      this.access_a_ride_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wav_request_flag = null; } else {
      this.wav_request_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.wav_match_flag = null; } else {
      this.wav_match_flag = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.___index_level_0__ = null; } else {
      this.___index_level_0__ = Long.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    fact_fhv_trips o = (fact_fhv_trips) super.clone();
    o.request_datetime = (o.request_datetime != null) ? (java.sql.Timestamp) o.request_datetime.clone() : null;
    o.on_scene_datetime = (o.on_scene_datetime != null) ? (java.sql.Timestamp) o.on_scene_datetime.clone() : null;
    o.pickup_datetime = (o.pickup_datetime != null) ? (java.sql.Timestamp) o.pickup_datetime.clone() : null;
    o.dropoff_datetime = (o.dropoff_datetime != null) ? (java.sql.Timestamp) o.dropoff_datetime.clone() : null;
    return o;
  }

  public void clone0(fact_fhv_trips o) throws CloneNotSupportedException {
    o.request_datetime = (o.request_datetime != null) ? (java.sql.Timestamp) o.request_datetime.clone() : null;
    o.on_scene_datetime = (o.on_scene_datetime != null) ? (java.sql.Timestamp) o.on_scene_datetime.clone() : null;
    o.pickup_datetime = (o.pickup_datetime != null) ? (java.sql.Timestamp) o.pickup_datetime.clone() : null;
    o.dropoff_datetime = (o.dropoff_datetime != null) ? (java.sql.Timestamp) o.dropoff_datetime.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("hvfhs_license_num", this.hvfhs_license_num);
    __sqoop$field_map.put("dispatching_base_num", this.dispatching_base_num);
    __sqoop$field_map.put("originating_base_num", this.originating_base_num);
    __sqoop$field_map.put("request_datetime", this.request_datetime);
    __sqoop$field_map.put("on_scene_datetime", this.on_scene_datetime);
    __sqoop$field_map.put("pickup_datetime", this.pickup_datetime);
    __sqoop$field_map.put("dropoff_datetime", this.dropoff_datetime);
    __sqoop$field_map.put("pu_location_id", this.pu_location_id);
    __sqoop$field_map.put("do_location_id", this.do_location_id);
    __sqoop$field_map.put("trip_miles", this.trip_miles);
    __sqoop$field_map.put("trip_time", this.trip_time);
    __sqoop$field_map.put("base_passenger_fare", this.base_passenger_fare);
    __sqoop$field_map.put("tolls", this.tolls);
    __sqoop$field_map.put("bcf", this.bcf);
    __sqoop$field_map.put("sales_tax", this.sales_tax);
    __sqoop$field_map.put("congestion_surcharge", this.congestion_surcharge);
    __sqoop$field_map.put("airport_fee", this.airport_fee);
    __sqoop$field_map.put("tips", this.tips);
    __sqoop$field_map.put("driver_pay", this.driver_pay);
    __sqoop$field_map.put("shared_request_flag", this.shared_request_flag);
    __sqoop$field_map.put("shared_match_flag", this.shared_match_flag);
    __sqoop$field_map.put("access_a_ride_flag", this.access_a_ride_flag);
    __sqoop$field_map.put("wav_request_flag", this.wav_request_flag);
    __sqoop$field_map.put("wav_match_flag", this.wav_match_flag);
    __sqoop$field_map.put("__index_level_0__", this.___index_level_0__);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("hvfhs_license_num", this.hvfhs_license_num);
    __sqoop$field_map.put("dispatching_base_num", this.dispatching_base_num);
    __sqoop$field_map.put("originating_base_num", this.originating_base_num);
    __sqoop$field_map.put("request_datetime", this.request_datetime);
    __sqoop$field_map.put("on_scene_datetime", this.on_scene_datetime);
    __sqoop$field_map.put("pickup_datetime", this.pickup_datetime);
    __sqoop$field_map.put("dropoff_datetime", this.dropoff_datetime);
    __sqoop$field_map.put("pu_location_id", this.pu_location_id);
    __sqoop$field_map.put("do_location_id", this.do_location_id);
    __sqoop$field_map.put("trip_miles", this.trip_miles);
    __sqoop$field_map.put("trip_time", this.trip_time);
    __sqoop$field_map.put("base_passenger_fare", this.base_passenger_fare);
    __sqoop$field_map.put("tolls", this.tolls);
    __sqoop$field_map.put("bcf", this.bcf);
    __sqoop$field_map.put("sales_tax", this.sales_tax);
    __sqoop$field_map.put("congestion_surcharge", this.congestion_surcharge);
    __sqoop$field_map.put("airport_fee", this.airport_fee);
    __sqoop$field_map.put("tips", this.tips);
    __sqoop$field_map.put("driver_pay", this.driver_pay);
    __sqoop$field_map.put("shared_request_flag", this.shared_request_flag);
    __sqoop$field_map.put("shared_match_flag", this.shared_match_flag);
    __sqoop$field_map.put("access_a_ride_flag", this.access_a_ride_flag);
    __sqoop$field_map.put("wav_request_flag", this.wav_request_flag);
    __sqoop$field_map.put("wav_match_flag", this.wav_match_flag);
    __sqoop$field_map.put("__index_level_0__", this.___index_level_0__);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
