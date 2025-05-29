// ORM class for table 'dim_location'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 03 18:45:44 MSK 2025
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

public class dim_location extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("location_id", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_location.this.location_id = (Integer)value;
      }
    });
    setters.put("zone", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_location.this.zone = (String)value;
      }
    });
    setters.put("borough", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_location.this.borough = (String)value;
      }
    });
    setters.put("service_zone", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_location.this.service_zone = (String)value;
      }
    });
  }
  public dim_location() {
    init0();
  }
  private Integer location_id;
  public Integer get_location_id() {
    return location_id;
  }
  public void set_location_id(Integer location_id) {
    this.location_id = location_id;
  }
  public dim_location with_location_id(Integer location_id) {
    this.location_id = location_id;
    return this;
  }
  private String zone;
  public String get_zone() {
    return zone;
  }
  public void set_zone(String zone) {
    this.zone = zone;
  }
  public dim_location with_zone(String zone) {
    this.zone = zone;
    return this;
  }
  private String borough;
  public String get_borough() {
    return borough;
  }
  public void set_borough(String borough) {
    this.borough = borough;
  }
  public dim_location with_borough(String borough) {
    this.borough = borough;
    return this;
  }
  private String service_zone;
  public String get_service_zone() {
    return service_zone;
  }
  public void set_service_zone(String service_zone) {
    this.service_zone = service_zone;
  }
  public dim_location with_service_zone(String service_zone) {
    this.service_zone = service_zone;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dim_location)) {
      return false;
    }
    dim_location that = (dim_location) o;
    boolean equal = true;
    equal = equal && (this.location_id == null ? that.location_id == null : this.location_id.equals(that.location_id));
    equal = equal && (this.zone == null ? that.zone == null : this.zone.equals(that.zone));
    equal = equal && (this.borough == null ? that.borough == null : this.borough.equals(that.borough));
    equal = equal && (this.service_zone == null ? that.service_zone == null : this.service_zone.equals(that.service_zone));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dim_location)) {
      return false;
    }
    dim_location that = (dim_location) o;
    boolean equal = true;
    equal = equal && (this.location_id == null ? that.location_id == null : this.location_id.equals(that.location_id));
    equal = equal && (this.zone == null ? that.zone == null : this.zone.equals(that.zone));
    equal = equal && (this.borough == null ? that.borough == null : this.borough.equals(that.borough));
    equal = equal && (this.service_zone == null ? that.service_zone == null : this.service_zone.equals(that.service_zone));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.location_id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.zone = JdbcWritableBridge.readString(2, __dbResults);
    this.borough = JdbcWritableBridge.readString(3, __dbResults);
    this.service_zone = JdbcWritableBridge.readString(4, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.location_id = JdbcWritableBridge.readInteger(1, __dbResults);
    this.zone = JdbcWritableBridge.readString(2, __dbResults);
    this.borough = JdbcWritableBridge.readString(3, __dbResults);
    this.service_zone = JdbcWritableBridge.readString(4, __dbResults);
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
    JdbcWritableBridge.writeInteger(location_id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(zone, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(borough, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(service_zone, 4 + __off, 12, __dbStmt);
    return 4;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeInteger(location_id, 1 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeString(zone, 2 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(borough, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(service_zone, 4 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.location_id = null;
    } else {
    this.location_id = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.zone = null;
    } else {
    this.zone = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.borough = null;
    } else {
    this.borough = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.service_zone = null;
    } else {
    this.service_zone = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.location_id);
    }
    if (null == this.zone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zone);
    }
    if (null == this.borough) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, borough);
    }
    if (null == this.service_zone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, service_zone);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.location_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.location_id);
    }
    if (null == this.zone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, zone);
    }
    if (null == this.borough) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, borough);
    }
    if (null == this.service_zone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, service_zone);
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
    __sb.append(FieldFormatter.escapeAndEnclose(location_id==null?"null":"" + location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zone==null?"null":zone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(borough==null?"null":borough, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(service_zone==null?"null":service_zone, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(location_id==null?"null":"" + location_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(zone==null?"null":zone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(borough==null?"null":borough, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(service_zone==null?"null":service_zone, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.location_id = null; } else {
      this.location_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.zone = null; } else {
      this.zone = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.borough = null; } else {
      this.borough = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.service_zone = null; } else {
      this.service_zone = __cur_str;
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.location_id = null; } else {
      this.location_id = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.zone = null; } else {
      this.zone = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.borough = null; } else {
      this.borough = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.service_zone = null; } else {
      this.service_zone = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    dim_location o = (dim_location) super.clone();
    return o;
  }

  public void clone0(dim_location o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("location_id", this.location_id);
    __sqoop$field_map.put("zone", this.zone);
    __sqoop$field_map.put("borough", this.borough);
    __sqoop$field_map.put("service_zone", this.service_zone);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("location_id", this.location_id);
    __sqoop$field_map.put("zone", this.zone);
    __sqoop$field_map.put("borough", this.borough);
    __sqoop$field_map.put("service_zone", this.service_zone);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
