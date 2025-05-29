// ORM class for table 'dim_calendar'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 03 18:46:14 MSK 2025
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

public class dim_calendar extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("date_key", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.date_key = (java.sql.Date)value;
      }
    });
    setters.put("year", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.year = (Integer)value;
      }
    });
    setters.put("quarter", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.quarter = (Integer)value;
      }
    });
    setters.put("month", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.month = (Integer)value;
      }
    });
    setters.put("day", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.day = (Integer)value;
      }
    });
    setters.put("day_of_week", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.day_of_week = (Integer)value;
      }
    });
    setters.put("is_weekend", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        dim_calendar.this.is_weekend = (Boolean)value;
      }
    });
  }
  public dim_calendar() {
    init0();
  }
  private java.sql.Date date_key;
  public java.sql.Date get_date_key() {
    return date_key;
  }
  public void set_date_key(java.sql.Date date_key) {
    this.date_key = date_key;
  }
  public dim_calendar with_date_key(java.sql.Date date_key) {
    this.date_key = date_key;
    return this;
  }
  private Integer year;
  public Integer get_year() {
    return year;
  }
  public void set_year(Integer year) {
    this.year = year;
  }
  public dim_calendar with_year(Integer year) {
    this.year = year;
    return this;
  }
  private Integer quarter;
  public Integer get_quarter() {
    return quarter;
  }
  public void set_quarter(Integer quarter) {
    this.quarter = quarter;
  }
  public dim_calendar with_quarter(Integer quarter) {
    this.quarter = quarter;
    return this;
  }
  private Integer month;
  public Integer get_month() {
    return month;
  }
  public void set_month(Integer month) {
    this.month = month;
  }
  public dim_calendar with_month(Integer month) {
    this.month = month;
    return this;
  }
  private Integer day;
  public Integer get_day() {
    return day;
  }
  public void set_day(Integer day) {
    this.day = day;
  }
  public dim_calendar with_day(Integer day) {
    this.day = day;
    return this;
  }
  private Integer day_of_week;
  public Integer get_day_of_week() {
    return day_of_week;
  }
  public void set_day_of_week(Integer day_of_week) {
    this.day_of_week = day_of_week;
  }
  public dim_calendar with_day_of_week(Integer day_of_week) {
    this.day_of_week = day_of_week;
    return this;
  }
  private Boolean is_weekend;
  public Boolean get_is_weekend() {
    return is_weekend;
  }
  public void set_is_weekend(Boolean is_weekend) {
    this.is_weekend = is_weekend;
  }
  public dim_calendar with_is_weekend(Boolean is_weekend) {
    this.is_weekend = is_weekend;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dim_calendar)) {
      return false;
    }
    dim_calendar that = (dim_calendar) o;
    boolean equal = true;
    equal = equal && (this.date_key == null ? that.date_key == null : this.date_key.equals(that.date_key));
    equal = equal && (this.year == null ? that.year == null : this.year.equals(that.year));
    equal = equal && (this.quarter == null ? that.quarter == null : this.quarter.equals(that.quarter));
    equal = equal && (this.month == null ? that.month == null : this.month.equals(that.month));
    equal = equal && (this.day == null ? that.day == null : this.day.equals(that.day));
    equal = equal && (this.day_of_week == null ? that.day_of_week == null : this.day_of_week.equals(that.day_of_week));
    equal = equal && (this.is_weekend == null ? that.is_weekend == null : this.is_weekend.equals(that.is_weekend));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof dim_calendar)) {
      return false;
    }
    dim_calendar that = (dim_calendar) o;
    boolean equal = true;
    equal = equal && (this.date_key == null ? that.date_key == null : this.date_key.equals(that.date_key));
    equal = equal && (this.year == null ? that.year == null : this.year.equals(that.year));
    equal = equal && (this.quarter == null ? that.quarter == null : this.quarter.equals(that.quarter));
    equal = equal && (this.month == null ? that.month == null : this.month.equals(that.month));
    equal = equal && (this.day == null ? that.day == null : this.day.equals(that.day));
    equal = equal && (this.day_of_week == null ? that.day_of_week == null : this.day_of_week.equals(that.day_of_week));
    equal = equal && (this.is_weekend == null ? that.is_weekend == null : this.is_weekend.equals(that.is_weekend));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.date_key = JdbcWritableBridge.readDate(1, __dbResults);
    this.year = JdbcWritableBridge.readInteger(2, __dbResults);
    this.quarter = JdbcWritableBridge.readInteger(3, __dbResults);
    this.month = JdbcWritableBridge.readInteger(4, __dbResults);
    this.day = JdbcWritableBridge.readInteger(5, __dbResults);
    this.day_of_week = JdbcWritableBridge.readInteger(6, __dbResults);
    this.is_weekend = JdbcWritableBridge.readBoolean(7, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.date_key = JdbcWritableBridge.readDate(1, __dbResults);
    this.year = JdbcWritableBridge.readInteger(2, __dbResults);
    this.quarter = JdbcWritableBridge.readInteger(3, __dbResults);
    this.month = JdbcWritableBridge.readInteger(4, __dbResults);
    this.day = JdbcWritableBridge.readInteger(5, __dbResults);
    this.day_of_week = JdbcWritableBridge.readInteger(6, __dbResults);
    this.is_weekend = JdbcWritableBridge.readBoolean(7, __dbResults);
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
    JdbcWritableBridge.writeDate(date_key, 1 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(year, 2 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(quarter, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(month, 4 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day, 5 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day_of_week, 6 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeBoolean(is_weekend, 7 + __off, -7, __dbStmt);
    return 7;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeDate(date_key, 1 + __off, 91, __dbStmt);
    JdbcWritableBridge.writeInteger(year, 2 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(quarter, 3 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(month, 4 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day, 5 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeInteger(day_of_week, 6 + __off, 5, __dbStmt);
    JdbcWritableBridge.writeBoolean(is_weekend, 7 + __off, -7, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.date_key = null;
    } else {
    this.date_key = new Date(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.year = null;
    } else {
    this.year = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.quarter = null;
    } else {
    this.quarter = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.month = null;
    } else {
    this.month = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.day = null;
    } else {
    this.day = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.day_of_week = null;
    } else {
    this.day_of_week = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.is_weekend = null;
    } else {
    this.is_weekend = Boolean.valueOf(__dataIn.readBoolean());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.date_key) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.date_key.getTime());
    }
    if (null == this.year) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.year);
    }
    if (null == this.quarter) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.quarter);
    }
    if (null == this.month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.month);
    }
    if (null == this.day) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day);
    }
    if (null == this.day_of_week) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day_of_week);
    }
    if (null == this.is_weekend) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.is_weekend);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.date_key) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.date_key.getTime());
    }
    if (null == this.year) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.year);
    }
    if (null == this.quarter) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.quarter);
    }
    if (null == this.month) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.month);
    }
    if (null == this.day) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day);
    }
    if (null == this.day_of_week) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.day_of_week);
    }
    if (null == this.is_weekend) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.is_weekend);
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
    __sb.append(FieldFormatter.escapeAndEnclose(date_key==null?"null":"" + date_key, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(year==null?"null":"" + year, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(quarter==null?"null":"" + quarter, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(month==null?"null":"" + month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day==null?"null":"" + day, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_of_week==null?"null":"" + day_of_week, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(is_weekend==null?"null":"" + is_weekend, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(date_key==null?"null":"" + date_key, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(year==null?"null":"" + year, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(quarter==null?"null":"" + quarter, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(month==null?"null":"" + month, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day==null?"null":"" + day, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(day_of_week==null?"null":"" + day_of_week, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(is_weekend==null?"null":"" + is_weekend, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.date_key = null; } else {
      this.date_key = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.year = null; } else {
      this.year = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.quarter = null; } else {
      this.quarter = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.month = null; } else {
      this.month = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day = null; } else {
      this.day = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day_of_week = null; } else {
      this.day_of_week = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.is_weekend = null; } else {
      this.is_weekend = BooleanParser.valueOf(__cur_str);
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.date_key = null; } else {
      this.date_key = java.sql.Date.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.year = null; } else {
      this.year = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.quarter = null; } else {
      this.quarter = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.month = null; } else {
      this.month = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day = null; } else {
      this.day = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.day_of_week = null; } else {
      this.day_of_week = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.is_weekend = null; } else {
      this.is_weekend = BooleanParser.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    dim_calendar o = (dim_calendar) super.clone();
    o.date_key = (o.date_key != null) ? (java.sql.Date) o.date_key.clone() : null;
    return o;
  }

  public void clone0(dim_calendar o) throws CloneNotSupportedException {
    o.date_key = (o.date_key != null) ? (java.sql.Date) o.date_key.clone() : null;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("date_key", this.date_key);
    __sqoop$field_map.put("year", this.year);
    __sqoop$field_map.put("quarter", this.quarter);
    __sqoop$field_map.put("month", this.month);
    __sqoop$field_map.put("day", this.day);
    __sqoop$field_map.put("day_of_week", this.day_of_week);
    __sqoop$field_map.put("is_weekend", this.is_weekend);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("date_key", this.date_key);
    __sqoop$field_map.put("year", this.year);
    __sqoop$field_map.put("quarter", this.quarter);
    __sqoop$field_map.put("month", this.month);
    __sqoop$field_map.put("day", this.day);
    __sqoop$field_map.put("day_of_week", this.day_of_week);
    __sqoop$field_map.put("is_weekend", this.is_weekend);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
