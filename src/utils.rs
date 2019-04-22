#[cfg(test)]
pub mod test_utils {
    extern crate tempfile;

    use self::tempfile::{Builder, NamedTempFile};
    use std::{fs, path::Path, rc::Rc};

    use parquet::{
        column::writer::ColumnWriter,
        data_type::{ByteArray, Int96},
        file::{
            properties::WriterProperties,
            writer::{FileWriter, SerializedFileWriter},
        },
        schema::parser::parse_message_type,
    };

    pub struct SimpleMessage {
        pub field_int32: i32,
        pub field_int64: i64,
        pub field_float: f32,
        pub field_double: f64,
        pub field_string: String,
        pub field_boolean: bool,
        pub field_timestamp: Vec<u32>,
    }

    pub static SIMPLE_MESSSAGE_SCHEMA: &str = "
        message simple_message {
            OPTIONAL INT32 field_int32;
            OPTIONAL INT64 field_int64;
            OPTIONAL FLOAT field_float;
            OPTIONAL DOUBLE field_double;
            OPTIONAL BYTE_ARRAY field_string (UTF8);
            OPTIONAL BOOLEAN field_boolean;
            OPTIONAL INT96 field_timestamp;
        }
        ";

    pub fn temp_file(name: &str, suffix: &str) -> NamedTempFile {
        return Builder::new()
            .suffix(suffix)
            .prefix(name)
            .rand_bytes(5)
            .tempfile()
            .expect("Fail to create tmp dir");
    }

    pub fn write_simple_message_parquet(path: &Path, msg: &SimpleMessage) {
        let schema = Rc::new(parse_message_type(SIMPLE_MESSSAGE_SCHEMA).unwrap());
        let props = Rc::new(WriterProperties::builder().build());
        let file = fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::Int32ColumnWriter(ref mut typed) = col_writer {
                typed
                    .write_batch(&[msg.field_int32], Some(&[1]), None)
                    .unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::Int64ColumnWriter(ref mut typed) = col_writer {
                typed
                    .write_batch(&[msg.field_int64], Some(&[1]), None)
                    .unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::FloatColumnWriter(ref mut typed) = col_writer {
                typed
                    .write_batch(&[msg.field_float], Some(&[1]), None)
                    .unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::DoubleColumnWriter(ref mut typed) = col_writer {
                typed
                    .write_batch(&[msg.field_double], Some(&[1]), None)
                    .unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::ByteArrayColumnWriter(ref mut typed) = col_writer {
                let string: &str = &msg.field_string;
                let byte_array = ByteArray::from(string);
                typed.write_batch(&[byte_array], Some(&[1]), None).unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::BoolColumnWriter(ref mut typed) = col_writer {
                typed
                    .write_batch(&[msg.field_boolean], Some(&[1]), None)
                    .unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            if let ColumnWriter::Int96ColumnWriter(ref mut typed) = col_writer {
                let vec = msg.field_timestamp.clone();
                let int96 = Int96::from(vec);
                typed.write_batch(&[int96], Some(&[1]), None).unwrap();
            }
            row_group_writer.close_column(col_writer).unwrap();
        };

        writer.close_row_group(row_group_writer).unwrap();
        writer.close().unwrap();
    }
}
