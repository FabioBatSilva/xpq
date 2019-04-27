#[cfg(test)]
pub mod test_utils {
    extern crate tempfile;

    use self::tempfile::{Builder, NamedTempFile, TempDir};
    use std::iter;
    use std::{fs, path::Path, rc::Rc};

    use parquet::{
        column::writer::ColumnWriter,
        data_type::{ByteArray, Int96},
        file::{
            properties::WriterProperties,
            writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
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
        Builder::new()
            .suffix(suffix)
            .prefix(name)
            .rand_bytes(5)
            .tempfile()
            .expect("Fail to create tmp file")
    }

    pub fn temp_dir() -> TempDir {
        Builder::new()
            .rand_bytes(5)
            .tempdir()
            .expect("Fail to create tmp file")
    }

    macro_rules! write_next_col_writer {
        ($WRITER:ident, $VARIANT:ident, $VEC:ident, $MAPPER:expr) => {
            if let Some(mut col_writer) = $WRITER.next_column().unwrap() {
                if let ColumnWriter::$VARIANT(ref mut typed) = col_writer {
                    typed
                        .write_batch(
                            $VEC.iter().map($MAPPER).collect::<Vec<_>>().as_slice(),
                            Some(
                                iter::repeat(1)
                                    .take($VEC.len())
                                    .collect::<Vec<_>>()
                                    .as_slice(),
                            ),
                            None,
                        )
                        .unwrap();
                }
                $WRITER.close_column(col_writer).unwrap();
            };
        };
    }

    pub fn write_simple_message_parquet(path: &Path, msg: &SimpleMessage) {
        write_simple_messages_parquet(path, &[msg]);
    }

    pub fn write_simple_messages_parquet(path: &Path, vec: &[&SimpleMessage]) {
        let schema = Rc::new(parse_message_type(SIMPLE_MESSSAGE_SCHEMA).unwrap());
        let props = Rc::new(WriterProperties::builder().build());
        let file = fs::File::create(path).unwrap();
        let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
        let mut row_group_writer = writer.next_row_group().unwrap();

        write_simple_row_group(&mut row_group_writer, vec);

        writer.close_row_group(row_group_writer).unwrap();
        writer.close().unwrap();
    }

    #[allow(clippy::borrowed_box)]
    fn write_simple_row_group(
        row_group_writer: &mut Box<RowGroupWriter>,
        vec: &[&SimpleMessage],
    ) {
        write_next_col_writer!(row_group_writer, Int32ColumnWriter, vec, |m| {
            m.field_int32
        });
        write_next_col_writer!(row_group_writer, Int64ColumnWriter, vec, |m| {
            m.field_int64
        });
        write_next_col_writer!(row_group_writer, FloatColumnWriter, vec, |m| {
            m.field_float
        });
        write_next_col_writer!(row_group_writer, DoubleColumnWriter, vec, |m| {
            m.field_double
        });
        write_next_col_writer!(row_group_writer, ByteArrayColumnWriter, vec, |m| {
            let string: &str = &m.field_string;

            ByteArray::from(string)
        });
        write_next_col_writer!(row_group_writer, BoolColumnWriter, vec, |m| {
            m.field_boolean
        });
        write_next_col_writer!(row_group_writer, Int96ColumnWriter, vec, |m| {
            let vec = m.field_timestamp.clone();

            Int96::from(vec)
        });
    }
}
