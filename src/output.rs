use std::cmp;
use std::io::Write;
use std::str;
use tabwriter::TabWriter;
use unicode_width::UnicodeWidthStr;

#[inline]
fn format_cell(value: &str, width: usize) -> String {
    let length = UnicodeWidthStr::width(value);
    let mut result = value.to_string();

    if length == width {
        return result;
    }

    if length < width {
        result.push_str(&format!("{:^1$}", " ", width - length));

        return result;
    }

    let is_quoted = result.starts_with('"');
    let trim_length = if is_quoted { 4 } else { 3 };
    let truncate = if is_quoted { "...\"" } else { "..." };
    let take = length - (trim_length + (length - width));
    let mut result_trim = result.chars().take(take).collect::<String>();

    result_trim.push_str(truncate);

    result_trim
}

#[inline]
fn format_row(
    index: usize,
    batch_size: usize,
    cells: &[String],
    width: &mut [usize],
) -> Vec<u8> {
    let mut row = cells
        .iter()
        .enumerate()
        .map(|e| {
            // collect max width for first x rows
            if index < batch_size {
                width[e.0] = cmp::max(e.1.len(), width[e.0]);

                return e.1.to_owned();
            }

            format_cell(&e.1, width[e.0])
        })
        .collect::<Vec<_>>()
        .join("\t");

    row.push('\n');

    row.into_bytes()
}

pub struct TableOutputWriter<T> {
    headers: Vec<String>,
    values: T,
}

impl<T> TableOutputWriter<T>
where
    T: Iterator<Item = Vec<String>>,
{
    pub fn new(headers: Vec<String>, values: T) -> Self {
        Self { headers, values }
    }

    pub fn write<W: Write>(&mut self, out: &mut W) -> Result<(), String> {
        let minwidth = 4;
        let batch_size = 500;
        let values = &mut self.values;
        let mut width = vec![minwidth; self.headers.len()];
        let mut tw = TabWriter::new(out).minwidth(minwidth);

        tw.write_all(&format_row(0, batch_size, &self.headers, &mut width))
            .map_err(|e| format!("Failed write headers : {}", e))?;

        for (i, vec) in values.enumerate() {
            tw.write_all(&format_row(i, batch_size, &vec, &mut width))
                .map_err(|e| format!("Failed write row : {}", e))?;

            if i > 0 && i % batch_size == 0 {
                tw.flush()
                    .map_err(|e| format!("Failed flush table : {}", e))?;
            }
        }

        tw.flush()
            .map_err(|e| format!("Failed flush table : {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::str;

    #[test]
    fn test_table_format_cell() {
        assert_eq!("1234...", format_cell("123456789", 7));
        assert_eq!("1234567", format_cell("1234567", 7));
        assert_eq!("1234   ", format_cell("1234", 7));

        assert_eq!("\"12345...\"", format_cell("\"123456789\"", 10));
        assert_eq!("\"12345678\"", format_cell("\"12345678\"", 10));
        assert_eq!("\"1234\"    ", format_cell("\"1234\"", 10));
        assert_eq!("\"\"        ", format_cell("\"\"", 10));
    }

    #[test]
    fn test_table_format_cell_unicode() {
        assert_eq!("tÞyk...", format_cell("tÞykkvibær", 7));
        assert_eq!("Â¿Â¿   ", format_cell("Â¿Â¿", 7));
    }

    #[test]
    fn test_table_format_row() {
        let batch_size = 1;
        let mut width = vec![0; 2];
        let values = vec![
            vec!["12345".to_string(), "123456".to_string()],
            vec!["123456789".to_string(), "123456789".to_string()],
            vec!["".to_string(), "".to_string()],
        ];

        let result1 = format_row(0, batch_size, &values[0], &mut width);
        let result2 = format_row(1, batch_size, &values[1], &mut width);
        let result3 = format_row(2, batch_size, &values[2], &mut width);

        assert_eq!(vec![5, 6], width);
        assert_eq!(13, result1.len());
        assert_eq!(13, result2.len());
        assert_eq!(13, result3.len());

        assert_eq!("12345\t123456\n", str::from_utf8(&result1).unwrap());
        assert_eq!("12...\t123...\n", str::from_utf8(&result2).unwrap());
        assert_eq!("     \t      \n", str::from_utf8(&result3).unwrap());
    }

    #[test]
    fn test_table_output_writer_write() {
        let mut buff = Cursor::new(Vec::new());
        let headers: Vec<String> = vec![String::from("c1"), String::from("c2")];
        let values = vec![
            vec![String::from("r1 - 1"), String::from("r1 - 2")],
            vec![String::from("r2 - 1"), String::from("r2 - 2")],
        ];

        let iter = values.into_iter();
        let mut writer = TableOutputWriter::new(headers, iter);

        writer.write(&mut buff).unwrap();

        let vec = buff.into_inner();
        let actual = str::from_utf8(&vec).unwrap();
        let expected = "c1      c2\nr1 - 1  r1 - 2\nr2 - 1  r2 - 2\n";

        assert_eq!(expected, actual);
    }
}
