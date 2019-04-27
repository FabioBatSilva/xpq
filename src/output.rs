use prettytable::{format, Cell, Row, Table};
use std::io::Write;

fn to_cell(vec: &Vec<String>) -> Vec<Cell> {
    vec.iter().map(|h| Cell::new(h)).collect()
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
        Self {
            headers: headers,
            values: values,
        }
    }

    pub fn write<W: Write>(&mut self, out: &mut W) -> Result<(), String> {
        let mut table = Table::new();
        let values = &mut self.values;
        let titles = to_cell(&self.headers);

        table.set_titles(Row::new(titles));
        table.set_format(*format::consts::FORMAT_CLEAN);

        for vec in values {
            table.add_row(Row::new(to_cell(&vec)));
        }

        table.print(out).expect("Fail to print table");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::str;

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
        let expected =
            vec![" c1      c2 ", " r1 - 1  r1 - 2 ", " r2 - 1  r2 - 2 ", ""].join("\n");

        assert_eq!(expected, actual);
    }
}
