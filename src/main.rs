use clap::Parser;
use serde_json::Value;
use sqlite::Connection;
use std::path::Path;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Sqlite file to migrate
    #[clap(short, long, value_parser)]
    input: String,

    /// Sqlite output file
    #[clap(short, long, value_parser)]
    output: String,
}

fn get_tables(connection: &Connection) -> Vec<String> {
    let cursor = connection
        .prepare("SELECT name FROM sqlite_schema WHERE type = ? AND name NOT LIKE ?")
        .unwrap()
        .bind(1, "table")
        .unwrap()
        .bind(2, "sqlite_%")
        .unwrap()
        .into_cursor();

    cursor.map(|row| row.unwrap().get::<String, _>(0)).collect()
}

fn process_table(connection: &Connection, conn_out: &Connection, table: String) {
    println!("Processing {} table", table);

    conn_out
        .execute(format!("CREATE TABLE '{}' (ID TEXT, json TEXT)", table))
        .unwrap();

    let mut cursor = connection
        .prepare(format!("SELECT ID, json FROM '{}'", table))
        .unwrap()
        .into_cursor();

    while let Some(Ok(row)) = cursor.next() {
        let id = row.get::<String, _>("ID");
        let json = process_row(&id, row.get("json"));
        conn_out
            .execute(format!(
                "INSERT INTO '{}' (ID, json)
        VALUES ('{}', '{}'); ",
                table, &id, json
            ))
            .unwrap();
    }
}

fn process_row(id: &str, json: String) -> String {
    println!("Processing row with key {}", id);

    // Parsing json until it matches
    let tmp_val = serde_json::from_str::<Value>(&json).unwrap();
    let mut tmp = tmp_val.as_str().unwrap().to_owned();
    loop {
        let current_val = serde_json::from_str::<Value>(&tmp).unwrap();
        if current_val.as_str().is_none() {
            break;
        }

        tmp = current_val.as_str().unwrap().to_string();
    }

    tmp
}

fn main() {
    let args = Args::parse();
    if args.input == args.output {
        panic!("Can't have the same output for input");
    }

    println!("Loading {} sqlite file", args.input);

    let connection = sqlite::open(args.input).expect("Couldn't find input sqlite file");
    println!("Sqlite loaded");
    println!("Creating output sqlite file");
    if Path::new(&args.output).exists() {
        panic!("Output file already exist");
    }

    let conn_output = sqlite::open(args.output).expect("Couldn't create output sqlite file");
    println!("Getting tables");

    let tables = get_tables(&connection);
    println!("Found tables {:?}", tables);

    for table in tables {
        process_table(&connection, &conn_output, table);
    }
}
