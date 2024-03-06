use std::path::PathBuf;

use clap::{arg, command, value_parser};
use eyre::Result;
use rocksdb::{IteratorMode, Options, DB};

fn main() {
    let matches = command!()
        .arg(arg!([db] "The scanned db").value_parser(value_parser!(PathBuf)))
        .get_matches();

    let db_path = if let Some(db) = matches.get_one::<PathBuf>("db") {
        db.clone()
    } else {
        PathBuf::from("db")
    };

    if let Err(e) = scan_db(db_path) {
        println!("SCAN FAILED: {e}");
    }
}

pub fn scan_db(db_path: PathBuf) -> Result<()> {
    let db_opts = Options::default();

    let cf_names = DB::list_cf(&db_opts, db_path.clone())?;
    println!("scanning {}", cf_names.join(", "));
    let db = DB::open_cf(&db_opts, db_path, cf_names.clone())?;

    let mut total_num_records: u64 = 0;
    let mut total_key_size: u64 = 0;
    let mut total_value_size: u64 = 0;
    for name in cf_names {
        let mut num_records: u64 = 0;
        let mut key_size: u64 = 0;
        let mut value_size: u64 = 0;
        let cf = db.cf_handle(&name).unwrap();
        let iter = db.iterator_cf(cf, IteratorMode::Start);
        for res in iter {
            num_records += 1;
            match res {
                Err(e) => {
                    println!("scan error: {e}");
                    return Err(e.into());
                }
                Ok((key, value)) => {
                    key_size += key.len() as u64;
                    value_size += value.len() as u64;
                }
            }
        }
        
        println!(
            "{}: records: {}, key size: {}, value size: {}",
            name, num_records, key_size, value_size
        );

        total_num_records += num_records;
        total_key_size += key_size;
        total_value_size += value_size;
    }

    println!("total records: {}", total_num_records);
    println!("total key size: {}", total_key_size);
    println!("total value size: {}", total_value_size);

    Ok(())
}
