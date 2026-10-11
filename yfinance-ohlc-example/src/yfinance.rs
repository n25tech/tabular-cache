use std::fs::{File, read_to_string};
use std::io::prelude::*;
use std::path::Path;
use scraper::{Html, Selector};


fn get_urls_for_ticker_symbol(ticker_symbol: &str, _context: &str) -> String{
    //let url_head = String::from("https://finance.yahoo.com/quote/");
    let url_head = String::from("https://query1.finance.yahoo.com/v8/finance/chart/");
    let mut url = url_head;
    //let history_context =  "/history?p=";
    url.push_str(ticker_symbol);
    url.push_str("?interval=1d&range=5d");
    return url;
}

fn get_website_data(ticker_symbol: &str) {
    println!("ticker_symbol: {}", ticker_symbol);
    let url = get_urls_for_ticker_symbol(ticker_symbol, "");
    println!("{}", url);
    let client = reqwest::blocking::Client::builder()
    .user_agent("Mozilla/5.0")
    .build().unwrap();
    let resp = match client.get(url).send() {
        Err(e) => panic!("couldn't access url for ticker symbol, {} : {}", ticker_symbol, e),
        Ok(resp) => resp.text(),
    };
    let r = match resp {
        Err(e) => panic!("couldn't convert resp to text for symbol, {}: {}", ticker_symbol, e),
        Ok(r) => r,
    };
    println!("resp: {}", r);
    let mut file_path = String::from("data/");
    file_path.push_str(ticker_symbol);
    file_path.push_str("_main");
    file_path.push_str(".rawdata");
    write_string_to_file(r, file_path);
}


fn write_string_to_file(data: String, file_path: String) {
    let path = Path::new(&file_path);
    let display = path.display();

    let mut file = match File::create(&path) {
        Err(e) => panic!("couldn't create {}: {}", display, e),
        Ok(file) => file,
    };

    match file.write_all(data.as_bytes()) {
        Err(e) => panic!("couldn't write to {}: {}", display, e),
        Ok(_) => println!("successfully wrote to {}", display),
    };
}


pub fn get_nse_equity_for_ticker(ticker_symbol: &str) {
    get_website_data(ticker_symbol);
}