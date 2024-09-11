mod data_fetcher;
use data_fetcher::{convert_response_to_kline_struct, fetch_klines, Klines};

#[tokio::main]
async fn main() {
    match fetch_klines("ETHUSDT", "1h", 1000).await {
        Ok(response) => match convert_response_to_kline_struct(response) {
            Ok(klines) => {
                let klines_struct: Klines = Klines {
                    klines: klines.clone(),
                };
                let df = klines_struct
                    .klines_to_dataframe()
                    .expect("Failed to created DataFrame");
                println!("{:?}", df);
            }
            Err(e) => eprintln!("Error converting response to kline struct: {}", e),
        },
        Err(e) => eprintln!("Error fetching klines: {}", e),
    }
}
