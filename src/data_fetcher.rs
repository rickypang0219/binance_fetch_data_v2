use polars::prelude::*;
use reqwest::Error;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize, Clone)]
pub struct Kline {
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub close_time: i64,
    pub quote_asset_volume: f64,
    pub number_of_trades: i32,
    pub taker_buy_base_asset_volume: f64,
    pub taker_buy_quote_asset_volume: f64,
    pub ignore: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Klines {
    pub klines: Vec<Kline>,
}

impl Klines {
    pub fn klines_to_dataframe(&self) -> PolarsResult<DataFrame> {
        let open_time: Vec<i64> = self.klines.iter().map(|k| k.open_time).collect();
        let open: Vec<f64> = self.klines.iter().map(|k| k.open).collect();
        let high: Vec<f64> = self.klines.iter().map(|k| k.high).collect();
        let low: Vec<f64> = self.klines.iter().map(|k| k.low).collect();
        let close: Vec<f64> = self.klines.iter().map(|k| k.close).collect();
        let volume: Vec<f64> = self.klines.iter().map(|k| k.volume).collect();
        let close_time: Vec<i64> = self.klines.iter().map(|k| k.close_time).collect();
        let quote_asset_volume: Vec<f64> =
            self.klines.iter().map(|k| k.quote_asset_volume).collect();
        let number_of_trades: Vec<i32> = self.klines.iter().map(|k| k.number_of_trades).collect();
        let taker_buy_base_asset_volume: Vec<f64> = self
            .klines
            .iter()
            .map(|k| k.taker_buy_base_asset_volume)
            .collect();
        let taker_buy_quote_asset_volume: Vec<f64> = self
            .klines
            .iter()
            .map(|k| k.taker_buy_quote_asset_volume)
            .collect();
        let ignore: Vec<f64> = self.klines.iter().map(|k| k.ignore).collect();

        let df = DataFrame::new(vec![
            Series::new("open_time", open_time),
            Series::new("open", open),
            Series::new("high", high),
            Series::new("low", low),
            Series::new("close", close),
            Series::new("volume", volume),
            Series::new("close_time", close_time),
            Series::new("quote_asset_volume", quote_asset_volume),
            Series::new("number_of_trades", number_of_trades),
            Series::new("taker_buy_base_asset_volume", taker_buy_base_asset_volume),
            Series::new("taker_buy_quote_asset_volume", taker_buy_quote_asset_volume),
            Series::new("ignore", ignore),
        ])?;

        Ok(df)
    }
}

// pub async fn fetch_klines() -> Result<Vec<Vec<Value>>, Error> {
//     let url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1s&limit=1000";
//     let response = reqwest::get(url).await?.json::<Vec<Vec<Value>>>().await?;
//     Ok(response)
// }

pub async fn fetch_klines(
    symbol: &str,
    interval: &str,
    limit: u32,
) -> Result<Vec<Vec<Value>>, Error> {
    let url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval={}&limit={}",
        symbol, interval, limit
    );
    let response = reqwest::get(&url).await?.json::<Vec<Vec<Value>>>().await?;
    Ok(response)
}

pub fn convert_response_to_kline_struct(response: Vec<Vec<Value>>) -> Result<Vec<Kline>, Error> {
    let klines: Vec<Kline> = response
        .into_iter()
        .map(|kline| Kline {
            open_time: kline[0].as_i64().unwrap(),
            open: kline[1].as_str().unwrap().parse::<f64>().unwrap(),
            high: kline[2].as_str().unwrap().parse::<f64>().unwrap(),
            low: kline[3].as_str().unwrap().parse::<f64>().unwrap(),
            close: kline[4].as_str().unwrap().parse::<f64>().unwrap(),
            volume: kline[5].as_str().unwrap().parse::<f64>().unwrap(),
            close_time: kline[6].as_i64().unwrap(),
            quote_asset_volume: kline[7].as_str().unwrap().parse::<f64>().unwrap(),
            number_of_trades: kline[8].as_i64().unwrap() as i32,
            taker_buy_base_asset_volume: kline[9].as_str().unwrap().parse::<f64>().unwrap(),
            taker_buy_quote_asset_volume: kline[10].as_str().unwrap().parse::<f64>().unwrap(),
            ignore: kline[11].as_str().unwrap().parse::<f64>().unwrap(),
        })
        .collect();
    Ok(klines)
}
