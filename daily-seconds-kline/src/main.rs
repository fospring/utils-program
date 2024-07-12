use anyhow::Result;
use chrono::{Datelike, Days, NaiveDate};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct KlineRow {
    open_time: i64,
    open_price: String,
    high: String,
    low: String,
    close: String,
    volume: String,
    close_time: i64,
    quote_volume: String,
    num_of_trades: u64,
    taker_buy_base_vol: String,
    taker_buy_quote_vol: String,
    unused: String,
}

pub(crate) fn init_log() {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_writer(std::io::stderr)
        .with_thread_ids(true)
        .with_thread_names(true)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_log();
    let mut start_time_ms = NaiveDate::from_ymd_opt(2024, 6, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let max_end_time_ms = NaiveDate::from_ymd_opt(2024, 6, 1)
        .unwrap()
        .and_hms_opt(0, 19, 59)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    tracing::info!(
        "start_time_ms: {}, max_end_time_ms: {}",
        start_time_ms,
        max_end_time_ms
    );
    let mut cache_tick: Vec<KlineRow> = Vec::new();
    let base_url = "https://api.binance.com";
    loop {
        let end_time_ms = std::cmp::min(start_time_ms + 10 * 60_000 - 1, max_end_time_ms);
        let url = format!(
            "{}/api/v3/klines?startTime={}&endTime={}&limit=1000&symbol=ETHUSDC&interval=1s",
            base_url, start_time_ms, end_time_ms
        );
        let mut resp = reqwest::get(url.clone())
            .await?
            .json::<Vec<KlineRow>>()
            .await?;
        tracing::info!("url: {}, response length: {}", url, resp.len());

        let start_time = chrono::DateTime::from_timestamp_millis(start_time_ms).unwrap();
        let next_day = start_time.checked_add_days(Days::new(1)).unwrap();
        let next_day_zero =
            NaiveDate::from_ymd_opt(next_day.year(), next_day.month(), next_day.day())
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
        let next_day_ms = next_day_zero.and_utc().timestamp_millis();
        let last = resp.last().cloned();
        resp = resp
            .into_iter()
            .filter(|r| r.open_time < next_day_ms)
            .collect();
        cache_tick.extend_from_slice(&resp);
        tracing::info!("cache_tick size: {}", cache_tick.len());

        match last {
            None => {
                if end_time_ms >= next_day_ms {
                    write_file(
                        &cache_tick,
                        start_time.year(),
                        start_time.month(),
                        start_time.day(),
                    )?;
                    start_time_ms = next_day_ms;
                    cache_tick.clear();
                } else {
                    start_time_ms = end_time_ms + 1000;
                }
            }
            Some(last) => {
                if last.close_time + 1 >= next_day_ms {
                    // start next day
                    write_file(
                        &cache_tick,
                        start_time.year(),
                        start_time.month(),
                        start_time.day(),
                    )?;
                    start_time_ms = last.open_time + 1000;
                    cache_tick.clear();
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    continue;
                } else {
                    start_time_ms = last.open_time + 1000;
                }
            }
        }
        if end_time_ms >= max_end_time_ms {
            // write last time and exit
            if !cache_tick.is_empty() {
                write_file(
                    &cache_tick,
                    start_time.year(),
                    start_time.month(),
                    start_time.day(),
                )?;
            }
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    Ok(())
}

fn write_file(data: &Vec<KlineRow>, year: i32, month: u32, day: u32) -> Result<()> {
    use csv::WriterBuilder;
    let file_name = format!("ETHUSDC-1s-{}-{:02}-{:02}.csv", year, month, day);
    let path = std::path::Path::new("1s_klines");
    let path = path.join(file_name);
    tracing::info!("data lenth: {}, file path: {:?}", data.len(), path);
    let mut wtr = WriterBuilder::new().has_headers(false).from_path(path)?;
    for rec in data {
        wtr.serialize(rec)?;
    }

    wtr.flush()?;

    Ok(())
}
