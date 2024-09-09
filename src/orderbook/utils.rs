use super::types::MarketType;
use super::MarketError;
use chrono::{Duration, NaiveDateTime};
/// 解析时间戳字符串为 `NaiveDateTime` 对象。
///
/// 时间戳字符串格式应为“年月日时分秒毫秒”，总共17位字符。
///
/// # 参数
/// - `timestamp`: 输入的时间戳字符串，格式必须为“年月日时分秒毫秒”
///
/// # 返回
/// - `Ok(NaiveDateTime)`: 解析成功，返回对应的 `NaiveDateTime` 对象。
/// - `Err(MarketError::ParseError)`: 解析失败，返回 `ParseError`。
///
/// # 示例
/// ```
/// let timestamp = "20230801093939123";
/// let datetime = parse_timestamp(timestamp).unwrap();
/// ```
pub fn parse_timestamp(timestamp: &str) -> Result<NaiveDateTime, MarketError> {
    // 如果输入的时间戳长度不是17，直接返回错误
    if timestamp.len() != 17 {
        return Err(MarketError::ParseError);
    }

    // 定义日期和时间格式：年4位 + 月2位 + 日2位 + 时2位 + 分2位 + 秒2位 + 毫秒3位
    let format = "%Y%m%d%H%M%S%3f";

    // 使用 `chrono::NaiveDateTime::parse_from_str` 解析字符串
    NaiveDateTime::parse_from_str(timestamp, format).map_err(|_| MarketError::ParseError)
}

/// 调整 `NaiveDateTime` 对象的毫秒数。
///
/// 输入一个 `NaiveDateTime` 对象和要调整的毫秒数（正值表示增加，负值表示减少）。
///
/// # 参数
/// - `datetime`: 要调整的 `NaiveDateTime` 对象。
/// - `milliseconds`: 要调整的毫秒数，正值表示增加，负值表示减少。
///
/// # 返回
/// - 返回调整后的 `NaiveDateTime` 对象。
///
/// # 示例
/// ```
/// let datetime = NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
/// let new_datetime = adjust_milliseconds(datetime, 500);
/// ```
pub fn adjust_milliseconds(datetime: NaiveDateTime, milliseconds: i64) -> NaiveDateTime {
    datetime + Duration::milliseconds(milliseconds)
}

/// 将 `NaiveDateTime` 对象格式化为原始时间戳格式字符串。
///
/// # 参数
/// - `datetime`: 要格式化的 `NaiveDateTime` 对象。
///
/// # 返回
/// - 返回格式化后的时间戳字符串，格式为“年月日时分秒毫秒”。
///
/// # 示例
/// ```
/// let datetime = NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
/// let timestamp = format_timestamp(datetime);
/// ```
pub fn format_timestamp(datetime: NaiveDateTime) -> String {
    datetime.format("%Y%m%d%H%M%S%3f").to_string()
}

/// 计算两个 `NaiveDateTime` 对象之间的时间差（以毫秒为单位）。
///
/// # 参数
/// - `datetime1`: 第一个 `NaiveDateTime` 对象。
/// - `datetime2`: 第二个 `NaiveDateTime` 对象。
///
/// # 返回
/// - 返回 `datetime2` 和 `datetime1` 之间的时间差（以毫秒为单位）。如果 `datetime2` 在 `datetime1` 之前，则返回负值。
///
/// # 示例
/// ```
/// let datetime1 = NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
/// let datetime2 = NaiveDateTime::parse_from_str("2023-08-01 09:39:41.123", "%Y-%m-%d %H:%M:%S%.3f").unwrap();
/// let diff = time_difference_ms(datetime1, datetime2);
/// assert_eq!(diff, 2000); // 2000毫秒 = 2秒
/// ```
pub fn time_difference_ms(datetime1: NaiveDateTime, datetime2: NaiveDateTime) -> i64 {
    let duration = datetime2.signed_duration_since(datetime1);
    duration.num_milliseconds()
}

/// 计算两个日期时间戳之间的时间差（以毫秒为单位）。
///
/// 解析两个输入的 `i64` 类型的日期时间戳，并计算它们之间的时间差。
///
/// # 参数
/// - `timestamp1`: 第一个日期时间戳，格式为“年月日时分秒毫秒”。
/// - `timestamp2`: 第二个日期时间戳，格式为“年月日时分秒毫秒”。
///
/// # 返回
/// - 两个时间戳之间的时间差，以毫秒为单位。如果 `timestamp2` 在 `timestamp1` 之前，返回负值。
///
/// # 示例
/// ```
/// let timestamp1: i64 = 20230801093939123;
/// let timestamp2: i64 = 20230801093940123;
/// let diff = time_difference_ms_i64(timestamp1, timestamp2);
/// assert_eq!(diff, 1000); // 两个时间戳之间的差异为1000毫秒
/// ```
///
#[inline(always)]
pub fn time_difference_ms_i64(timestamp1: i64, timestamp2: i64) -> Result<i64, MarketError> {
    // 将 i64 类型的时间戳转换为字符串
    let timestamp1_str = timestamp1.to_string();
    let timestamp2_str = timestamp2.to_string();

    // 解析时间戳字符串为 NaiveDateTime 对象
    let datetime1 = parse_timestamp(&timestamp1_str)?;
    let datetime2 = parse_timestamp(&timestamp2_str)?;

    // 计算时间差
    let duration = datetime2.signed_duration_since(datetime1);
    Ok(duration.num_milliseconds())
}

/// 调整原始格式的日期时间字符串中的毫秒数。
///
/// 解析输入的时间戳字符串，调整指定的毫秒数，然后返回新的时间戳字符串。
///
/// # 参数
/// - `timestamp`: 输入的时间戳字符串，格式必须为“年月日时分秒毫秒”。
/// - `milliseconds`: 要调整的毫秒数，正值表示增加，负值表示减少。
///
/// # 返回
/// - 调整后的时间戳字符串，格式为“年月日时分秒毫秒”。
///
/// # 示例
/// ```
/// let timestamp = "20230801093939123";
/// let new_timestamp = adjust_timestamp_milliseconds(timestamp, 500);
/// assert_eq!(new_timestamp, "20230801093939123"); // 这里应替换为实际调整后的值
/// ```
pub fn adjust_timestamp_milliseconds(
    timestamp: &str,
    milliseconds: i64,
) -> Result<String, MarketError> {
    // 解析时间戳字符串为 NaiveDateTime 对象
    let datetime = parse_timestamp(timestamp)?;

    // 调整时间
    let adjusted_datetime = adjust_milliseconds(datetime, milliseconds);

    // 格式化为原始时间戳格式字符串
    Ok(format_timestamp(adjusted_datetime))
}

/// 调整原始格式的日期时间戳中的毫秒数。
///
/// 解析输入的时间戳 `i64`，调整指定的毫秒数，然后返回新的日期时间戳 `i64`。
///
/// # 参数
/// - `timestamp`: 输入的日期时间戳，格式必须为“年月日时分秒毫秒”，如 `20230801093939123`。
/// - `milliseconds`: 要调整的毫秒数，正值表示增加，负值表示减少。
///
/// # 返回
/// - 调整后的日期时间戳 `i64`，格式为“年月日时分秒毫秒”。
///
/// # 示例
/// ```
/// let timestamp: i64 = 20230801093939123;
/// let new_timestamp = adjust_timestamp_milliseconds_i64(timestamp, 500);
/// assert_eq!(new_timestamp, 20230801093939623); // 这里应替换为实际调整后的值
/// ```
#[inline(always)]
pub fn adjust_timestamp_milliseconds_i64(
    timestamp: i64,
    milliseconds: i64,
) -> Result<i64, MarketError> {
    // 将 i64 类型的时间戳转换为字符串
    let timestamp_str = timestamp.to_string();

    // 解析时间戳字符串为 NaiveDateTime 对象
    let datetime = parse_timestamp(&timestamp_str)?;

    // 调整时间
    let adjusted_datetime = adjust_milliseconds(datetime, milliseconds);

    // 格式化为原始时间戳格式字符串
    let new_timestamp_str = format_timestamp(adjusted_datetime);

    // 将调整后的时间戳字符串转换回 i64
    new_timestamp_str
        .parse::<i64>()
        .map_err(|_| MarketError::InvalidTimestamp)
}

/// 计算两个原始格式的时间戳字符串之间的时间差（以毫秒为单位）。
///
/// 解析两个时间戳字符串，计算它们之间的时间差，然后返回差值（以毫秒为单位）。
///
/// # 参数
/// - `timestamp1`: 第一个时间戳字符串，格式必须为“年月日时分秒毫秒”。
/// - `timestamp2`: 第二个时间戳字符串，格式必须为“年月日时分秒毫秒”。
///
/// # 返回
/// - 返回 `timestamp2` 和 `timestamp1` 之间的时间差（以毫秒为单位）。如果 `timestamp2` 在 `timestamp1` 之前，则返回负值。
///
/// # 示例
/// ```
/// let timestamp1 = "20230801093939123";
/// let timestamp2 = "20230801093941123";
/// let diff = time_difference_ms_from_timestamps(timestamp1, timestamp2).unwrap();
/// assert_eq!(diff, 2000); // 2000毫秒 = 2秒
/// ```
pub fn time_difference_ms_from_timestamps(
    timestamp1: &str,
    timestamp2: &str,
) -> Result<i64, MarketError> {
    let datetime1 = parse_timestamp(timestamp1)?;
    let datetime2 = parse_timestamp(timestamp2)?;
    Ok(time_difference_ms(datetime1, datetime2))
}

/// 判断是否应该调用收盘竞价
#[inline(always)]
pub fn should_call_auction_on_close(
    timestamp: i64,
    market: MarketType,
) -> Result<bool, MarketError> {
    let only_time = timestamp % 1_000_000_000;
    match market {
        MarketType::SH | MarketType::SZ => {
            let should = only_time > 150000000;
            Ok(should)
        }
        _ => Err(MarketError::MarketTypeUnknownError),
    }
}

/// 判断是否处于开盘竞价时间
#[inline(always)]
pub fn is_in_call_auction(timestamp: i64, market: MarketType) -> Result<bool, MarketError> {
    let only_time = timestamp % 1_000_000_000;
    match market {
        MarketType::SH | MarketType::SZ => {
            let yes_or_no: bool = only_time < 93000000 || only_time > 145700000;
            Ok(yes_or_no)
        }
        _ => Err(MarketError::MarketTypeUnknownError),
    }
}

#[inline(always)]
pub fn extract_market_code(stock_code: &str) -> &str {
    stock_code.split('.').last().unwrap_or("SH")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp() {
        let timestamp = "20230801093939123";
        let expected =
            NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();

        match parse_timestamp(timestamp) {
            Ok(datetime) => {
                print!("{:?}\n", datetime);
                assert_eq!(datetime, expected);
            }
            Err(e) => {
                panic!("Failed to parse timestamp: {}", e);
            }
        }
    }

    #[test]
    fn test_parse_timestamp_invalid_length() {
        let timestamp = "2023080109393912"; // 少一个字符
        assert_eq!(parse_timestamp(timestamp), Err(MarketError::ParseError));
    }

    /// 测试 `adjust_milliseconds` 函数的正确性。
    ///
    /// 验证调整 `NaiveDateTime` 对象的毫秒数后是否能正确得到新的 `NaiveDateTime` 对象。
    #[test]
    fn test_adjust_milliseconds() {
        let datetime =
            NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        let adjusted_datetime = adjust_milliseconds(datetime, 500);
        let expected_datetime =
            NaiveDateTime::parse_from_str("2023-08-01 09:39:39.623", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        assert_eq!(adjusted_datetime, expected_datetime);
    }

    /// 测试 `format_timestamp` 函数的正确性。
    ///
    /// 验证 `NaiveDateTime` 对象是否能被正确格式化为时间戳字符串。
    #[test]
    fn test_format_timestamp() {
        let datetime =
            NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        let timestamp = format_timestamp(datetime);
        assert_eq!(timestamp, "20230801093939123");
    }

    /// 测试 `time_difference_ms` 函数的正确性。
    ///
    /// 验证计算两个 `NaiveDateTime` 对象之间的时间差是否正确。
    #[test]
    fn test_time_difference_ms() {
        let datetime1 =
            NaiveDateTime::parse_from_str("2023-08-01 09:39:39.123", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        let datetime2 =
            NaiveDateTime::parse_from_str("2023-08-01 09:39:41.123", "%Y-%m-%d %H:%M:%S%.3f")
                .unwrap();
        let diff = time_difference_ms(datetime1, datetime2);
        assert_eq!(diff, 2000); // 2000毫秒 = 2秒
    }

    /// 测试 `adjust_timestamp_milliseconds` 函数的正确性。
    ///
    /// 验证调整时间戳字符串的毫秒数后是否能正确得到新的时间戳字符串。
    #[test]
    fn test_adjust_timestamp_milliseconds() {
        let timestamp = "20230801093939123";
        let new_timestamp = adjust_timestamp_milliseconds(timestamp, 500).unwrap();
        let expected_timestamp = "20230801093939623"; // 这里应该是调整后的值
        assert_eq!(new_timestamp, expected_timestamp);
    }

    /// 测试 `adjust_timestamp_milliseconds` 函数的负值调整情况。
    ///
    /// 验证当调整的毫秒数为负值时，时间戳是否能正确处理减少的毫秒数。
    #[test]
    fn test_adjust_timestamp_milliseconds_negative() {
        let timestamp = "20230801093939123";
        let new_timestamp = adjust_timestamp_milliseconds(timestamp, -500).unwrap();
        let expected_timestamp = "20230801093938623"; // 500毫秒减去后的值
        assert_eq!(new_timestamp, expected_timestamp);
    }

    /// 测试 `adjust_timestamp_milliseconds` 函数的极端时间戳情况。
    ///
    /// 验证对非常早的时间戳进行调整时，函数是否能够正确处理。
    #[test]
    fn test_adjust_timestamp_milliseconds_extreme() {
        let timestamp = "00010101000000000"; // 0001年1月1日的时间戳
        let new_timestamp = adjust_timestamp_milliseconds(timestamp, 3600000).unwrap();
        let expected_timestamp = "00010101010000000"; // 期望值应为增加3600000毫秒（1小时）后的时间戳
        assert_eq!(new_timestamp, expected_timestamp);
    }

    /// 测试 `time_difference_ms_from_timestamps` 函数的正确性。
    ///
    /// 验证计算两个时间戳字符串之间的时间差是否正确。
    #[test]
    fn test_time_difference_ms_from_timestamps() {
        let timestamp1 = "20230801093939123";
        let timestamp2 = "20230801093941123";
        let diff = time_difference_ms_from_timestamps(timestamp1, timestamp2).unwrap();
        assert_eq!(diff, 2000); // 2000毫秒 = 2秒
    }

    /// 测试 `time_difference_ms_from_timestamps` 函数的无效格式处理。
    ///
    /// 验证时间戳字符串格式不正确时是否会返回 `ParseError`。
    #[test]
    fn test_time_difference_ms_from_timestamps_invalid() {
        let timestamp1 = "20230801093939123";
        let timestamp2 = "2023080109393912"; // 少一个字符
        assert_eq!(
            time_difference_ms_from_timestamps(timestamp1, timestamp2),
            Err(MarketError::ParseError)
        );
    }

    #[test]
    fn test_adjust_timestamp_milliseconds_i64_increase() {
        let timestamp: i64 = 20230801093939123;
        let result = adjust_timestamp_milliseconds_i64(timestamp, 500).unwrap();
        assert_eq!(result, 20230801093939623); // 增加500毫秒后的预期结果
    }

    #[test]
    fn test_adjust_timestamp_milliseconds_i64_decrease() {
        let timestamp: i64 = 20230801093939123;
        let result = adjust_timestamp_milliseconds_i64(timestamp, -123).unwrap();
        assert_eq!(result, 20230801093939000); // 减少123毫秒后的预期结果
    }

    #[test]
    fn test_adjust_timestamp_milliseconds_i64_overflow() {
        let timestamp: i64 = 20230801095959500; // 接近一分钟末尾的时间戳
        let result = adjust_timestamp_milliseconds_i64(timestamp, 600).unwrap();
        assert_eq!(result, 20230801100000100); // 跨越分钟的结果
    }

    #[test]
    fn test_adjust_timestamp_milliseconds_i64_underflow() {
        let timestamp: i64 = 20230801000000100; // 一天开始的时间戳
        let result = adjust_timestamp_milliseconds_i64(timestamp, -200).unwrap();
        assert_eq!(result, 20230731235959900); // 跨越到前一天的结果
    }

    #[test]
    fn test_adjust_timestamp_milliseconds_i64_invalid_timestamp() {
        let timestamp: i64 = 99999999999999999; // 无效的时间戳
        let result = adjust_timestamp_milliseconds_i64(timestamp, 500);
        assert!(result.is_err()); // 应该返回错误
    }

    #[test]
    fn test_time_difference_ms_i64_cross_day() {
        let timestamp1: i64 = 20230801235959900; // 8月1日23:59:59.900
        let timestamp2: i64 = 20230802000000100; // 8月2日00:00:00.100
        let result = time_difference_ms_i64(timestamp1, timestamp2).unwrap();
        assert_eq!(result, 200); // 期望差异为200毫秒
    }

    #[test]
    fn test_time_difference_ms_i64_invalid_timestamp() {
        let timestamp1: i64 = 20230801093939123;
        let timestamp2: i64 = 99999999999999999; // 无效时间戳
        let result = time_difference_ms_i64(timestamp1, timestamp2);
        assert!(result.is_err()); // 应该返回错误
    }
}
