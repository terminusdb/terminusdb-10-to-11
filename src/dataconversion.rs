use chrono::{NaiveDateTime, NaiveTime};
use lazy_static::lazy_static;
use regex::Regex;
use rug::Integer;
use std::{
    borrow::Cow,
    io::{Cursor, Read},
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataConversionError {
    #[error("value string in store had unexpected format `{0}`")]
    ValueStringHadUnexpectedFormat(String),
    #[error("could not parse value `{value}` as a type `{typ}`")]
    ParseError { value: String, typ: String },
    #[error("unrecognized type `{typ}` of value `{value}`")]
    UnrecognizedType { value: String, typ: String },
}

impl From<DecimalValidationError> for DataConversionError {
    fn from(e: DecimalValidationError) -> Self {
        Self::ParseError {
            value: e.value,
            typ: "http://www.w3.org/2001/XMLSchema#decimal".to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, DataConversionError>;

use terminus_store_11::structure::{
    tfc as tfc_11, AnySimpleType, AnyURI, Base64Binary, Date, DateTimeStamp, DayTimeDuration,
    Decimal, DecimalValidationError, Duration, Entity, GDay, GMonth, GMonthDay, GYear, GYearMonth,
    HexBinary, IDRef, LangString, Language, NCName, NMToken, Name, NegativeInteger,
    NonNegativeInteger, NonPositiveInteger, NormalizedString, PositiveInteger, QName, Token,
    YearMonthDuration, ID,
};

pub enum LangOrType<'a> {
    Lang(&'a str, &'a str),
    Type(&'a str, &'a str),
}

pub fn value_string_to_slices(s: &str) -> Result<LangOrType> {
    if s.is_empty() {
        return Err(DataConversionError::ValueStringHadUnexpectedFormat(
            s.to_string(),
        ));
    }

    if s.as_bytes()[s.len() - 1] == b'\'' {
        let pos = s[..s.len() - 1].rfind('\'');
        if pos.is_none() {
            return Err(DataConversionError::ValueStringHadUnexpectedFormat(
                s.to_string(),
            ));
        }
        let pos = pos.unwrap();
        if pos == 0 {
            return Err(DataConversionError::ValueStringHadUnexpectedFormat(
                s.to_string(),
            ));
        }

        if s.as_bytes()[pos - 1] == b'^' {
            if pos == 1 || s.as_bytes()[pos - 2] != b'^' {
                return Err(DataConversionError::ValueStringHadUnexpectedFormat(
                    s.to_string(),
                ));
            }

            Ok(LangOrType::Type(&s[0..pos - 2], &s[pos + 1..s.len() - 1]))
        } else {
            if s.as_bytes()[pos - 1] != b'@' {
                return Err(DataConversionError::ValueStringHadUnexpectedFormat(
                    s.to_string(),
                ));
            }

            Ok(LangOrType::Lang(&s[..pos - 1], &s[pos..]))
        }
    } else {
        let pos = s.rfind('@');
        if pos.is_none() {
            return Err(DataConversionError::ValueStringHadUnexpectedFormat(
                s.to_string(),
            ));
        }
        let pos = pos.unwrap();

        Ok(LangOrType::Lang(&s[..pos], &s[pos + 1..]))
    }
}

pub fn convert_value_string_to_dict_entry(value: &str) -> Result<tfc_11::TypedDictEntry> {
    let res = value_string_to_slices(value)?;
    Ok(match res {
        LangOrType::Lang(s, l) => {
            <LangString as tfc_11::TdbDataType>::make_entry(&format!("{l}@{s}"))
        }
        LangOrType::Type(s, t) => {
            if t == "http://www.w3.org/2001/XMLSchema#boolean" {
                let b = s == "true";
                <bool as tfc_11::TdbDataType>::make_entry(&b)
            } else if t == "http://www.w3.org/2001/XMLSchema#decimal" {
                let s = normalize_decimal(s)?;
                <Decimal as tfc_11::TdbDataType>::make_entry(&Decimal::new(s.into_owned())?)
            } else if t == "http://www.w3.org/2001/XMLSchema#double" {
                <f64 as tfc_11::TdbDataType>::make_entry(&s.parse::<f64>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#float" {
                <f32 as tfc_11::TdbDataType>::make_entry(&s.parse::<f32>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#byte" {
                <i8 as tfc_11::TdbDataType>::make_entry(&s.parse::<i8>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#short" {
                <i16 as tfc_11::TdbDataType>::make_entry(&s.parse::<i16>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#int" {
                <i32 as tfc_11::TdbDataType>::make_entry(&s.parse::<i32>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#long" {
                <i64 as tfc_11::TdbDataType>::make_entry(&s.parse::<i64>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedByte" {
                <u8 as tfc_11::TdbDataType>::make_entry(&s.parse::<u8>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedShort" {
                <u16 as tfc_11::TdbDataType>::make_entry(&s.parse::<u16>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedInt" {
                <u32 as tfc_11::TdbDataType>::make_entry(&s.parse::<u32>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedLong" {
                <u64 as tfc_11::TdbDataType>::make_entry(&s.parse::<u64>().map_err(|_| {
                    DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    }
                })?)
            } else if t == "http://www.w3.org/2001/XMLSchema#integer" {
                <Integer as tfc_11::TdbDataType>::make_entry(&s.parse::<Integer>().map_err(
                    |_| DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    },
                )?)
            } else if t == "http://www.w3.org/2001/XMLSchema#positiveInteger" {
                <PositiveInteger as tfc_11::TdbDataType>::make_entry(&PositiveInteger(
                    s.parse::<Integer>()
                        .map_err(|_| DataConversionError::ParseError {
                            value: s.to_string(),
                            typ: t.to_string(),
                        })?,
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#nonNegativeInteger" {
                <NonNegativeInteger as tfc_11::TdbDataType>::make_entry(&NonNegativeInteger(
                    s.parse::<Integer>()
                        .map_err(|_| DataConversionError::ParseError {
                            value: s.to_string(),
                            typ: t.to_string(),
                        })?,
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#negativeInteger" {
                <NegativeInteger as tfc_11::TdbDataType>::make_entry(&NegativeInteger(
                    s.parse::<Integer>()
                        .map_err(|_| DataConversionError::ParseError {
                            value: s.to_string(),
                            typ: t.to_string(),
                        })?,
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#nonPositiveInteger" {
                <NonPositiveInteger as tfc_11::TdbDataType>::make_entry(&NonPositiveInteger(
                    s.parse::<Integer>()
                        .map_err(|_| DataConversionError::ParseError {
                            value: s.to_string(),
                            typ: t.to_string(),
                        })?,
                ))
            } else {
                // all these are stringy. we expect them to be surrounded by quotes.
                if s.is_empty() || &s[0..1] != "\"" || &s[s.len() - 1..] != "\"" {
                    return Err(DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    });
                }
                let slice = &s[1..s.len() - 1];

                if t == "http://www.w3.org/2001/XMLSchema#string" {
                    <String as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#time" {
                    let nt: NaiveTime =
                        NaiveTime::parse_from_str(slice, "%H:%M:%S%.f%Z").map_err(|_| {
                            DataConversionError::ParseError {
                                value: s.to_string(),
                                typ: t.to_string(),
                            }
                        })?;
                    <NaiveTime as tfc_11::TdbDataType>::make_entry(&nt)
                } else if t == "http://www.w3.org/2001/XMLSchema#date" {
                    let date: Date = parse_date_from_string(slice)?;
                    <Date as tfc_11::TdbDataType>::make_entry(&date)
                } else if t == "http://www.w3.org/2001/XMLSchema#dateTime" {
                    let datetime = NaiveDateTime::parse_from_str(slice, "%Y-%m-%dT%H:%M:%S%.f%Z")
                        .map_err(|_| DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    })?;
                    <NaiveDateTime as tfc_11::TdbDataType>::make_entry(&datetime)
                } else if t == "http://www.w3.org/2001/XMLSchema#dateTimeStamp" {
                    let datetime = NaiveDateTime::parse_from_str(slice, "%Y-%m-%dT%H:%M:%S%.f%Z")
                        .map_err(|_| DataConversionError::ParseError {
                        value: s.to_string(),
                        typ: t.to_string(),
                    })?;
                    <DateTimeStamp as tfc_11::TdbDataType>::make_entry(&DateTimeStamp(datetime))
                } else if t == "http://www.w3.org/2001/XMLSchema#gYear" {
                    let gyear = parse_gyear(slice)?;
                    <GYear as tfc_11::TdbDataType>::make_entry(&gyear)
                } else if t == "http://www.w3.org/2001/XMLSchema#gMonth" {
                    let gmonth = parse_gmonth(slice)?;
                    <GMonth as tfc_11::TdbDataType>::make_entry(&gmonth)
                } else if t == "http://www.w3.org/2001/XMLSchema#gDay" {
                    let gday = parse_gday(slice)?;
                    <GDay as tfc_11::TdbDataType>::make_entry(&gday)
                } else if t == "http://www.w3.org/2001/XMLSchema#gYearMonth" {
                    let gyearmonth = parse_gyearmonth(slice)?;
                    <GYearMonth as tfc_11::TdbDataType>::make_entry(&gyearmonth)
                } else if t == "http://www.w3.org/2001/XMLSchema#gMonthDay" {
                    let gmonthday = parse_gmonthday(slice)?;
                    <GMonthDay as tfc_11::TdbDataType>::make_entry(&gmonthday)
                } else if t == "http://www.w3.org/2001/XMLSchema#duration" {
                    let duration = parse_duration(slice)?;
                    <Duration as tfc_11::TdbDataType>::make_entry(&duration)
                } else if t == "http://www.w3.org/2001/XMLSchema#yearMonthDuration" {
                    let duration = parse_duration(slice)?;
                    <YearMonthDuration as tfc_11::TdbDataType>::make_entry(&YearMonthDuration(
                        duration,
                    ))
                } else if t == "http://www.w3.org/2001/XMLSchema#dayTimeDuration" {
                    let duration = parse_duration(slice)?;
                    <DayTimeDuration as tfc_11::TdbDataType>::make_entry(&DayTimeDuration(duration))
                } else if t == "http://www.w3.org/2001/XMLSchema#base64Binary" {
                    let mut wrapped_reader = Cursor::new(slice.to_string());
                    let mut decoder =
                        base64::read::DecoderReader::new(&mut wrapped_reader, base64::STANDARD);
                    // handle errors as you normally would
                    let mut result = Vec::new();
                    decoder.read_to_end(&mut result).map_err(|_| {
                        DataConversionError::ParseError {
                            value: s.to_string(),
                            typ: t.to_string(),
                        }
                    })?;
                    <Base64Binary as tfc_11::TdbDataType>::make_entry(&Base64Binary(result))
                } else if t == "http://www.w3.org/2001/XMLSchema#hexBinary" {
                    <HexBinary as tfc_11::TdbDataType>::make_entry(&HexBinary(
                        hex::decode(slice).map_err(|_| DataConversionError::ParseError {
                            value: s.to_string(),
                            typ: t.to_string(),
                        })?,
                    ))
                } else if t == "http://www.w3.org/2001/XMLSchema#anyURI" {
                    <AnyURI as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#language" {
                    <Language as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#normalizedString" {
                    <NormalizedString as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#token" {
                    <Token as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#NMTOKEN" {
                    <NMToken as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#Name" {
                    <Name as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#NCName" {
                    <NCName as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#QName" {
                    <QName as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#ID" {
                    <ID as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#IDREF" {
                    <IDRef as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#ENTITY" {
                    <Entity as tfc_11::TdbDataType>::make_entry(&slice)
                } else if t == "http://www.w3.org/2001/XMLSchema#anySimpleType"
                    || t == "http://terminusdb.com/schema/xdd#coordinate"
                    || t == "http://terminusdb.com/schema/xdd#coordinatePolygon"
                    || t == "http://terminusdb.com/schema/xdd#coordinatePolyline"
                    || t == "http://terminusdb.com/schema/xdd#dateRange"
                    || t == "http://terminusdb.com/schema/xdd#gYearRange"
                    || t == "http://terminusdb.com/schema/xdd#integerRange"
                    || t == "http://terminusdb.com/schema/xdd#decimalRange"
                    || t == "http://terminusdb.com/schema/xdd#json"
                    || t == "http://terminusdb.com/schema/xdd#url"
                    || t == "http://terminusdb.com/schema/xdd#email"
                    || t == "http://terminusdb.com/schema/xdd#html"
                {
                    <AnySimpleType as tfc_11::TdbDataType>::make_entry(&slice)
                } else {
                    return Err(DataConversionError::UnrecognizedType {
                        value: s.to_string(),
                        typ: t.to_string(),
                    });
                }
            }
        }
    })
}

fn parse_gyear(s: &str) -> Result<GYear> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"(-?\d{4})(.*)").unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#gYear".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let year = cap[1].parse::<i64>().map_err(|_| error_mapping())?;
    let offset = parse_offset(&cap[2]).ok_or_else(error_mapping)?;
    Ok(GYear { year, offset })
}

fn parse_gmonth(s: &str) -> Result<GMonth> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"--(\d{2})(.*)").unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#gMonth".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let month = cap[1].parse::<u8>().map_err(|_| error_mapping())?;
    let offset = parse_offset(&cap[2]).ok_or_else(error_mapping)?;
    Ok(GMonth { month, offset })
}

fn parse_gday(s: &str) -> Result<GDay> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"---(\d{2})(.*)").unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#gDay".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let day = cap[1].parse::<u8>().map_err(|_| error_mapping())?;
    let offset = parse_offset(&cap[2]).ok_or_else(error_mapping)?;
    Ok(GDay { day, offset })
}

fn parse_gyearmonth(s: &str) -> Result<GYearMonth> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"(-?\d{4})-(\d{2})(.*)").unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#gYearMonth".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let year = cap[1].parse::<i64>().map_err(|_| error_mapping())?;
    let month = cap[2].parse::<u8>().map_err(|_| error_mapping())?;
    let offset = parse_offset(&cap[3]).ok_or_else(error_mapping)?;
    Ok(GYearMonth {
        year,
        month,
        offset,
    })
}

fn parse_gmonthday(s: &str) -> Result<GMonthDay> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"--(\d{2})-(\d{2})(.*)").unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#gMonthDay".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let month = cap[1].parse::<u8>().map_err(|_| error_mapping())?;
    let day = cap[2].parse::<u8>().map_err(|_| error_mapping())?;
    let offset = parse_offset(&cap[3]).ok_or_else(error_mapping)?;
    Ok(GMonthDay { month, day, offset })
}

fn parse_offset(s: &str) -> Option<i16> {
    if s.is_empty() {
        Some(0)
    } else {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"(\+|-)(\d{2}):(\d{2})").unwrap();
        }
        if let Some(cap) = RE.captures(s) {
            let sign = if cap[1] == *"+" { 1 } else { -1 };
            let h = cap[2].parse::<i16>().ok()?;
            let m = cap[3].parse::<i16>().ok()?;
            Some(sign * h * 60 + m)
        } else {
            Some(0)
        }
    }
}

fn parse_date_from_string(s: &str) -> Result<Date> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"(\d{4})-(\d{2})-(\d{2})(.*)").unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#date".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let year = cap[1].parse::<i64>().map_err(|_| error_mapping())?;
    let month = cap[2].parse::<u8>().map_err(|_| error_mapping())?;
    let day = cap[3].parse::<u8>().map_err(|_| error_mapping())?;
    let offset = parse_offset(&cap[4]).ok_or_else(error_mapping)?;
    Ok(Date {
        year,
        month,
        day,
        offset,
    })
}

fn parse_duration(s: &str) -> Result<Duration> {
    lazy_static! {
        static ref RE:  Regex = Regex::new(
            r"(-?)P((\d{0,4}Y)?)((\d{0,2}M)?)((\d{0,2}D)?)(T?)((\d{0,2}H)?)((\d{0,2}M)?)((\d{0,2}S)?)",
        ).unwrap();
    }
    let error_mapping = || DataConversionError::ParseError {
        value: format!("\"{s}\""),
        typ: "http://www.w3.org/2001/XMLSchema#date".to_string(),
    };

    let cap = RE.captures(s).ok_or_else(error_mapping)?;
    let sign = if cap[1].is_empty() { 1 } else { -1 };
    let year = if cap[2].is_empty() {
        0_i64
    } else {
        cap[2][0..cap[2].len() - 1]
            .parse::<i64>()
            .map_err(|_| error_mapping())?
    };
    let month = if cap[4].is_empty() {
        0_u8
    } else {
        cap[4][0..cap[4].len() - 1]
            .parse::<u8>()
            .map_err(|_| error_mapping())?
    };
    let day = if cap[6].is_empty() {
        0_u8
    } else {
        cap[6][0..cap[6].len() - 1]
            .parse::<u8>()
            .map_err(|_| error_mapping())?
    };
    let (hour, minute, second) = if cap[8].is_empty() {
        (0, 0, 0)
    } else {
        let hour = if cap[9].is_empty() {
            0
        } else {
            cap[9][0..cap[9].len() - 1]
                .parse::<u8>()
                .map_err(|_| error_mapping())?
        };
        let minute = if cap[11].is_empty() {
            0
        } else {
            cap[11][0..cap[11].len() - 1]
                .parse::<u8>()
                .map_err(|_| error_mapping())?
        };
        let second = if cap[13].is_empty() {
            0
        } else {
            cap[13][0..cap[13].len() - 1]
                .parse::<u8>()
                .map_err(|_| error_mapping())?
        };
        (hour, minute, second)
    };
    Ok(Duration {
        sign,
        year,
        month,
        day,
        hour,
        minute,
        second,
    })
}

pub fn normalize_decimal(s: &str) -> std::result::Result<Cow<str>, DecimalValidationError> {
    lazy_static! {
        static ref NORMALIZED_RE: Regex = Regex::new(r"^-?\d+(\.\d+)?$").unwrap();
        static ref SCIENTIFIC_RE: Regex =
            Regex::new(r"^(-?)(\d+)(\.(\d+)?)[Ee]([+-]\d+)$").unwrap();
    }
    if NORMALIZED_RE.is_match(s) {
        eprintln!("already normalized: {s}");
        Ok(Cow::Borrowed(s))
    } else if let Some(cap) = SCIENTIFIC_RE.captures(s) {
        let prefix = &cap[2];
        let suffix = &cap[4];
        eprintln!("{}", &cap[5]);
        let exp = cap[5].parse::<i64>().unwrap();
        let new_decimal = if exp < 0 {
            let padding = "0".repeat(-exp as usize - 1);
            format!("0.{padding}{prefix}{suffix}")
        } else {
            let exp = exp as usize;
            if suffix.len() <= exp {
                let padding = "0".repeat(exp - suffix.len());
                format!("{prefix}{suffix}{padding}")
            } else {
                let beginning = &suffix[..exp];
                let end = &suffix[exp..];
                format!("{prefix}{beginning}.{end}")
            }
        };
        Ok(Cow::Owned(new_decimal))
    } else {
        Err(DecimalValidationError {
            value: s.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_decimal(input: &str, expected: &str) {
        assert_eq!(expected, normalize_decimal(input).unwrap());
    }

    #[test]
    fn check_normalization() {
        check_decimal("1.03432e+10", "10343200000");
        check_decimal("1.03432e-10", "0.000000000103432");
        check_decimal("1.03432e+2", "103.432");
    }
}
