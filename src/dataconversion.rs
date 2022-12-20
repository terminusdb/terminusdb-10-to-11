use chrono::{NaiveDateTime, NaiveTime};
use regex::Regex;
use rug::Integer;
use std::io::{Cursor, Read};

use terminus_store_11::structure::{
    tfc as tfc_11, AnyURI, Base64Binary, Date, DateTimeStamp, DayTimeDuration, Decimal, Duration,
    Entity, GDay, GMonth, GMonthDay, GYear, GYearMonth, HexBinary, IDRef, LangString, Language,
    NCName, NMToken, Name, NegativeInteger, NonNegativeInteger, NonPositiveInteger,
    NormalizedString, PositiveInteger, QName, Token, YearMonthDuration, ID,
};

pub enum LangOrType<'a> {
    Lang(&'a str, &'a str),
    Type(&'a str, &'a str),
}

pub fn value_string_to_slices(s: &str) -> LangOrType {
    // The format of these value strings is something like
    if s.as_bytes()[s.len() - 1] == b'\'' {
        let pos = s[..s.len() - 1].rfind('\'').unwrap();
        if s.as_bytes()[pos - 1] == b'^' {
            assert!(s.as_bytes()[pos - 2] == b'^');
            LangOrType::Type(&s[0..pos - 2], &s[pos + 2..s.len() - 1])
        } else {
            assert!(s.as_bytes()[pos - 1] == b'@');
            LangOrType::Lang(&s[..pos - 1], &s[pos..])
        }
    } else {
        let pos = s.rfind('@').unwrap();
        LangOrType::Lang(&s[..pos], &s[pos + 1..])
    }
}

pub fn convert_value_string_to_dict_entry(value: &str) -> tfc_11::TypedDictEntry {
    let res = value_string_to_slices(value);
    match res {
        LangOrType::Lang(s, l) => {
            <LangString as tfc_11::TdbDataType>::make_entry(&format!("{l}@{s}"))
        }
        LangOrType::Type(s, t) => {
            if t == "http://www.w3.org/2001/XMLSchema#string" {
                let s = s[1..s.len() - 1].to_string();
                <String as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#boolean" {
                let b = s == "true";
                <bool as tfc_11::TdbDataType>::make_entry(&b)
            } else if t == "http://www.w3.org/2001/XMLSchema#decimal" {
                <Decimal as tfc_11::TdbDataType>::make_entry(&Decimal(s.to_string()))
            } else if t == "http://www.w3.org/2001/XMLSchema#double" {
                <f64 as tfc_11::TdbDataType>::make_entry(&s.parse::<f64>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#float" {
                <f32 as tfc_11::TdbDataType>::make_entry(&s.parse::<f32>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#time" {
                let s = &s[1..s.len() - 1];
                let nt: NaiveTime = NaiveTime::parse_from_str(s, "%H:%M:%S%.f%Z").unwrap();
                <NaiveTime as tfc_11::TdbDataType>::make_entry(&nt)
            } else if t == "http://www.w3.org/2001/XMLSchema#date" {
                let s = &s[1..s.len() - 1];
                let date: Date = parse_date_from_string(s);
                <Date as tfc_11::TdbDataType>::make_entry(&date)
            } else if t == "http://www.w3.org/2001/XMLSchema#dateTime" {
                let s = &s[1..s.len() - 1];
                let datetime = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%Z").unwrap();
                <NaiveDateTime as tfc_11::TdbDataType>::make_entry(&datetime)
            } else if t == "http://www.w3.org/2001/XMLSchema#dateTimeStamp" {
                let s = &s[1..s.len() - 1];
                let datetime = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f%Z").unwrap();
                <DateTimeStamp as tfc_11::TdbDataType>::make_entry(&DateTimeStamp(datetime))
            } else if t == "http://www.w3.org/2001/XMLSchema#gYear" {
                let s = &s[1..s.len() - 1];
                let gyear = parse_gyear(s);
                <GYear as tfc_11::TdbDataType>::make_entry(&gyear)
            } else if t == "http://www.w3.org/2001/XMLSchema#gMonth" {
                let s = &s[1..s.len() - 1];
                let gmonth = parse_gmonth(s);
                <GMonth as tfc_11::TdbDataType>::make_entry(&gmonth)
            } else if t == "http://www.w3.org/2001/XMLSchema#gDay" {
                let s = &s[1..s.len() - 1];
                let gday = parse_gday(s);
                <GDay as tfc_11::TdbDataType>::make_entry(&gday)
            } else if t == "http://www.w3.org/2001/XMLSchema#gYearMonth" {
                let s = &s[1..s.len() - 1];
                let gyearmonth = parse_gyearmonth(s);
                <GYearMonth as tfc_11::TdbDataType>::make_entry(&gyearmonth)
            } else if t == "http://www.w3.org/2001/XMLSchema#gMonthDay" {
                let s = &s[1..s.len() - 1];
                let gmonthday = parse_gmonthday(s);
                <GMonthDay as tfc_11::TdbDataType>::make_entry(&gmonthday)
            } else if t == "http://www.w3.org/2001/XMLSchema#duration" {
                let s = &s[1..s.len() - 1];
                let duration = parse_duration(s);
                <Duration as tfc_11::TdbDataType>::make_entry(&duration)
            } else if t == "http://www.w3.org/2001/XMLSchema#yearMonthDuration" {
                let s = &s[1..s.len() - 1];
                let duration = parse_duration(s);
                <YearMonthDuration as tfc_11::TdbDataType>::make_entry(&YearMonthDuration(duration))
            } else if t == "http://www.w3.org/2001/XMLSchema#dayTimeDuration" {
                let s = &s[1..s.len() - 1];
                let duration = parse_duration(s);
                <DayTimeDuration as tfc_11::TdbDataType>::make_entry(&DayTimeDuration(duration))
            } else if t == "http://www.w3.org/2001/XMLSchema#byte" {
                <i8 as tfc_11::TdbDataType>::make_entry(&s.parse::<i8>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#short" {
                <i16 as tfc_11::TdbDataType>::make_entry(&s.parse::<i16>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#int" {
                <i32 as tfc_11::TdbDataType>::make_entry(&s.parse::<i32>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#long" {
                <i64 as tfc_11::TdbDataType>::make_entry(&s.parse::<i64>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedByte" {
                <u8 as tfc_11::TdbDataType>::make_entry(&s.parse::<u8>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedShort" {
                <u16 as tfc_11::TdbDataType>::make_entry(&s.parse::<u16>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedInt" {
                <u32 as tfc_11::TdbDataType>::make_entry(&s.parse::<u32>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#unsignedLong" {
                <u64 as tfc_11::TdbDataType>::make_entry(&s.parse::<u64>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#integer" {
                <Integer as tfc_11::TdbDataType>::make_entry(&s.parse::<Integer>().unwrap())
            } else if t == "http://www.w3.org/2001/XMLSchema#positiveInteger" {
                <PositiveInteger as tfc_11::TdbDataType>::make_entry(&PositiveInteger(
                    s.parse::<Integer>().unwrap(),
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#nonNegativeInteger" {
                <NonNegativeInteger as tfc_11::TdbDataType>::make_entry(&NonNegativeInteger(
                    s.parse::<Integer>().unwrap(),
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#negativeInteger" {
                <NegativeInteger as tfc_11::TdbDataType>::make_entry(&NegativeInteger(
                    s.parse::<Integer>().unwrap(),
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#nonPositiveInteger" {
                <NonPositiveInteger as tfc_11::TdbDataType>::make_entry(&NonPositiveInteger(
                    s.parse::<Integer>().unwrap(),
                ))
            } else if t == "http://www.w3.org/2001/XMLSchema#base64Binary" {
                let s = &s[1..s.len() - 1];
                let mut wrapped_reader = Cursor::new(s.to_string());
                let mut decoder =
                    base64::read::DecoderReader::new(&mut wrapped_reader, base64::STANDARD);
                // handle errors as you normally would
                let mut result = Vec::new();
                decoder.read_to_end(&mut result).unwrap();
                <Base64Binary as tfc_11::TdbDataType>::make_entry(&Base64Binary(result))
            } else if t == "http://www.w3.org/2001/XMLSchema#hexBinary" {
                let s = &s[1..s.len() - 1];
                <HexBinary as tfc_11::TdbDataType>::make_entry(&HexBinary(hex::decode(s).unwrap()))
            } else if t == "http://www.w3.org/2001/XMLSchema#anyURI" {
                let s = s[1..s.len() - 1].to_string();
                <AnyURI as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#language" {
                let s = s[1..s.len() - 1].to_string();
                <Language as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#normalizedString" {
                let s = s[1..s.len() - 1].to_string();
                <NormalizedString as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#token" {
                let s = s[1..s.len() - 1].to_string();
                <Token as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#NMTOKEN" {
                let s = s[1..s.len() - 1].to_string();
                <NMToken as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#Name" {
                let s = s[1..s.len() - 1].to_string();
                <Name as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#NCName" {
                let s = s[1..s.len() - 1].to_string();
                <NCName as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#QName" {
                let s = s[1..s.len() - 1].to_string();
                <QName as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#ID" {
                let s = s[1..s.len() - 1].to_string();
                <ID as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#IDREF" {
                let s = s[1..s.len() - 1].to_string();
                <IDRef as tfc_11::TdbDataType>::make_entry(&s)
            } else if t == "http://www.w3.org/2001/XMLSchema#ENTITY" {
                let s = s[1..s.len() - 1].to_string();
                <Entity as tfc_11::TdbDataType>::make_entry(&s)
            } else {
                panic!("We should have exaustive analysis of available types")
            }
        }
    }
}

fn parse_gyear(s: &str) -> GYear {
    let re = Regex::new(r"(-?\d{4})(.*)").unwrap();
    let cap = re.captures(s).unwrap();
    let year = cap[0].parse::<i64>().unwrap();
    let offset = parse_offset(&cap[1]);
    GYear { year, offset }
}

fn parse_gmonth(s: &str) -> GMonth {
    let re = Regex::new(r"--(\d{2})(.*)").unwrap();
    let cap = re.captures(s).unwrap();
    let month = cap[0].parse::<u8>().unwrap();
    let offset = parse_offset(&cap[1]);
    GMonth { month, offset }
}

fn parse_gday(s: &str) -> GDay {
    let re = Regex::new(r"---(\d{2})(.*)").unwrap();
    let cap = re.captures(s).unwrap();
    let day = cap[0].parse::<u8>().unwrap();
    let offset = parse_offset(&cap[1]);
    GDay { day, offset }
}

fn parse_gyearmonth(s: &str) -> GYearMonth {
    let re = Regex::new(r"(-?\d{4})-(\d{2})(.*)").unwrap();
    let cap = re.captures(s).unwrap();
    let year = cap[0].parse::<i64>().unwrap();
    let month = cap[1].parse::<u8>().unwrap();
    let offset = parse_offset(&cap[2]);
    GYearMonth {
        year,
        month,
        offset,
    }
}

fn parse_gmonthday(s: &str) -> GMonthDay {
    let re = Regex::new(r"--(\d{2})-(\d{2})(.*)").unwrap();
    let cap = re.captures(s).unwrap();
    let month = cap[0].parse::<u8>().unwrap();
    let day = cap[1].parse::<u8>().unwrap();
    let offset = parse_offset(&cap[2]);
    GMonthDay { month, day, offset }
}

fn parse_offset(s: &str) -> i16 {
    if s.is_empty() {
        0
    } else {
        let re = Regex::new(r"(\+|-)(\d{2}:\d{2})").unwrap();
        let cap = re.captures(s).unwrap();
        let sign = if cap[0] == *"+" { 1 } else { -1 };
        let h = cap[1].parse::<i16>().unwrap();
        let m = cap[2].parse::<i16>().unwrap();
        sign * h * 60 + m
    }
}

fn parse_date_from_string(s: &str) -> Date {
    let re = Regex::new(r"(\d{4})-(\d{2})-(\d{2})((\+|-)\d{2}:\d{2}){0,1}").unwrap();
    let cap = re.captures(s).unwrap();
    let year = cap[1].parse::<i64>().unwrap();
    let month = cap[2].parse::<u8>().unwrap();
    let day = cap[3].parse::<u8>().unwrap();
    let offset = parse_offset(&cap[4]);
    Date {
        year,
        month,
        day,
        offset,
    }
}

fn parse_duration(s: &str) -> Duration {
    let re = Regex::new(
        r"(-?)P((\d{0,4}Y)?)((\d{0,2}M)?)((\d{0,2}D)?)(T?)((\d{0,2}H)?)((\d{0,2}M)?)((\d{0,2}S)?)",
    )
    .unwrap();
    let cap = re.captures(s).unwrap();
    let sign = if cap[0].is_empty() { 1 } else { -1 };
    let year = if cap[1].is_empty() {
        0_i64
    } else {
        cap[1].parse::<i64>().unwrap()
    };
    let month = if cap[2].is_empty() {
        0_u8
    } else {
        cap[2].parse::<u8>().unwrap()
    };
    let day = if cap[3].is_empty() {
        0_u8
    } else {
        cap[3].parse::<u8>().unwrap()
    };
    let (hour, minute, second) = if cap[4].is_empty() {
        (0, 0, 0)
    } else {
        let hour = if cap[5].is_empty() {
            0
        } else {
            cap[5].parse::<u8>().unwrap()
        };
        let minute = if cap[6].is_empty() {
            0
        } else {
            cap[6].parse::<u8>().unwrap()
        };
        let second = if cap[7].is_empty() {
            0
        } else {
            cap[7].parse::<u8>().unwrap()
        };
        (hour, minute, second)
    };
    Duration {
        sign,
        year,
        month,
        day,
        hour,
        minute,
        second,
    }
}
