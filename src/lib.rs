use serde::Serialize;
use memchr::{memchr, memchr2};
use std::fmt;

// --- OPTIMIZATIONS ---
use bumpalo::{
    Bump, 
    collections::Vec as BumpVec,
    collections::String as BumpString, // 1. Use Bumpalo's String (Arena)
};
use hashbrown::HashMap as BumpHashMap;
use ahash::RandomState as AHasher;

use serde_json::{Value as JsonValue, Number as JsonNumber, Map as JsonMap};
// --- END OPTIMIZATION PLAN ---

// --- Data Structures ---

/// Represents a numeric value (Integer or Float)
#[derive(Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum FdonNumber {
    Integer(i64),
    Float(f64),
}

/// Represents any FDON value (Zero-Copy)
#[derive(Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum FdonValue<'a, 'bump> {
    Null,
    Bool(bool),
    Number(FdonNumber), // N...
    Timestamp(FdonNumber), // T... (numeric format)
    RawString(&'a str), // S"..."
    EscapedString(BumpString<'bump>), // SE"..."
    Date(&'a str), // D"..."
    Time(&'a str), // T"..." (string format)
    Array(BumpVec<'bump, FdonValue<'a, 'bump>>),
    Object(BumpHashMap<&'a str, FdonValue<'a, 'bump>, AHasher, &'bump Bump>),
}

/// Parse Error type with coordinate information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FdonError {
    pub message: String,
    pub index: usize,
    pub row: usize,
    pub column: usize,
}

impl fmt::Display for FdonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at row {}, column {} (index {})", self.message, self.row, self.column, self.index)
    }
}

impl std::error::Error for FdonError {}

pub type ParseResult<T> = Result<T, FdonError>;

#[inline]
fn index_to_coord(data: &[u8], index: usize) -> (usize, usize) {
    let mut row = 1;
    let mut col = 1;
    for &b in data.iter().take(index) {
        if b == b'\n' {
            row += 1;
            col = 1;
        } else {
            col += 1;
        }
    }
    (row, col)
}

// --- Parser ---

pub struct FdonParser<'a, 'bump> {
    data: &'a [u8],
    index: usize,
    arena: &'bump Bump, 
}

impl<'a, 'bump> FdonParser<'a, 'bump> {
    #[inline(always)]
    pub fn new(input: &'a str, arena: &'bump Bump) -> Self {
        FdonParser {
            data: input.as_bytes(),
            index: 0,
            arena,
        }
    }

    #[inline(always)]
    fn err<T>(&self, message: impl Into<String>, index: usize) -> ParseResult<T> {
        let (row, column) = index_to_coord(self.data, index);
        Err(FdonError {
            message: message.into(),
            index,
            row,
            column,
        })
    }

    // --- Helpers ---
    #[inline(always)]
    fn peek(&self) -> Option<u8> {
        self.data.get(self.index).copied()
    }

    #[inline(always)]
    fn advance(&mut self) {
        self.index += 1;
    }
    
    #[inline(always)]
    fn skip_whitespace(&mut self) {
        while let Some(b) = self.peek() {
            if b.is_ascii_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    #[inline(always)]
    fn consume(&mut self, char: u8) -> ParseResult<()> {
        if self.peek() == Some(char) {
            self.advance();
            Ok(())
        } else {
            let found = self.peek().map(|c| c as char).map(|c| c.to_string()).unwrap_or_else(|| "EOF".to_string());
            self.err(format!("Expected '{}' but found '{}'", char as char, found), self.index)
        }
    }

    // --- Parse Logic ---
    #[inline(always)]
    pub fn parse(&mut self) -> ParseResult<FdonValue<'a, 'bump>> {
        self.skip_whitespace();
        if self.index == self.data.len() {
            return self.err("Empty input", self.index);
        }
        let value = self.parse_value()?;
        self.skip_whitespace();
        if self.index != self.data.len() {
            self.err("Extra data detected at end of file", self.index)
        } else {
            Ok(value)
        }
    }

    #[inline(always)]
    fn parse_value(&mut self) -> ParseResult<FdonValue<'a, 'bump>> {
        self.skip_whitespace();
        let type_char = self.peek().ok_or_else(|| {
            let (row, column) = index_to_coord(self.data, self.index);
            FdonError {
                message: "Unexpected EOF".to_string(),
                index: self.index,
                row,
                column,
            }
        })?;
        self.advance(); 

        match type_char {
            b'O' => self.parse_object(),
            b'A' => self.parse_array(),
            
            b'S' => {
                // Check for SE"..." (Escaped String)
                if self.peek() == Some(b'E') {
                    self.advance(); // consume 'E'
                    self.skip_whitespace();
                    self.parse_escaped_string()
                } else {
                    // S"..." (Raw String)
                    self.skip_whitespace();
                    self.parse_raw_string(FdonValue::RawString)
                }
            }
            
            b'D' => {
                self.skip_whitespace();
                self.parse_raw_string(FdonValue::Date) // D"..."
            }
            
            b'T' => {
                // T (Polymorphic): Could be T"..." (String) or T... (Number)
                self.skip_whitespace();
                if self.peek() == Some(b'"') {
                    // T"..." -> String path
                    self.parse_raw_string(FdonValue::Time)
                } else {
                    // T... -> Number path
                    self.parse_number_internal()
                        .map(FdonValue::Timestamp) // Wrap in Timestamp
                }
            }

            b'N' => {
                // N... -> Number path
                self.skip_whitespace();
                self.parse_number_internal()
                    .map(FdonValue::Number) // Wrap in Number
            }

            b'B' => self.parse_boolean(),
            b'U' => Ok(FdonValue::Null),
            
            _ => self.err(format!("Unknown data type specifier '{}'", type_char as char), self.index - 1),
        }
    }

    // --- Parse Object ---
    fn parse_object(&mut self) -> ParseResult<FdonValue<'a, 'bump>> {
        let hasher = AHasher::new();
        let mut obj = BumpHashMap::with_hasher_in(hasher, self.arena);
        
        self.consume(b'{')?;
        self.skip_whitespace();

        while self.peek() != Some(b'}') {
            let key = self.parse_key()?;
            self.skip_whitespace();
            self.consume(b':')?;
            self.skip_whitespace();
            let value = self.parse_value()?;
            obj.insert(key, value);

            self.skip_whitespace();
            if self.peek() == Some(b',') {
                self.advance();
                self.skip_whitespace();
                if self.peek() == Some(b'}') {
                    return self.err("Trailing comma detected in object", self.index);
                }
            } else if self.peek() != Some(b'}') {
                return self.err("Missing comma or '}' in object", self.index);
            }
        }
        self.consume(b'}')?;
        Ok(FdonValue::Object(obj))
    }

    // --- Parse Key ---
    #[inline(always)]
    fn parse_key(&mut self) -> ParseResult<&'a str> {
        let start = self.index;
        let mut end_key = start;
        let mut found_colon = false;

        while self.index < self.data.len() {
            let b = self.data[self.index];
            if b == b':' {
                found_colon = true;
                break;
            } else if !b.is_ascii_whitespace() {
                end_key = self.index + 1; // track the end of non-whitespace char for the key
            }
            self.index += 1;
        }

        if found_colon {
            let key_slice = &self.data[start..end_key];
            unsafe {
                Ok(std::str::from_utf8_unchecked(key_slice))
            }
        } else {
            self.err("EOF while reading key (':' not found)", self.index)
        }
    }

    // --- Parse Array ---
    fn parse_array(&mut self) -> ParseResult<FdonValue<'a, 'bump>> {
        let mut arr = BumpVec::new_in(self.arena);
        
        self.consume(b'[')?;
        self.skip_whitespace();

        while self.peek() != Some(b']') {
            arr.push(self.parse_value()?);

            self.skip_whitespace();
            if self.peek() == Some(b',') {
                self.advance();
                self.skip_whitespace();
                if self.peek() == Some(b']') {
                    return self.err("Trailing comma detected in array", self.index);
                }
            } else if self.peek() != Some(b']') {
                return self.err("Missing comma or ']' in array", self.index);
            }
        }
        self.consume(b']')?;
        Ok(FdonValue::Array(arr))
    }

    // --- Parse Raw String (S"...", D"...", T"...") ---
    #[inline(always)]
    fn parse_raw_string(
        &mut self, 
        constructor: fn(&'a str) -> FdonValue<'a, 'bump>
    ) -> ParseResult<FdonValue<'a, 'bump>> {
        self.consume(b'"')?;
        let start = self.index;
        let remaining_data = &self.data[self.index..];

        match memchr(b'"', remaining_data) {
            Some(pos) => {
                let end = self.index + pos;
                let val_slice = &self.data[start..end];
                
                self.index = end + 1; 

                let val_str = unsafe { std::str::from_utf8_unchecked(val_slice) };
                
                Ok(constructor(val_str))
            }
            None => self.err("EOF while reading string ('\"' not found)", start),
        }
    }
    
    // --- Parse Escaped String (SE"...") ---
    fn parse_escaped_string(&mut self) -> ParseResult<FdonValue<'a, 'bump>> {
        self.consume(b'"')?;
        
        // Use Bumpalo's String to hold unescaped result
        let mut unescaped_str = BumpString::new_in(self.arena);
        
        let mut start_chunk = self.index;

        // Optimize: Use memchr2 to find \ or " (end)
        while let Some(pos) = memchr2(b'\\', b'"', &self.data[self.index..]) {
            
            let found_char = self.data[self.index + pos];
            
            if found_char == b'"' {
                // --- END OF STRING ---
                let end = self.index + pos;
                let chunk_slice = &self.data[start_chunk..end];
                
                // Add the last chunk (if any)
                if !chunk_slice.is_empty() {
                    unescaped_str.push_str(unsafe { std::str::from_utf8_unchecked(chunk_slice) });
                }
                
                self.index = end + 1; // Skip "
                return Ok(FdonValue::EscapedString(unescaped_str));
            }

            if found_char == b'\\' {
                // --- ESCAPE CHARACTER ---
                
                // 1. Add the previous safe chunk
                let end_chunk = self.index + pos;
                let chunk_slice = &self.data[start_chunk..end_chunk];
                if !chunk_slice.is_empty() {
                    unescaped_str.push_str(unsafe { std::str::from_utf8_unchecked(chunk_slice) });
                }
                
                // 2. Skip the \ character
                self.index = end_chunk + 1;
                
                // 3. Process the escaped character
                match self.peek() {
                    Some(b'n') => unescaped_str.push('\n'),
                    Some(b't') => unescaped_str.push('\t'),
                    Some(b'r') => unescaped_str.push('\r'),
                    Some(b'"') => unescaped_str.push('\"'),
                    Some(b'\\') => unescaped_str.push('\\'),
                    Some(other) => {
                        // Invalid escape character, just keep it as is
                        // (e.g., \a -> a)
                         unescaped_str.push(other as char);
                    }
                    None => return self.err("EOF after escape character '\\'", self.index),
                }
                
                // 4. Advance and reset chunk
                self.advance();
                start_chunk = self.index;
            }
        }

        // If " is not found (EOF error)
        self.err("EOF while reading escaped string ('\"' not found)", self.index)
    }

    // --- Parse Number Internal (Used for both N and T) ---
    #[inline(always)]
    fn parse_number_internal(&mut self) -> ParseResult<FdonNumber> {
        let start = self.index;
        let mut iter_idx = self.index;
        let mut has_float = false;

        while iter_idx < self.data.len() {
            let b = self.data[iter_idx];
            if b == b',' || b == b'}' || b == b']' || b.is_ascii_whitespace() {
                break;
            }
            if b == b'.' {
                has_float = true;
            }
            iter_idx += 1;
        }

        let num_slice = &self.data[start..iter_idx];
        self.index = iter_idx; // Advance index

        if num_slice.is_empty() {
            return self.err("Empty number value", self.index);
        }

        if has_float {
            let val: f64 = fast_float::parse(num_slice)
                .map_err(|e| {
                    let (row, column) = index_to_coord(self.data, start);
                    FdonError {
                        message: format!("Invalid float format: {}", e),
                        index: start,
                        row,
                        column,
                    }
                })?;
            Ok(FdonNumber::Float(val))
        } else {
            let val: i64 = atoi::atoi(num_slice)
                .ok_or_else(|| {
                    let (row, column) = index_to_coord(self.data, start);
                    FdonError {
                        message: "Invalid integer format or out of range".to_string(),
                        index: start,
                        row,
                        column,
                    }
                })?;
            Ok(FdonNumber::Integer(val))
        }
    }

    // --- Parse Boolean ---
    #[inline(always)]
    fn parse_boolean(&mut self) -> ParseResult<FdonValue<'a, 'bump>> {
        if self.data.get(self.index..self.index + 4) == Some(b"true") {
            self.index += 4;
            Ok(FdonValue::Bool(true))
        } else if self.data.get(self.index..self.index + 5) == Some(b"false") {
            self.index += 5;
            Ok(FdonValue::Bool(false))
        } else {
            self.err("Invalid boolean value", self.index)
        }
    }
}

// --- Public APIs ---

/// Parses FDON text allocating to a provided Bumpalo Arena (Zero-Copy where possible).
#[inline]
pub fn parse_fdon_zero_copy_arena<'a, 'bump>(
    input: &'a str,
    arena: &'bump Bump
) -> ParseResult<FdonValue<'a, 'bump>> {
    let mut parser = FdonParser::new(input, arena);
    parser.parse()
}

/// Parses FDON text directly into a standard serde_json::Value.
/// This hides the Bumpalo arena complexity from the user.
#[inline]
pub fn parse_fdon(input: &str) -> Result<JsonValue, FdonError> {
    let arena = Bump::new();
    let val = parse_fdon_zero_copy_arena(input, &arena)?;
    Ok(fdon_to_json(val))
}

/// Helper to convert zero-copy FdonValue to owned serde_json::Value
fn fdon_to_json(val: FdonValue<'_, '_>) -> JsonValue {
    match val {
        FdonValue::Null => JsonValue::Null,
        FdonValue::Bool(b) => JsonValue::Bool(b),
        FdonValue::Number(n) | FdonValue::Timestamp(n) => match n {
            FdonNumber::Integer(i) => JsonValue::Number(JsonNumber::from(i)),
            FdonNumber::Float(f) => JsonNumber::from_f64(f).map(JsonValue::Number).unwrap_or(JsonValue::Null),
        },
        FdonValue::RawString(s) => JsonValue::String(s.to_string()),
        FdonValue::EscapedString(s) => JsonValue::String(s.as_str().to_string()),
        FdonValue::Date(s) => JsonValue::String(s.to_string()),
        FdonValue::Time(s) => JsonValue::String(s.to_string()),
        FdonValue::Array(arr) => {
            let mut j_arr = Vec::with_capacity(arr.len());
            for item in arr {
                j_arr.push(fdon_to_json(item));
            }
            JsonValue::Array(j_arr)
        }
        FdonValue::Object(obj) => {
            let mut j_map = JsonMap::with_capacity(obj.len());
            for (k, v) in obj {
                j_map.insert(k.to_string(), fdon_to_json(v));
            }
            JsonValue::Object(j_map)
        }
    }
}