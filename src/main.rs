use std::env;
use std::fs;
use std::process;
use std::time::Instant;

// Import the new ergonomic API and Error structure
use fdon::{parse_fdon, FdonError};

// Helper function to print error with context
fn print_error(error: FdonError, raw_content: &str) -> ! {
    eprintln!("FDON Syntax Error: {}", error);
    
    // Only print a part of the content if it's too long
    const MAX_LEN: usize = 100;
    if raw_content.len() > MAX_LEN {
         let start = if error.index > MAX_LEN / 2 { error.index - MAX_LEN / 2 } else { 0 };
         let end = std::cmp::min(raw_content.len(), start + MAX_LEN);
         eprintln!("...{}...", &raw_content[start..end]);
         // Calculate ^ position
         if error.index >= start {
            eprintln!("{}^", " ".repeat(error.index - start));
         } else {
            eprintln!("^ (Error at start)");
         }
    } else {
        eprintln!("{}", raw_content);
        eprintln!("{}^", " ".repeat(error.index));
    }
    
    process::exit(1);
}

fn main() {
    // --- Argument handling ---
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <filename>.fdon", args[0]);
        process::exit(1);
    }
    let filename = &args[1];

    // --- Read file ---
    let content = match fs::read_to_string(filename) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: File not found or read error for '{}': {}", filename, e);
            process::exit(1);
        }
    };

    println!("--- FDON Process Timing ---");
    println!("Data Size: {} bytes", content.len());
    println!("{}", "-".repeat(30));

    // --- High-level Parse (Zero-Copy internal with API Ergonomics) ---
    
    let start_time_parse = Instant::now();
    
    // 'value' is a standard serde_json::Value without lifetime issues
    let value = match parse_fdon(&content) {
        Ok(v) => v,
        Err(e) => print_error(e, &content),
    };

    let duration_parse = start_time_parse.elapsed(); 

    // --- Serialization and Output ---
    let start_time_serialize = Instant::now();

    let json_output = serde_json::to_string(&value)
        .unwrap_or_else(|e| format!("Error serializing to JSON: {}", e));

    let duration_serialize = start_time_serialize.elapsed();

    // --- Print Results ---
    println!("--- Result (JSON) ---");
    let sample = json_output.chars().take(100).collect::<String>();
    println!("Sample (first 100 chars): {}", sample);
    println!("Total JSON size: {} bytes", json_output.len());
    println!("{}", "-".repeat(30));
    
    // Calculate and print speed
    let duration_parse_ms = duration_parse.as_secs_f64() * 1000.0;
    let duration_serialize_ms = duration_serialize.as_secs_f64() * 1000.0;
    
    println!("--- FDON Process Timing (Summary) ---");
    println!("🚀 Parse Time (API Ergonomics Wrapper): {:.6} ms", duration_parse_ms);
    println!("⚡ Serialize Time: {:.6} ms", duration_serialize_ms);
    println!("Total Time (Parse + Serialize): {:.6} ms", duration_parse_ms + duration_serialize_ms);
    println!("{}", "-".repeat(30));
}