use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

const MAX_QUERY_LENGTH: usize = 50;
const MAX_SEARCH_RESULTS: usize = 10;
const K1: f64 = 1.2;
const B: f64 = 0.75;

struct Document {
    url: String,
    content: String,
}

struct TermFrequency {
    token: String,
    count: i32,
}

fn read_documents(filename: &str) -> Vec<Document> {
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);
    let mut documents = Vec::new();
    let mut url = String::new();
    let mut content = String::new();
    for line in reader.lines() {
        let line = line.unwrap();
        if line.starts_with("#url") {
            url = line[5..].trim().to_string();
        } else if line.starts_with("#content") {
            content = line[9..].trim().to_string();
        } else if line.trim().is_empty() {
            if !url.is_empty() && !content.is_empty() {
                documents.push(Document { url, content });
                url = String::new();
                content = String::new();
            }
        }
    }
    if !url.is_empty() && !content.is_empty() {
        documents.push(Document { url, content });
    }
    documents
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .collect()
}

fn compute_term_frequencies(document: &Document) -> Vec<TermFrequency> {
    let tokens = tokenize(&document.content);
    let mut term_frequencies = HashMap::new();
    for token in tokens {
        *term_frequencies.entry(token).or_insert(0) += 1;
    }
    let mut term_frequencies = term_frequencies
        .into_iter()
        .map(|(token, count)| TermFrequency { token, count })
        .collect::<Vec<_>>();
    term_frequencies.sort_by_key(|tf| tf.token.clone());
    term_frequencies
}

fn compute_document_lengths(documents: &[Document]) -> Vec<f64> {
    documents
        .iter()
        .map(|document| {
            let term_frequencies = compute_term_frequencies(document);
            let mut document_length = 0.0;
            for tf in term_frequencies {
                document_length += tf.count as f64;
            }
            document_length
        })
        .collect()
}

fn compute_average_document_length(document_lengths: &[f64]) -> f64 {
    let total_document_length: f64 = document_lengths.iter().sum();
    total_document_length / document_lengths.len() as f64
}

fn compute_inverse_document_frequencies(documents: &[Document]) -> HashMap<String, f64> {
    let mut inverse_document_frequencies = HashMap::new();
    let num_documents = documents.len() as f64;
    for document in documents {
        let term_frequencies = compute_term_frequencies(document);
        for tf in term_frequencies {
            *inverse_document_frequencies.entry(tf.token).or_insert(0.0) += 1.0;
        }
    }
    for idf in inverse_document_frequencies.values_mut() {
        *idf = (num_documents / *idf).ln();
    }
    inverse_document_frequencies
}

fn compute_bm25(query: &str, document: &Document, document_length: f64, avg_document_length: f64, inverse_document_frequencies: &HashMap<String, f64>) -> f64 {
    let k = K1 * ((1.0 - B) + B * (document_length / avg_document_length));
    let tokens = tokenize(query);
    let mut score = 0.0;
    for token in tokens {
        if let Some(idf) = inverse_document_frequencies.get(&token) {
            let term_frequencies = compute_term_frequencies(document);
            let tf = term_frequencies.iter().find(|tf| tf.token == token).map(|tf| tf.count).unwrap_or(0);
            score += idf * ((tf as f64) * (K1 + 1.0)) / (tf as f64 + k);
        }
    }
    score
}

fn search_documents(documents: &[Document], query: &str, limit: usize) -> Vec<(String, f64)> {
    let document_lengths = compute_document_lengths(documents);
    let avg_document_length = compute_average_document_length(&document_lengths);
    let inverse_document_frequencies = compute_inverse_document_frequencies(documents);
    let mut search_results = Vec::new();
    for document in documents {
        let document_length = document_lengths.iter().find(|&len| document_length == *len).unwrap();
        let score = compute_bm25(query, document, *document_length, avg_document_length, &inverse_document_frequencies);
        search_results.push((document.url.clone(), score));
    }
    search_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    search_results.truncate(limit);
    search_results
        .into_iter()
        .map(|(url, score)| (url, score))
        .collect()
}

fn main() {
    let documents = read_documents("documents.txt");
    loop {
        let mut query = String::new();
        println!("Enter a search query:");
        std::io::stdin().read_line(&mut query).unwrap();
        let search_results = search_documents(&documents, &query, MAX_SEARCH_RESULTS);
        if !search_results.is_empty() {
            println!("Search results:");
            for (url, score) in search_results {
                println!("{} ({})", url, score);
            }
        } else {
            println!("No search results found.");
        }
    }
}