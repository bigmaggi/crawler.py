#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define MAX_DOCUMENTS 10000
#define MAX_URL_LENGTH 1000
#define MAX_CONTENT_LENGTH 1010000000
#define MAX_QUERY_LENGTH 1000
#define MAX_SEARCH_RESULTS 10

typedef struct {
    char url[MAX_URL_LENGTH];
    char content[MAX_CONTENT_LENGTH];
} Document;

typedef struct {
    char token[50];
    int count;
} TermFrequency;

typedef struct {
    char url[MAX_URL_LENGTH];
    double score;
} SearchResult;

double idf(char* token, Document* documents, int num_documents);
double bm25(char* query, Document* document, int num_documents, double avg_document_length, TermFrequency* query_tf);
int compare_scores(const void* a, const void* b);
SearchResult* search_documents(Document* documents, int num_documents, char* query, int limit);

int main() {
    // Load documents from file
    FILE* fp = fopen("documents.txt", "r");
    if (fp == NULL) {
        printf("Error: could not open file.\n");
        return 1;
    }
    Document documents[MAX_DOCUMENTS];
    int num_documents = 0;
    while (fgets(documents[num_documents].url, MAX_URL_LENGTH, fp) != NULL) {
        fgets(documents[num_documents].content, MAX_CONTENT_LENGTH, fp);
        num_documents++;
    }
    fclose(fp);

  // Search for a query
    char query[MAX_QUERY_LENGTH];
    printf("Enter a search query: ");
    fgets(query, MAX_QUERY_LENGTH, stdin);
    SearchResult* search_results = search_documents(documents, num_documents, query, MAX_SEARCH_RESULTS);
    if (search_results != NULL) {
        printf("Search results:\n");
        for (int i = 0; i < MAX_SEARCH_RESULTS; i++) {
            printf("%s (%f)\n", search_results[i].url, search_results[i].score);
        }
        free(search_results);
    } else {
        printf("No search results found.\n");
    }

    // Print search results
    printf("Search Results:\n");
    for (int i = 0; i < 10; i++) {
        printf("URL: %s\n", search_results[i].url);
        printf("Score: %f\n\n", search_results[i].score);
    }

    // Free memory
    free(search_results);

    return 0;
}

double idf(char* token, Document* documents, int num_documents) {
    int doc_freq = 0;
    for (int i = 0; i < num_documents; i++) {
        if (strstr(documents[i].content, token) != NULL) {
            doc_freq++;
        }
    }
    return log((double) num_documents / (double) doc_freq);
}

double bm25(char* query, Document* document, int num_documents, double avg_document_length, TermFrequency* query_tf) {
    // Tokenize the query and document
    char* token = strtok(query, " ");
    while (token != NULL) {
        // Calculate the term frequency for the query term in the document
        int tf = 0;
        char* doc_token = strtok(document->content, " ");
        while (doc_token != NULL) {
            if (strcmp(token, doc_token) == 0) {
                tf++;
            }
            doc_token = strtok(NULL, " ");
        }

        // Add the query term frequency to the query term frequency array
        strcpy(query_tf->token, token);
        query_tf->count = tf;
        query_tf++;

        token = strtok(NULL, " ");
    }

    // Calculate the document length
    int document_length = 0;
    char* doc_token = strtok(document->content, " ");
    while (doc_token != NULL) {
        document_length++;
        doc_token = strtok(NULL, " ");
    }

    // Calculate the Okapi BM25 score
    double k1 = 1.2;
    double b = 0.75;
    double score = 0;
    for (int i = 0; i < num_documents; i++) {
        double tf = 0;
        for (int j = 0; j < strlen(query); j++) {
            if (strcmp(query_tf[j].token, token) == 0) {
                tf = query_tf[j].count;
                break;
            }
        }
        double idf_score = idf(token, document, num_documents);
        score += idf_score * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * ((double) document_length / avg_document_length))));
    }

    return score;
}

int compare_scores(const void* a, const void* b) {
    SearchResult* result_a = (SearchResult*) a;
    SearchResult* result_b = (SearchResult*) b;
    if (result_a->score < result_b->score) {
        return 1;
    } else if (result_a->score > result_b->score) {
        return -1;
    } else {
        return 0;
    }
}

SearchResult* search_documents(Document* documents, int num_documents, char* query, int limit) {
    // Calculate the average document length in the corpus
    double avg_document_length = 0;
    for (int i = 0; i < num_documents; i++) {
        int document_length = 0;
        char* token = strtok(documents[i].content, " ");
        while (token != NULL) {
            document_length++;
            token = strtok(NULL, " ");
        }
        avg_document_length += (double) document_length / (double) num_documents;
    }

    // Calculate the Okapi BM25 score for each document and query pair
    SearchResult* search_results = (SearchResult*) malloc(num_documents * sizeof(SearchResult));
    for (int i = 0; i < num_documents; i++) {
        double score = bm25(query, &documents[i], num_documents, avg_document_length, NULL);
        strcpy(search_results[i].url, documents[i].url);
        search_results[i].score = score;
    }

    // Sort the results by score
    qsort(search_results, num_documents, sizeof(SearchResult), compare_scores);

    // Return the top search results
    return search_results;
    return 0;
}