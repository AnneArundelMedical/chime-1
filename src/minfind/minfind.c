// vim: et ts=2 sts=2 sw=2

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

static const char* USAGE = "USAGE: minfind <filename>"

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "Invalid command-line arguments.\n");
    fprintf(stderr, "%s\n", USAGE);
    return 1;
  }
  minfind(argv[1]);
}

#define MAX_HEADER_COUNT 1024
#define LINE_BUFFER_SIZE 4096

static char* headers[MAX_HEADER_COUNT];
static char headers_line[LINE_BUFFER_SIZE];
static char data_line[LINE_BUFFER_SIZE];

void minfind(const char* path)
{
  FILE* csv_file = fopen(path, "r");
  int headers_line_len;
  if (!readline(csv_file, headers_line, &headers_line_len)) {
    fprintf("CSV file has no header line.\n");
    exit(1);
  }
  headers[0] = headers_line;
  int h = 1;
  for (int i=0; i < headers_line_len; i++) {
    if (headers_line[i] == '\0')
      headers[h++] = &headers_line[i+1];
  }
  int headers_count = h;
  int h_mse, h_mse_icu, h_mse_cum;
  find_mse_columns(headers, headers_count, &h_mse, &h_mse_icu, &h_mse_cum);
  find_min_errors(csv_file, 
}

void find_mse_columns(
    const char* headers, int headers_count,
    int* h_mse, int* h_mse_icu, int* h_mse_cum)
{
  *h_mse = -1;
  *h_mse_icu = -1;
  *h_mse_cum = -1;
  for (h=0; h < headers_count; h++) {
    if (!strcmp("mse", headers[h]))
      *h_mse = h;
    else if (!strcmp("mse_icu", headers[h]))
      *h_mse_icu = h;
    else if (!strcmp("mse_cum", headers[h]))
      *h_mse_cum = h;
  }
  if (*h_mse < 0 || *h_mse_icu < 0 || *h_mse_cum < 0) {
    fprintf("Didn't find all error columns, aborting.\n");
    exit(1);
  }
}

bool readline(FILE* f, char* buffer, int* len) {
  if (feof(f))
    return false;
  int c;
  int i = 0;
  while ((c = getc(f)) != EOF) {
    if (c == '\n')
      break;
    if (c == ',')
      buffer[i] = 0;
    else
      buffer[i] = c;
    i++;
    if (i == LINE_BUFFER_SIZE) {
      fprintf(stderr, "Exceeded max line length, aborting.\n");
      exit(1);
    }
  }
  if (ferror(f)) {
    fprintf(stderr, "Error reading file: %s\n", strerror(errno));
    exit(1);
  }
  buffer[i] = 0;
  len = i;
  return true;
}

