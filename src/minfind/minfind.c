// vim: et ts=2 sts=2 sw=2

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>

#define MAX_COLS_COUNT 1024
#define LINE_BUFFER_SIZE 4096

static const char* USAGE = "USAGE: minfind <filename>";

void minfind(const char* path);
void find_mse_columns(const char* headers, int headers_count,
    int* h_mse, int* h_mse_icu, int* h_mse_cum);

bool readline(FILE* f, char* buffer, int* len, char** columns, int* columns_count)
{
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
  *len = i;
  columns[0] = buffer;
  int col = 1;
  for (int i=0; i < *len; i++) {
    if (buffer[i] == '\0')
      columns[col++] = &buffer[i+1];
  }
  *columns_count = col;
  return true;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "Invalid command-line arguments.\n");
    fprintf(stderr, "%s\n", USAGE);
    return 1;
  }
  minfind(argv[1]);
}

static char line[LINE_BUFFER_SIZE];
static char* cols[MAX_COLS_COUNT];
static char* column_headers[] = { "mse", "mse_icu", "mse_cum" };

void minfind(const char* path)
{
  FILE* csv_file = fopen(path, "r");
  int line_len, cols_count;
  if (!readline(csv_file, line, &line_len, cols, &cols_count)) {
    fprintf(stderr, "CSV file has no header line.\n");
    exit(1);
  }
  int h_mse = find_column(cols, cols_count, "mse");
  int h_mse_icu = find_column(cols, cols_count, "mse_icu");
  int h_mse_cum = find_column(cols, cols_count, "mse_cum");
  double mse_min, mse_icu_min, mse_cum_min;
  mse_min = mse_icu_min = mse_cum_min = 1e10;
  int line_number = 2;
  while (readline(csv_file, line, &line_len, cols, &cols_count)) {

    line_number++;
  }
}

int find_column(const char* headers, int headers_count, const char* header_to_find)
{
  int found_index = -1;
  for (h=0; h < headers_count; h++) {
    if (!strcmp(header_to_find, headers[h]))
      return h;
  }
  fprintf("Didn't find column '%s', aborting.\n", header_to_find);
  exit(1);
}

