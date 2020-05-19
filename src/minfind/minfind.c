// vim: et ts=2 sts=2 sw=2

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <limits.h>

#define MAX_COLS_COUNT 1024
#define LINE_BUFFER_SIZE 4096

static const char* USAGE = "USAGE: minfind <filename>";

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

double parse_double(const char* str)
{
  char* endptr;
  double d = strtod(str, &endptr);
  if (endptr == str || *endptr != 0) {
    fprintf(stderr, "Unable to parse double.\n");
    exit(1);
  }
  return d;
}

int parse_int(const char* str)
{
  char* endptr;
  long x = strtol(str, &endptr, 10);
  if (endptr == str || *endptr != 0) {
    fprintf(stderr, "Unable to parse int.\n");
    exit(1);
  }
  if (x < INT_MIN || x > INT_MAX) {
    fprintf(stderr, "Int out of range.\n");
    exit(1);
  }
  return (int)x;
}

int find_column(char** headers, int headers_count, const char* header_to_find)
{
  for (int h=0; h < headers_count; h++) {
    if (!strcmp(header_to_find, headers[h]))
      return h;
  }
  fprintf(stderr, "Didn't find column '%s', aborting.\n", header_to_find);
  exit(1);
}

static char line[LINE_BUFFER_SIZE];
static char* cols[MAX_COLS_COUNT];

void minfind(const char* path)
{
  FILE* csv_file = fopen(path, "r");
  int line_len, cols_count;
  if (!readline(csv_file, line, &line_len, cols, &cols_count)) {
    fprintf(stderr, "CSV file has no header line.\n");
    exit(1);
  }
  char** cs = cols;
  int h_mse = find_column(cs, cols_count, "mse");
  //int h_mse_icu = find_column(cs, cols_count, "mse_icu");
  //int h_mse_cum = find_column(cs, cols_count, "mse_cum");
  int h_param_set_id = find_column(cs, cols_count, "param_set_id");
  double mse_min, mse_icu_min, mse_cum_min;
  int mse_min_psi, mse_icu_min_psi, mse_cum_min_psi;
  mse_min = mse_icu_min = mse_cum_min = 1e10;
  mse_min_psi = mse_icu_min_psi = mse_cum_min_psi = -1;
  int line_number = 2;
  while (readline(csv_file, line, &line_len, cols, &cols_count)) {
    double mse = parse_double(cols[h_mse]);
    //double mse_icu = parse_double(cols[h_mse_icu]);
    //double mse_cum = parse_double(cols[h_mse_cum]);
    if (mse < mse_min) {
      mse_min = mse;
      mse_min_psi = parse_int(cols[h_param_set_id]);
    }
    line_number++;
  }
}

int main(int argc, char** argv) {
  if (argc != 2) {
    fprintf(stderr, "Invalid command-line arguments.\n");
    fprintf(stderr, "%s\n", USAGE);
    return 1;
  }
  minfind(argv[1]);
}

