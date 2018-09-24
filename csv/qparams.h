/*

C bindings for Burntsushi's csv parser

*/

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

typedef struct Field Field;

typedef struct Reader Reader;

typedef struct Record Record;

typedef struct RecordsIter RecordsIter;

size_t field_len(const Field *fp);

const char *field_ptr(const Field *fp);

int read_next(RecordsIter *rec_iter_p, Record *rec_p);

void reader_free(Reader *rdr_p);

Reader *reader_from_path(const char *path);

Reader *reader_from_stdin(void);

RecordsIter *reader_iter(Reader *rdr_p);

const Field *record_end_ts(const Record *rp);

const Field *record_host(const Record *rp);

Record *record_new(void);

const Field *record_start_ts(const Record *rp);
