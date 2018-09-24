#include <libdill.h>
#include <sqlite3.h>
#include <xxhash.h>

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <stdbool.h>
#include <inttypes.h>
#include <argp.h>

#include "failure.h"
#include "../csv/qparams.h"

#define MAX_WORKERS 16
#define DEFAULT_WORKERS 4
#define QTEMPLATE "\
SELECT \
  STRFTIME('%Y-%m-%d %H:%M', TS) AS BUCKET \
, MAX(USAGE) AS MAX_CPU_USAGE \
, MIN(USAGE) AS MIN_CPU_USAGE  \
FROM CPU_USAGE  \
WHERE HOST = :HOST \
  AND TS BETWEEN :START_TIME AND :END_TIME \
GROUP BY BUCKET \
;\
"
#define NUMQPARAMS 3

typedef struct {
    int64_t duration;
    bool err;
} QDuration;

int 
bind_qparams(
    sqlite3_stmt *stmt,
    const Record *rec
) {
    int err;
    const Field *field = record_host(rec);
    const char *beg = field_ptr(field);
    size_t len = field_len(field);
    err = sqlite3_bind_text(
        stmt,
        1,
        beg,
        len,
        SQLITE_STATIC
    );
    if (err) {
        format_err("could not bind the query parameter 'host'");
        return ERR;
    }
    field = record_start_ts(rec);
    beg = field_ptr(field);
    len = field_len(field);
    err = sqlite3_bind_text(
        stmt,
        2,
        beg,
        len,
        SQLITE_STATIC
    );
    if (err) {
        format_err("could not bind the query parameter 'start_ts'");
        return ERR;
    }
    field = record_end_ts(rec);
    beg = field_ptr(field);
    len = field_len(field);
    err = sqlite3_bind_text(
        stmt,
        3,
        beg,
        len,
        SQLITE_STATIC
    );
    if (err) {
        format_err("could not bind the query parameter 'end_ts'");
        return ERR;
    }
    return OK;
}

int 
qexec(
    sqlite3_stmt *stmt,
    const Record *rec,
    int64_t *duration
) {
    int err = sqlite3_reset(stmt);
    if (err) {
        format_err("could not reset the statement");
        return ERR;
    }

    err = bind_qparams(stmt, rec);
    if (err) {
        format_err("could not bind the parameters");
        return ERR;
    }

    int64_t start = now();
    err = sqlite3_step(stmt);
    *duration = now() - start;
    if (err == SQLITE_ROW) {
        debug("SQLITE_ROW");
        // TODO duration
    } else {
        format_err("warning: no rows returned: '%d'", err);
        return NONE;
    }

    return OK;
}


coroutine void 
worker(int ich, int och, sqlite3 *db) {
    debug("init worker with channel: '%d'", ich);
    Record *rec;
    sqlite3_stmt *stmt;
    int err = sqlite3_prepare_v2(db, QTEMPLATE, -1, &stmt, NULL);
    if (err) {
        format_err("could not prepare SQL statement: %s", sqlite3_errmsg(db));
        return;
    }

    QDuration qdur = {0, true};
    while (1) {
        int err = chrecv(ich, &rec, sizeof(rec), -1);
        if (err || errno == ECANCELED) 
            goto CLEANUP;

        err = qexec(stmt, rec, &qdur.duration);
        if (err) {
            debug("%s", sqlite3_errmsg(db));
        } else {
            qdur.err = false;
        }

        err = chsend(och, &qdur, sizeof(qdur), -1);
        if (err)
            format_err("worker '%d' could not send the output message", ich);
    }

CLEANUP:
    debug("closing worker with channel: '%d'", ich);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    debug("worker with channel: '%d' closed", ich);

    return;
}

coroutine void 
stats(int ich) {
    debug("init stats");
    QDuration qdur;
    int err;
    size_t nq = 0;
    size_t nq_ok = 0;
    int64_t totaldur = 0;
    int64_t mindur = INT64_MAX;
    int64_t maxdur = 0;

    while (1) {
        err = chrecv(ich, &qdur, sizeof(qdur), -1);
        if (err || errno == ECANCELED)
            break;
        ++nq;
        if (!qdur.err) {
            ++nq_ok;
            totaldur += qdur.duration;
            if (qdur.duration < mindur) 
                mindur = qdur.duration;
            if (qdur.duration > maxdur) 
                maxdur = qdur.duration;
        }
    }

    printf("The number of queries processed: %zu\n", nq);
    printf("The number of queries which returned some data: %zu\n", nq_ok);
    if (nq_ok > 0) {
        printf("The sum of the single query times: %" PRId64 " (ms)\n", totaldur);
        printf("The minimum query time: %" PRId64 " (ms)\n", mindur);
        printf("The maximum query time: %" PRId64 " (ms)\n", maxdur);
        printf("The average query time: %" PRId64 " (ms)\n", nq_ok > 0 ? totaldur/nq_ok : 0);
    }
}

// Use hash function to always assign a host to the same worker.
// Pro: No need to keep track which host goes to which worker
// Con: Some workers might get zero data it there are only few query params.
size_t 
host_to_worker(Record *rec, size_t nwrk) {
    const Field *field = record_host(rec);
    const char *beg = field_ptr(field);
    size_t len = field_len(field);
    uint64_t seed = 42;
    return XXH64(beg, len, seed) % nwrk;
}

typedef struct {
    size_t nwrk;
    Reader *rdr;
    const char *dbname;
} Args;

static int
parse_opt(
    int key,
    char *arg,
    struct argp_state *state
) {
    Args *args = state->input;
    switch (key) {
        case 'w': {
            char *trail;
            errno = 0;
            ssize_t nwrk = strtoul(arg, &trail, 10);
            if (errno) {
                nwrk = DEFAULT_WORKERS;
                fprintf(stderr, "warning: the number of workers specified is out \
of range. Setting to default %" PRIu64 ".\n", nwrk);
            }
            if (nwrk > MAX_WORKERS) {
                nwrk = MAX_WORKERS;
                fprintf(stderr, "warning: the number of workers specified is greater \
than allowed maximum. Setting to %" PRIu64 ".\n", nwrk);
            }
            if (nwrk <= 0) {
                argp_failure(state, 1, 0, 
                    "the number of workers must be greater than 0: '%s'", arg);
            }
            args->nwrk = nwrk;
            break;
        }
        case ARGP_KEY_ARG:
            switch (state->arg_num) {
                case 0:
                    args->dbname = arg;
                    break;
                case 1:
                    args->rdr = reader_from_path(arg);
                    if (!args->rdr)
                        argp_failure(state, 1, 0,
                            "could not read query parameters from path '%s'", arg);
                    break;
                default:
                    argp_usage(state);
                    break;
            }
            break;
        case ARGP_KEY_END:
            if (state->arg_num == 1) {
                args->rdr = reader_from_stdin();
                if (!args->rdr)
                    argp_failure(state, 1, 0,
                        "could not read query parameters from stdin'");
            }
            else if (state->arg_num == 0 || state->arg_num > 2)
                argp_usage(state);
            break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

static struct argp_option options[] = {
    {"workers", 'w', "NUM", 0, "the number of workers"},    
    {0}
};

int 
main(
    int argc,
    char *argv[]
) {
    Args args = {DEFAULT_WORKERS, NULL, NULL};
    struct argp argp = {options, parse_opt, "DB_FILE [PARAMS_FILE]"};
    int err = argp_parse(&argp, argc, argv, 0, 0, &args);
    if (err) {
        format_err("invalid arguments");
        exit(1);
    }
    debug("num workers: '%" PRIu64 "'", args.nwrk);

    // workers bundle
    int wb = bundle();
    if (wb < 0) {
        format_err("could not create the workers' bundle");
        exit(1);
    }

    int och[2];
    err = chmake(och);
    if (err) {
        format_err("could not create output channel");
        exit(1);
    } 
    int sb = bundle();
    if (sb < 0) {
        format_err("could not create the stats bundle");
        exit(1);
    }
    int orx = och[0];
    int otx = och[1];
    err = bundle_go(sb, stats(orx));
    if (err) {
        format_err("could not create stats coroutine");
        exit(1);
    }

    int ichs[args.nwrk][2];
    // create input channels, db connections and workers
    for (int i = 0; i < args.nwrk; ++i) {
        int err = chmake(ichs[i]);
        if (err) {
            format_err("could not create channel '%d'", i);
            exit(1);
        }

        sqlite3 *db;
        err = sqlite3_open(args.dbname, &db);
        if (err) {
            format_err(
                "could not open db connection: '%s'. %s", args.dbname,
                sqlite3_errmsg(db)
            );
            exit(1);
        }

        int irx = ichs[i][0];
        err = bundle_go(wb, worker(irx, otx, db));
        if (err) {
            format_err("could not add worker '%d' to the bundle ", i);
            exit(1);
        }
    }

    RecordsIter *r_it = reader_iter(args.rdr);
    if (!r_it) {
        format_err("could not create the params iterator");
        exit(1);
    }
    // guaranteed not to be NULL
    Record *rec = record_new();
    size_t pcount = 0;
    int64_t start = now();
    while (!(err = read_next(r_it, rec))) {
        size_t wrk_id = host_to_worker(rec, args.nwrk);
        int err = chsend(ichs[wrk_id][1], &rec, sizeof(rec), -1);
        if (err) {
            format_err("could not send a message to worker '%" PRIu64 "'", wrk_id);
            exit(1);
        }
        pcount++;
    }

    if (err < 0) {
        format_err("could not read the record: %" PRIu64 ". Interrupting...", pcount + 1);
        exit(1);
    }


    // close workers by closing their input channel
    for (int i = 0; i < args.nwrk; ++i) {
        int err = chdone(ichs[i][1]);
        if (err) {
            format_err("could not close the channel to worker '%d'", i);
            exit(1);
        }
    }
        
    debug("wait for workers to finish");
    err = bundle_wait(wb, -1);
    printf("The overall query time: %" PRId64 " (ms)\n", now() - start);
    if (err) {
        format_err("could not wait the workers");
        exit(1);
    }

    // close stats
    err = chdone(och[1]);
    if (err) {
        format_err("could not close the channel to stats");
        exit(1);
    }

    err = bundle_wait(sb, -1);
    if (err) {
        format_err("could not wait the stats");
        exit(1);
    }

    exit(0);
}        


        
    
