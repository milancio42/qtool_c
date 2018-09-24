#ifndef FAILURE_H
#define FAILURE_H

#include <stdio.h>
#include <string.h>
#include <errno.h>

#define OK 0 // function finished successfuly
#define NONE 1 // function finished successfuly but without output
#define ERR -1 // function failed

#ifndef DEBUG
#define DEBUG 0
#endif

#define format_err(...) \
    do { if (DEBUG) {\
             fprintf(stderr, "%s:%d:%s():", __FILE__, \
                 __LINE__, __func__); \
         }\
         fprintf(stderr, __VA_ARGS__); \
         fprintf(stderr, "\n"); } while (0) 

#define debug(...) \
    do { if (DEBUG) {\
            fprintf(stderr, "%s:%d:%s():", __FILE__, \
            __LINE__, __func__); \
            fprintf(stderr, __VA_ARGS__); \
            fprintf(stderr, "\n"); \
         }\
    } while (0) 

#endif // FAILURE_H
