
#ifndef KUDU_EXPORT_H
#define KUDU_EXPORT_H

#include <sstream>

#ifdef KUDU_STATIC_DEFINE
#  define KUDU_EXPORT
#  define KUDU_NO_EXPORT
#else
#  ifndef KUDU_EXPORT
#    ifdef kudu_client_exported_EXPORTS
        /* We are building this library */
#      define KUDU_EXPORT __attribute__((visibility("default")))
#    else
        /* We are using this library */
#      define KUDU_EXPORT __attribute__((visibility("default")))
#    endif
#  endif

#  ifndef KUDU_NO_EXPORT
#    define KUDU_NO_EXPORT __attribute__((visibility("hidden")))
#  endif
#endif

#ifndef KUDU_DEPRECATED
#  define KUDU_DEPRECATED __attribute__ ((__deprecated__))
#endif

#ifndef KUDU_DEPRECATED_EXPORT
#  define KUDU_DEPRECATED_EXPORT KUDU_EXPORT KUDU_DEPRECATED
#endif

#ifndef KUDU_DEPRECATED_NO_EXPORT
#  define KUDU_DEPRECATED_NO_EXPORT KUDU_NO_EXPORT KUDU_DEPRECATED
#endif

#if 0 /* DEFINE_NO_DEPRECATED */
#  ifndef KUDU_NO_DEPRECATED
#    define KUDU_NO_DEPRECATED
#  endif
#endif

#endif
