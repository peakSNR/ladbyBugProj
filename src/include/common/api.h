#pragma once

// Helpers
#if defined _WIN32 || defined __CYGWIN__
#define MONAD_HELPER_DLL_IMPORT __declspec(dllimport)
#define MONAD_HELPER_DLL_EXPORT __declspec(dllexport)
#define MONAD_HELPER_DLL_LOCAL
#define MONAD_HELPER_DEPRECATED __declspec(deprecated)
#else
#define MONAD_HELPER_DLL_IMPORT __attribute__((visibility("default")))
#define MONAD_HELPER_DLL_EXPORT __attribute__((visibility("default")))
#define MONAD_HELPER_DLL_LOCAL __attribute__((visibility("hidden")))
#define MONAD_HELPER_DEPRECATED __attribute__((__deprecated__))
#endif

#ifdef MONAD_STATIC_DEFINE
#define MONAD_API
#else
#ifndef MONAD_API
#ifdef MONAD_EXPORTS
/* We are building this library */
#define MONAD_API MONAD_HELPER_DLL_EXPORT
#else
/* We are using this library */
#define MONAD_API MONAD_HELPER_DLL_IMPORT
#endif
#endif
#endif

#ifndef MONAD_DEPRECATED
#define MONAD_DEPRECATED MONAD_HELPER_DEPRECATED
#endif

#ifndef MONAD_DEPRECATED_EXPORT
#define MONAD_DEPRECATED_EXPORT MONAD_API MONAD_DEPRECATED
#endif
