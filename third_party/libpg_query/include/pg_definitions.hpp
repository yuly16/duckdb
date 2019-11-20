// this is a bit of a mess from c.h, port.h and some others. Upside is it makes the parser compile with minimal
// dependencies.

#pragma once

#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef uintptr_t Datum;
typedef uint64_t Size;

typedef int64_t int64;

typedef uint32_t uint32;
typedef uint32_t Index;
typedef uint32_t Oid;

#define InvalidOid ((Oid)0)

typedef int32_t int32;

typedef int16_t int16;
typedef uint16_t uint16;

typedef uint8_t uint8;

#ifndef true
#define true 1
#define false 0
#endif
#ifndef TRUE
#define TRUE 1
#define FALSE 0
#endif

#define HAVE_LONG_INT_64

#ifndef _MSC_VER
#include <assert.h>
#define Assert(a) assert(a);
#define AssertMacro(p) ((void)assert(p))
#else
#define Assert(a) (a);
#define AssertMacro(p) ((void)(p))
#endif
#define _(a) (a)

#define lengthof(array) (sizeof(array) / sizeof((array)[0]))
#define CppConcat(x, y) x##y

#define HIGHBIT (0x80)
#define IS_HIGHBIT_SET(ch) ((unsigned char)(ch)&HIGHBIT)

#define NAMEDATALEN 64
#define FUNC_MAX_ARGS 100
#define FLEXIBLE_ARRAY_MEMBER

#define DEFAULT_INDEX_TYPE "art"
#define INTERVAL_MASK(b) (1 << (b))

#ifdef _MSC_VER
#define __thread __declspec(thread)
#endif

typedef struct {
	int32_t vl_len_;    /* these fields must match ArrayType! */
	int ndim;         /* always 1 for int2vector */
	int32_t dataoffset; /* always 0 for int2vector */
	Oid elemtype;
	int dim1;
	int lbound1;
	int16_t values[];
} int2vector;

typedef struct NameData {
	char data[NAMEDATALEN];
} NameData;
typedef NameData *Name;

struct varlena {
	char vl_len_[4];                    /* Do not touch this field directly! */
	char vl_dat[1]; /* Data content is here */
};

typedef struct varlena bytea;

typedef int MemoryContext;

typedef enum PostgresParserErrors {
	ERRCODE_SYNTAX_ERROR,
	ERRCODE_FEATURE_NOT_SUPPORTED,
	ERRCODE_INVALID_PARAMETER_VALUE,
	ERRCODE_WINDOWING_ERROR,
	ERRCODE_RESERVED_NAME,
	ERRCODE_INVALID_ESCAPE_SEQUENCE,
	ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER,
	ERRCODE_NAME_TOO_LONG
} PostgresParserErrors;

typedef enum PostgresRelPersistence {
	RELPERSISTENCE_TEMP,
	RELPERSISTENCE_UNLOGGED,
	RELPERSISTENCE_PERMANENT
} PostgresRelPersistence;

typedef enum PostgresErrorLevel {
	UNDEFINED,
	NOTICE,
	WARNING,
	ERROR
} PostgresErrorLevel;

typedef enum PostgresAttributIdentityTypes {
	ATTRIBUTE_IDENTITY_ALWAYS,
	ATTRIBUTE_IDENTITY_BY_DEFAULT
} PostgresAttributIdentityTypes;
