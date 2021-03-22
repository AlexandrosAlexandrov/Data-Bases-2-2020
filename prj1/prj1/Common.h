#pragma once

#include <stdint.h>

// A NULL type for pointers.
#define nullptr 0

// The type of files created by the application.
typedef enum FileType
{
	// No file.
	None = 0,

	// A heap file.
	HeapFile,

	// A hash file.
	HashFile
} FileType;

// A common file header for all files created by the application.
typedef struct CommonFileHeader
{
	// The type of the file stored.
	FileType Type;
} CommonFileHeader;

// The structure of the records we insert in the heap and hash files.
typedef struct Record
{
	// The key of the record.
	int32_t ID;

	// The name.
	char Name[15];

	// The surname.
	char Surname[25];

	// The address.
	char Address[50];
} Record;
