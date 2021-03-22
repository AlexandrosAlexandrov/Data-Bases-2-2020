#pragma once

#include <stdint.h>

// A NULL type for pointers.
#define nullptr 0

// Boolean definition.
typedef uint8_t bool;
#define true  1
#define false 0

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

// The structure that is used for insertion in the secondary hash table.
typedef struct SecondaryRecord
{
	// The actual record.
	Record Record;

	// The block ID that is part of the primary hash file, where the record is stored.
	int32_t BlockID;
} SecondaryRecord;

// Evaluates the hash function used in the hash file, both secondary and primary. Returns 0 on success and -1 on failure.
int32_t HashStatistics(char* fileName);

// ==============
// INTERNAL TYPES
// ==============

// An invalid block index.
#define INVALID_BLOCK_INDEX -1

// The index of the hash file header block and the secondary hash file header block.
#define HEADER_BLOCK_INDEX 0

// The type of files created by the application.
typedef enum FileType
{
	// No file.
	None = 0,

	// A heap file.
	HeapFile,

	// A hash file.
	HashFile,

	// A secondary hash file.
	SecondaryHashFile
} FileType;

// A common file header for all files created by the application.
typedef struct CommonFileHeader
{
	// The type of the file stored.
	FileType Type;
} CommonFileHeader;

// The memory layout of the hash file header block. This structure is stored only on the
// first block of a hash file, both primary and secondary.
typedef struct HashFileHeader
{
	// The common header that every file in this application has.
	CommonFileHeader CommonHeader;

	// The number of buckets in the hash file.
	uint32_t BucketCount;

	// The index of the next block in the hash file.
	int32_t NextBlockIndex;
} HashFileHeader;

// The memory layout of a hash file block that containts the buckets.
// This structure is stored in all hash file blocks that contain buckets, both primary and secondary.
typedef struct HashBucketBlockHeader
{
	// The index of the next bucket block in the hash file.
	int32_t NextBlockIndex;
} HashBucketBlockHeader;

// The memory layout of the hash file data block. This structure is stored on the
// first block of a hash file data block, both primary and secondary.
typedef struct HashDataBlockHeader
{
	// The number of elements in the hash file.
	uint32_t ElementCount;

	// The index of the next data block in the hash file.
	int32_t NextBlockIndex;
} HashDataBlockHeader;

// Calculate at compile time the maximum number of buckets in a block.
#define MAX_BUCKET_COUNT_PER_BLOCK ((BLOCK_SIZE - sizeof(HashBucketBlockHeader)) / sizeof(int32_t))
