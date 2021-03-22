#pragma once

#include "Common.h"

// The handle of a hash file.
typedef int32_t HT_info;

// Creates a hash file with bucketCount number of buckets. This returns 0 on success and -1 on failure.
int32_t HT_CreateIndex(char* fileName, char attributeType, char* attributeName, int32_t attributeLength, int32_t bucketCount);

// Opens a hash file and returns a pointer to it's handle. Returns the file handle on success and nullptr on failure.
HT_info* HT_OpenIndex(char* fileName);

// Closes a hash file. Returns 0 on success and -1 on failure.
int32_t HT_CloseIndex(HT_info* handle);

// Inserts a record to the hash file based on the hasing of the ID. Returns the block index where the record was
// inserted on success and -1 on failure.
int32_t HT_InsertEntry(HT_info handle, Record record);

// Deletes a record from the hash file if inserted. Returns 0 on success and -1 on failure.
int32_t HT_DeleteEntry(HT_info handle, void* keyValue);

// If keyValue == nullptr, prints all entries in he hash file, otherwise prints the entry with key == keyValue if it exists.
// Returns the number of blocks traversed on success and -1 on failure.
int32_t HT_GetAllEntries(HT_info handle, void* keyValue);
