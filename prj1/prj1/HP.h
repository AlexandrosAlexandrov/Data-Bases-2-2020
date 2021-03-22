#pragma once

#include "Common.h"

// The handle of a heap file.
typedef int32_t HP_info;

// Creates a heap file with the name fileName. Returns 0 on success and -1 on failure.
int32_t HP_CreateFile(char* fileName, char attributeType, char* attributeName, int32_t attributeLength);

// Opens a heap file and returns a pointer to it's handle. Returns the file handle on success and nullptr on failure.
HP_info* HP_OpenFile(char* fileName);

// Closes a heap file. Returns 0 on success and -1 on failure.
int32_t HP_CloseFile(HP_info* handle);

// Inserts a record to the first available block in the heap file. Creates blocks if required. Returns the block index
// where the record was inserted on success and -1 on failure.
int32_t HP_InsertEntry(HP_info handle, Record record);

// Deletes a record from the heap file if inserted. Returns 0 on success and -1 on failure.
int32_t HP_DeleteEntry(HP_info handle, void* keyValue);

// If keyValue == nullptr, prints all entries in he heap file, otherwise prints the entry with key == keyValue if it exists.
// Returns the number of blocks traversed on success and -1 on failure.
int32_t HP_GetAllEntries(HP_info handle, void* keyValue);

// TODO: Remove this!
int32_t HP_DebugPrint(HP_info handle);
