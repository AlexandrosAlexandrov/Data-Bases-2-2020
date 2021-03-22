#include "HT.h"

// The handle of a secondary hash file.
typedef int32_t SHT_info;

// Creates a secondary hash file with bucketCount number of buckets, and is assosiated with the hash table
// with file name primaryFileName. This function inserts any elements already in the main hash table into the
// secondary one. This returns 0 on success and -1 on failure.
int32_t SHT_CreateSecondaryIndex(char* fileName, char attributeType, char* attributeName, int32_t attributeLength,
	int32_t bucketCount, char* primaryFileName);

// Opens a secondary hash file and returns a pointer to it's handle. Returns the file handle on success and nullptr on failure.
SHT_info* SHT_OpenSecondaryIndex(char* fileName);

// Closes a secondary hash file. Returns 0 on success and -1 on failure.
int32_t SHT_CloseSecondaryIndex(SHT_info* handle);

// Inserts a record to the hash file based on the hasing of the surname. Returns 0 on success and -1 on failure.
int32_t SHT_SecondaryInsertEntry(SHT_info handle, SecondaryRecord record);

// Prints the entry with key == keyValue if it exists.
// Returns the number of blocks traversed on success and -1 on failure.
int32_t SHT_SecondaryGetAllEntries(SHT_info handle, HT_info primaryHandle, void* keyValue);
