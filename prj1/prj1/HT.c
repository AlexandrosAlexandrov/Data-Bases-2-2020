#include "HT.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "BF/BF.h"

// The index of the hash file header block.
#define HEADER_BLOCK_INDEX 0

// An invalid block index.
#define INVALID_BLOCK_INDEX -1

// The memory layout of the hash file header block. This structure is stored only on the first block of a hash file.
typedef struct FileHeader
{
	// The common header that every file in this application has.
	CommonFileHeader CommonHeader;

	// The number of buckets in the current hash block.
	uint32_t BucketCount;

	// The index of the next block in the hash file.
	int32_t NextBlockIndex;
} FileHeader;

// The memory layout of a hash table file that containts the buckets. This structure is stored in all hash file
// blocks that contain buckets.
typedef struct BucketBlockHeader
{
	// The index of the next block in the hash file.
	int32_t NextBlockIndex;
} BucketBlockHeader;

// The memory layout of a hash data block. This structure is stored in all hash data.
typedef struct DataBlockHeader
{
	// The number of records in the current data block.
	uint8_t RecordCount;

	// The index of the next data block in the hash file.
	int32_t NextBlockIndex;
} DataBlockHeader;

// Calculate at compile time the maximum number of buckets in a block.
#define MAX_BUCKET_COUNT_PER_BLOCK ((BLOCK_SIZE - sizeof(BucketBlockHeader)) / sizeof(int32_t))

// Calculate at compile time the maximum number of records in a hash data block.
#define MAX_RECORD_COUNT_PER_BLOCK ((BLOCK_SIZE - sizeof(DataBlockHeader)) / sizeof(Record))

// Storage for the currently open hash file handle.
static HT_info s_HandleStorage = -1;

// Knuth Variant on Cormen Division
// Reference: https://www.cs.hmc.edu/~geoff/classes/hmc.cs070.200101/homework10/hashfuncs.html
static int32_t HashFunction(int32_t key, int32_t hashTableSize)
{
	return (key * (key + 3)) % hashTableSize;
}

int32_t HT_CreateIndex(char* fileName, char attributeType, char* attributeName, int32_t attributeLength, int32_t bucketCount)
{
	// Initialize the block level.
	BF_Init();

	// Create the block level file.
	if (BF_CreateFile(fileName) < 0)
	{
		printf("Could not create block level file for the hash file! FileName: %s\n", fileName);
		BF_PrintError("");

		return -1;
	}

	// Open the block level file.
	HT_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
	{
		printf("Could not open block level file for the hash file! FileName: %s\n", fileName);
		BF_PrintError("");

		return -1;
	}

	// Allocate the hash file header block.
	if (BF_AllocateBlock(fileHandle) < 0)
	{
		printf("Could not allocate header block for the hash file! FileHandle: %d\n", fileHandle);
		BF_PrintError("");

		return -1;
	}

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Create the hash file header and fill it's data.
	FileHeader header = { };
	header.CommonHeader.Type = HashFile;
	header.BucketCount = bucketCount;
	header.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the file header into the hash file header block.
	memcpy(headerBlockPtr, &header, sizeof(FileHeader));

	// Write the hash file header block to the disk.
	if (BF_WriteBlock(fileHandle, HEADER_BLOCK_INDEX) < 0)
	{
		printf("Could not write hash header block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Now we need to create all the blocks required to store the hash table.

	// Calculate the required blocks for the bucket count using an integer division.
	int32_t requiredBlockCount = (bucketCount / MAX_BUCKET_COUNT_PER_BLOCK);

	// If there is a remainder we add one more block to account for those buckets.
	requiredBlockCount += ((bucketCount % MAX_BUCKET_COUNT_PER_BLOCK) > 0) ? 1 : 0;

	// Set the previous bucket block index to the header block.
	int32_t previousBucketBlockIndex = HEADER_BLOCK_INDEX;

	// Loop through all the required hash file bucket blocks.
	for (int32_t index = 0; index < requiredBlockCount; index++)
	{
		// Allocate a new bucket block.
		if (BF_AllocateBlock(fileHandle) < 0)
		{
			printf("Could not allocate bucket block for the hash file! FileHandle: %d\n", fileHandle);
			BF_PrintError("");

			return -1;
		}

		// Retrieve the block count of the hash file.
		int32_t blockCount = BF_GetBlockCounter(fileHandle);
		if (blockCount < 0)
		{
			printf("Could not retrieve block count for the hash file! FileHandle: %d\n", fileHandle);
			BF_PrintError("");

			return -1;
		}

		// Calculate the index of the new bucket block.
		int32_t newBucketBlockIndex = blockCount - 1;

		// Retrieve a pointer to the new bucket block.
		uint8_t* newBucketBlockPtr = nullptr;
		if (BF_ReadBlock(fileHandle, newBucketBlockIndex, (void**)&newBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", fileHandle, newBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Create the bucket block header and set it's data.
		BucketBlockHeader newBucketBlockHeader = { };
		newBucketBlockHeader.NextBlockIndex = INVALID_BLOCK_INDEX;

		// Copy the new bucket block header to he beginning of the new bucket block.
		memcpy(newBucketBlockPtr, &newBucketBlockHeader, sizeof(BucketBlockHeader));

		// Offset the new bucket block pointer to point to the first byte of the bucket indices.
		newBucketBlockPtr += sizeof(BucketBlockHeader);

		// Initially set the number of buckets in the current bucket block to max.
		int32_t bucketCountInCurrentBucketBlock = MAX_BUCKET_COUNT_PER_BLOCK;

		// If this is the last bucket block we create, the number of buckets is the remainder of the following division.
		if (index == requiredBlockCount - 1)
			bucketCountInCurrentBucketBlock = bucketCount % MAX_BUCKET_COUNT_PER_BLOCK;

		// Fill the bucketCountInCurrentBucketBlock number of bucket indices to the invalid block index.
		memset(newBucketBlockPtr, INVALID_BLOCK_INDEX, bucketCountInCurrentBucketBlock * sizeof(int32_t));

		// Write the new bucket block to the disk.
		if (BF_WriteBlock(fileHandle, newBucketBlockIndex) < 0)
		{
			printf("Could not write hash bucket block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, newBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Now we need to update the previous bucket block's NextBlockIndex.

		// Retrieve a pointer to the previous bucket block.
		uint8_t* previousBucketBlockPtr = nullptr;
		if (BF_ReadBlock(fileHandle, previousBucketBlockIndex, (void**)&previousBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", fileHandle, previousBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// If the previous bucket block is the header block, we need to treat it differently since it has a different header structure.
		if (previousBucketBlockIndex == HEADER_BLOCK_INDEX)
		{
			FileHeader* previousBlockHeader = (FileHeader*)previousBucketBlockPtr;
			previousBlockHeader->NextBlockIndex = newBucketBlockIndex;
		}
		else
		{
			BucketBlockHeader* previousBlockHeader = (BucketBlockHeader*)previousBucketBlockPtr;
			previousBlockHeader->NextBlockIndex = newBucketBlockIndex;
		}

		// Write the updated previous bucket block to the disk.
		if (BF_WriteBlock(fileHandle, previousBucketBlockIndex) < 0)
		{
			printf("Could not write hash bucket block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, previousBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Update the previous bucket block index to the new bucket block index.
		previousBucketBlockIndex = newBucketBlockIndex;
	}

	// Close the block level file.
	if (BF_CloseFile(fileHandle) < 0)
	{
		printf("Could not close block level file! FileHandle: %d\n", fileHandle);
		BF_PrintError("");

		return -1;
	}

	return 0;
}

HT_info* HT_OpenIndex(char* fileName)
{
	// Ensure that there are no files currently open.
	if (s_HandleStorage != -1)
	{
		printf("Cannot open hash file since there's another file open!\n");
		return nullptr;
	}

	// Open the block level file.
	HT_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
	{
		printf("Could not open block level file for the hash file! FileName: %s\n", fileName);
		BF_PrintError("");

		return nullptr;
	}

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a CommonFileHeader in the first bytes of the header block. So we treat the pointer as such.
	CommonFileHeader* commonFileHeader = (CommonFileHeader*)headerBlockPtr;

	// Ensure that the file we open is a indeed hash file.
	if (commonFileHeader->Type != HashFile)
	{
		printf("File specified is not a hash file! FileName: %s\n", fileName);
		return nullptr;
	}

	// Store the handle in a global variable so that we can return a pointer to it.
	s_HandleStorage = fileHandle;

	return &s_HandleStorage;
}

int32_t HT_CloseIndex(HT_info* handle)
{
	// Ensure that the file we want to close is actually open.
	if (s_HandleStorage != *handle)
	{
		printf("Cannot close hash file since it's not open!\n");
		return -1;
	}

	// Close the block level file.
	if (BF_CloseFile(*handle) < 0)
	{
		printf("Could not close block level file! FileHandle: %d\n", *handle);
		BF_PrintError("");

		return -1;
	}

	// Reset the internal storage.
	s_HandleStorage = -1;

	return 0;
}

int32_t HT_InsertEntry(HT_info handle, Record record)
{
	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	// Hash the record ID and find the bucket index.
	int32_t bucketIndex = HashFunction(record.ID, fileHeader->BucketCount);

	// First we need to find the bucket block where the bucket index is in.

	// Calculate in which block the above bucket index is in. This is similar to calculating the total bucket block size.
	int32_t bucketBlockNumber = ((bucketIndex + 1) / MAX_BUCKET_COUNT_PER_BLOCK);
	bucketBlockNumber += (((bucketIndex + 1) % MAX_BUCKET_COUNT_PER_BLOCK) > 0) ? 1 : 0;

	// Start from the first actuall bucket block.
	int32_t currentBucketBlockIndex = fileHeader->NextBlockIndex;

	// Start with an invalid index since there's no previous bucket block.
	int32_t previousBucketBlockIndex = INVALID_BLOCK_INDEX;

	// Loop through all the blocks to find the index of the block the bucket index is in.
	for (int32_t index = 0; index < bucketBlockNumber; index++)
	{
		// Retrieve a pointer to the new bucket block.
		uint8_t* currentBucketBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
		BucketBlockHeader* currentBucketBlockHeader = (BucketBlockHeader*)currentBucketBlockPtr;

		// Update the previous and current bucket block index to the next one until we exit out of the loop.
		previousBucketBlockIndex = currentBucketBlockIndex;
		currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;
	}

	// This is the bucket block index where the bucket index is.
	int32_t bucketBlockIndex = previousBucketBlockIndex;

	// Find the bucket index relative to the bucket block.
	int32_t bucketIndexInBucketBlock = bucketIndex % MAX_BUCKET_COUNT_PER_BLOCK;

	// Retrieve a pointer to the bucket block.
	uint8_t* bucketBlockPtr = nullptr;
	if (BF_ReadBlock(handle, bucketBlockIndex, (void**)&bucketBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Offset the bucket block pointer by the size of the header so it points at the beginning of it's section of the hash table.
	bucketBlockPtr += sizeof(BucketBlockHeader);

	// Offset the bucket block pointer by the index of the bucket index times the size of a bucket index, so in points at the beginning
	// of the bucket index we want.
	bucketBlockPtr += bucketIndexInBucketBlock * sizeof(int32_t);

	// Extract the index of the data block from the bucket.
	int32_t dataBlockIndex = *(int32_t*)bucketBlockPtr;

	// Now we need to look for the record and make sure it's not already in the hash file.

	// Start from the first data block.
	int32_t currentDataBlockIndex = dataBlockIndex;

	// Loop until the end of the allocated data blocks.
	while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current data block.
		uint8_t* currentDataBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a DataBlockHeader in the first bytes of the block. So we treat the pointer as such.
		DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentDataBlockPtr += sizeof(DataBlockHeader);

		// Interate through all the record slots that are occupied in the current data block.
		for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->RecordCount; recordIndex++)
		{
			// Treat the current pointer as a record.
			Record* currentRecord = (Record*)currentDataBlockPtr;

			// If the current record's key is the same as the one we want to insert, it's already in the hash so we exit.
			if (currentRecord->ID == record.ID)
			{
				printf("The specified record is already in the hash file! RecordID: %d\n", record.ID);
				return -1;
			}

			// Offset the block poiter by the size of a record so it pointes to the first byte of the next record slot.
			currentDataBlockPtr += sizeof(Record);
		}

		// Update the current block index.
		currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
	}

	// If we're here the record is not in the hash so we try to insert it.

	// Reset the current data block index and prepare for insertion.
	currentDataBlockIndex = dataBlockIndex;

	// The previous block is invalid initially.
	int32_t previousDataBlockIndex = INVALID_BLOCK_INDEX;

	// Loop until the end of the allocated data blocks.
	while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current data block.
		uint8_t* currentDataBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a DataBlockHeader in the first bytes of the block. So we treat the pointer as such.
		DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;

		// If there's space in the current data block, we insert here.
		if (currentDataBlockHeader->RecordCount < MAX_RECORD_COUNT_PER_BLOCK)
		{
			// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
			currentDataBlockPtr += sizeof(DataBlockHeader);

			// Offset the block pointer by the size of the record times the number of records so it points to the first byte
			// of the first empty record slot.
			currentDataBlockPtr += currentDataBlockHeader->RecordCount * sizeof(Record);

			// Copy the record into the data block.
			memcpy(currentDataBlockPtr, &record, sizeof(Record));

			// Increment the current data block's record count.
			currentDataBlockHeader->RecordCount++;

			// Write the updated contents of the current hash file data block to the disk.
			if (BF_WriteBlock(handle, currentDataBlockIndex) < 0)
			{
				printf("Could not write hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Return the current data block's index.
			return currentDataBlockIndex;
		}

		// Update the previous and current block indices.
		previousDataBlockIndex = currentDataBlockIndex;
		currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
	}

	// If we are here, a new data block needs to be created. Either because this is the first entry in the bucket or because we ran
	// out of space in all of the currently allocated data blocks.

	// Allocate a new data block.
	if (BF_AllocateBlock(handle) < 0)
	{
		printf("Could not allocate data block for the hash file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Get the new block count.
	int32_t blockCount = BF_GetBlockCounter(handle);
	if (blockCount < 0)
	{
		printf("Could not retrieve block count for the hash file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Calculate the new data block index.
	int32_t newDataBlockIndex = blockCount - 1;

	// Retrieve a pointer to the new data block.
	uint8_t* newDataBlockPtr = nullptr;
	if (BF_ReadBlock(handle, newDataBlockIndex, (void**)&newDataBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, newDataBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Create the data block header and fill it's data.
	DataBlockHeader newDataBlockHeader = { };
	newDataBlockHeader.RecordCount = 1;
	newDataBlockHeader.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the new data block header into the new data block.
	memcpy(newDataBlockPtr, &newDataBlockHeader, sizeof(DataBlockHeader));

	// Offset the data block pointer by the size of the header so it points to the first byte of the first record slot.
	newDataBlockPtr += sizeof(DataBlockHeader);

	// Copy the record into the block.
	memcpy(newDataBlockPtr, &record, sizeof(Record));

	// Write the contents of the new hash data block to the disk.
	if (BF_WriteBlock(handle, newDataBlockIndex) < 0)
	{
		printf("Could not write hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, newDataBlockIndex);
		BF_PrintError("");

		return -1;
	}

	if (previousDataBlockIndex != INVALID_BLOCK_INDEX)
	{
		// If a previous data block exists, we need to update the NextBlockIndex.

		// Retrieve a pointer to the previous data block.
		uint8_t* previousDataBlockPtr = nullptr;
		if (BF_ReadBlock(handle, previousDataBlockIndex, (void**)&previousDataBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, previousDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Get the header and update the next block index.
		DataBlockHeader* previousDataBlockHeader = (DataBlockHeader*)previousDataBlockPtr;
		previousDataBlockHeader->NextBlockIndex = newDataBlockIndex;

		// Write the contents of the previous hash data block to the disk.
		if (BF_WriteBlock(handle, previousDataBlockIndex) < 0)
		{
			printf("Could not write hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, previousDataBlockIndex);
			BF_PrintError("");

			return -1;
		}
	}
	else
	{
		// Otherwise we need to update the bucket block.

		// Retrieve a pointer to the bucket block.
		uint8_t* bucketBlockPtr = nullptr;
		if (BF_ReadBlock(handle, bucketBlockIndex, (void**)&bucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Offset the bucket block pointer by the size of the header so it points at the beginning of it's section of the hash table.
		bucketBlockPtr += sizeof(BucketBlockHeader);

		// Offset the bucket block pointer by the index of the bucket index times the size of a bucket index, so in points at the beginning
		// of the bucket index we want.
		bucketBlockPtr += bucketIndexInBucketBlock * sizeof(int32_t);

		// Extract the index of the data block from the bucket.
		int32_t* dataBlockIndex = (int32_t*)bucketBlockPtr;
		*dataBlockIndex = newDataBlockIndex;

		// Write the contents of the bucket block to the disk.
		if (BF_WriteBlock(handle, bucketBlockIndex) < 0)
		{
			printf("Could not write hash bucket block to disk! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
			BF_PrintError("");

			return -1;
		}
	}

	// Return the index of the new block.
	return newDataBlockIndex;
}

int32_t HT_DeleteEntry(HT_info handle, void* keyValue)
{
	// The key is an integer so cast the void pointer.
	int32_t key = *(int32_t*)keyValue;

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	// Hash the record ID and find the bucket index.
	int32_t bucketIndex = HashFunction(key, fileHeader->BucketCount);

	// First we need to find the bucket block where the bucket index is in.

	// Calculate in which block the above bucket index is in. This is similar to calculating the total bucket block size.
	int32_t bucketBlockNumber = ((bucketIndex + 1) / MAX_BUCKET_COUNT_PER_BLOCK);
	bucketBlockNumber += (((bucketIndex + 1) % MAX_BUCKET_COUNT_PER_BLOCK) > 0) ? 1 : 0;

	// Start from the first actuall bucket block.
	int32_t currentBucketBlockIndex = fileHeader->NextBlockIndex;

	// Start with an invalid index since there's no previous bucket block.
	int32_t previousBucketBlockIndex = INVALID_BLOCK_INDEX;

	// Loop through all the blocks to find the index of the block the bucket index is in.
	for (int32_t index = 0; index < bucketBlockNumber; index++)
	{
		// Retrieve a pointer to the new bucket block.
		uint8_t* currentBucketBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
		BucketBlockHeader* currentBucketBlockHeader = (BucketBlockHeader*)currentBucketBlockPtr;

		// Update the previous and current bucket block index to the next one until we exit out of the loop.
		previousBucketBlockIndex = currentBucketBlockIndex;
		currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;
	}

	// This is the bucket block index where the bucket index is.
	int32_t bucketBlockIndex = previousBucketBlockIndex;

	// Find the bucket index relative to the bucket block.
	int32_t bucketIndexInBucketBlock = bucketIndex % MAX_BUCKET_COUNT_PER_BLOCK;

	// Retrieve a pointer to the bucket block.
	uint8_t* bucketBlockPtr = nullptr;
	if (BF_ReadBlock(handle, bucketBlockIndex, (void**)&bucketBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Offset the bucket block pointer by the size of the header so it points at the beginning of it's section of the hash table.
	bucketBlockPtr += sizeof(BucketBlockHeader);

	// Offset the bucket block pointer by the index of the bucket index times the size of a bucket index, so in points at the beginning
	// of the bucket index we want.
	bucketBlockPtr += bucketIndexInBucketBlock * sizeof(int32_t);

	// Extract the index of the data block from the bucket.
	int32_t dataBlockIndex = *(int32_t*)bucketBlockPtr;

	// Start from the first actual block of data.
	int32_t currentDataBlockIndex = dataBlockIndex;

	// Loop until the end of the allocated blocks.
	while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current hash data block.
		uint8_t* currentDataBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a DataBlockHeader in the first bytes of the block. So we treat the pointer as such.
		DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentDataBlockPtr += sizeof(DataBlockHeader);

		// Interate through all the record slots that are occupied in the current block.
		for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->RecordCount; recordIndex++)
		{
			// Treat the current pointer as a record.
			Record* currentRecord = (Record*)currentDataBlockPtr;

			// If the current records ID is the same as the key, we want to delete it and exit.
			if (currentRecord->ID == key)
			{
				// What we want to do is move the contents of the records after the current record up the size of one record.

				// Calculate the size of the records after the current record.
				uint32_t byteCountOfRecordDataAfterCurrentRecord = (currentDataBlockHeader->RecordCount - (recordIndex + 1)) * sizeof(Record);

				// Copy the records after the current record, to the current record's position in the block.
				memcpy(currentDataBlockPtr, currentDataBlockPtr + sizeof(Record), byteCountOfRecordDataAfterCurrentRecord);

				// Decrement the current block record count;
				currentDataBlockHeader->RecordCount--;

				// Offset the block pointer by the size of a block times the number of records left in the block after the current one.
				// This way the pointer points to the first byte of the empty space in the block.
				currentDataBlockPtr += byteCountOfRecordDataAfterCurrentRecord;

				// Calculate the number of empty bytes in the block.
				uint32_t emptyByteCount = BLOCK_SIZE - (sizeof(DataBlockHeader) + currentDataBlockHeader->RecordCount * sizeof(Record));

				// Set the empty bytes to zero.
				memset(currentDataBlockPtr, 0, emptyByteCount);

				// Write the updated contents of the current hash data block to the disk.
				if (BF_WriteBlock(handle, currentDataBlockIndex) < 0)
				{
					printf("Could not write hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
					BF_PrintError("");

					return -1;
				}

				// Exit the function since we deleted.
				return 0;
			}

			// Offset the block poiter by the size of a record so it pointer to the first byte of the next record slot.
			currentDataBlockPtr += sizeof(Record);
		}

		// Update the current block index.
		currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
	}

	// If we're here, the record with key keyValue was not found.
	printf("Could not find record with key %d!\n", key);
	return -1;
}

int32_t HT_GetAllEntries(HT_info handle, void* keyValue)
{
	// Key value can be nullptr. If it's not get the actual value otherwise use a dummy.
	int32_t key = -1;
	if (keyValue != nullptr)
		key = *(int32_t*)keyValue;

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	if (key != -1)
	{
		// If key is valid, search for the entry.

		// Hash the record ID and find the bucket index.
		int32_t bucketIndex = HashFunction(key, fileHeader->BucketCount);

		// First we need to find the bucket block where the bucket index is in.

		// The number of blocks that we traversed. Set to one to account for the hash file header block.
		uint32_t blocksTraversed = 1;

		// Calculate in which block the above bucket index is in. This is similar to calculating the total bucket block size.
		int32_t bucketBlockNumber = ((bucketIndex + 1) / MAX_BUCKET_COUNT_PER_BLOCK);
		bucketBlockNumber += (((bucketIndex + 1) % MAX_BUCKET_COUNT_PER_BLOCK) > 0) ? 1 : 0;

		// Start from the first actuall bucket block.
		int32_t currentBucketBlockIndex = fileHeader->NextBlockIndex;

		// Start with an invalid index since there's no previous bucket block.
		int32_t previousBucketBlockIndex = INVALID_BLOCK_INDEX;

		// Loop through all the blocks to find the index of the block the bucket index is in.
		for (int32_t index = 0; index < bucketBlockNumber; index++)
		{
			// Retrieve a pointer to the new bucket block.
			uint8_t* currentBucketBlockPtr = nullptr;
			if (BF_ReadBlock(handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
			{
				printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Increment the blocks traversed counter.
			blocksTraversed++;

			// Since this file exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
			BucketBlockHeader* currentBucketBlockHeader = (BucketBlockHeader*)currentBucketBlockPtr;

			// Update the previous and current bucket block index to the next one until we exit out of the loop.
			previousBucketBlockIndex = currentBucketBlockIndex;
			currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;
		}

		// This is the bucket block index where the bucket index is.
		int32_t bucketBlockIndex = previousBucketBlockIndex;

		// Find the bucket index relative to the bucket block.
		int32_t bucketIndexInBucketBlock = bucketIndex % MAX_BUCKET_COUNT_PER_BLOCK;

		// Retrieve a pointer to the bucket block.
		uint8_t* bucketBlockPtr = nullptr;
		if (BF_ReadBlock(handle, bucketBlockIndex, (void**)&bucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Offset the bucket block pointer by the size of the header so it points at the beginning of it's section of the hash table.
		bucketBlockPtr += sizeof(BucketBlockHeader);

		// Offset the bucket block pointer by the index of the bucket index times the size of a bucket index, so in points at the beginning
		// of the bucket index we want.
		bucketBlockPtr += bucketIndexInBucketBlock * sizeof(int32_t);

		// Extract the index of the data block from the bucket.
		int32_t dataBlockIndex = *(int32_t*)bucketBlockPtr;

		// Start from the first actual block of data.
		int32_t currentDataBlockIndex = dataBlockIndex;

		// Loop until the end of the allocated blocks.
		while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
		{
			// Retrieve a pointer to the current hash data block.
			uint8_t* currentDataBlockPtr = nullptr;
			if (BF_ReadBlock(handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
			{
				printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Increment the blocks traversed counter.
			blocksTraversed++;

			// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
			DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;

			// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
			currentDataBlockPtr += sizeof(DataBlockHeader);

			// Interate through all the record slots that are occupied in the current block.
			for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->RecordCount; recordIndex++)
			{
				// Treat the current pointer as a record.
				Record* currentRecord = (Record*)currentDataBlockPtr;

				// If the current records ID is the same as the key, we want to print it and exit.
				if (currentRecord->ID == key)
				{
					printf("ID: %d, Name: %s, Surname: %s, Address: %s\n", currentRecord->ID, currentRecord->Name, currentRecord->Surname, currentRecord->Address);
					return blocksTraversed;
				}

				// Offset the block poiter by the size of a record so it pointer to the first byte of the next record slot.
				currentDataBlockPtr += sizeof(Record);
			}

			// Update the current block index.
			currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
		}

		// If we are here, it means that the record with the specified key was not found in the hash file.
		printf("Could not find record with key %d!\n", key);
		return -1;
	}
	else
	{
		// Start from the first bucket block.
		int32_t currentBucketBlockIndex = fileHeader->NextBlockIndex;

		// Loop through all the bucket blocks.
		while (currentBucketBlockIndex != INVALID_BLOCK_INDEX)
		{
			// Retrieve a pointer to the current bucket block.
			uint8_t* currentBucketBlockPtr = nullptr;
			if (BF_ReadBlock(handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
			{
				printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Since this block exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
			BucketBlockHeader* currentBucketBlockHeader = (BucketBlockHeader*)currentBucketBlockPtr;

			// Offset the pointer by the size of the header so it points to the first bucket.
			currentBucketBlockPtr += sizeof(BucketBlockHeader);

			// Calculate the number of buckets in the current bucket block. If it's not the last one there's the
			// max number of buckets. Otherwise it's the remainder.
			int32_t bucketsInCurrentBlock = MAX_BUCKET_COUNT_PER_BLOCK;
			if (currentBucketBlockHeader->NextBlockIndex == INVALID_BLOCK_INDEX)
				bucketsInCurrentBlock = fileHeader->BucketCount % MAX_BUCKET_COUNT_PER_BLOCK;

			// Loop though all the buckets in the block.
			for (uint32_t bucketIndex = 0; bucketIndex < bucketsInCurrentBlock; bucketIndex++)
			{
				// Get the value of the bucket.
				int32_t bucketValue = *(int32_t*)currentBucketBlockPtr;

				// Start from the first data block.
				int32_t currentDataBlockIndex = bucketValue;

				// Loop through all the data blocks in the bucket.
				while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
				{
					// Retrieve a pointer to the data block.
					uint8_t* currentDataBlockPtr = nullptr;
					if (BF_ReadBlock(handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
					{
						printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
						BF_PrintError("");

						return -1;
					}

					// Since this block exists we know there is a DataBlockHeader is the first byte so treat is as such.
					DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;

					// Offset the pointer by the size of the header so it points to the beginning of the record data.
					currentDataBlockPtr += sizeof(DataBlockHeader);

					// Loop through all the records in the block.
					for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->RecordCount; recordIndex++)
					{
						// Get the current record and print it.
						Record* currentRecord = (Record*)currentDataBlockPtr;
						printf("ID: %d, Name: %s, Surname: %s, Address: %s\n", currentRecord->ID, currentRecord->Name, currentRecord->Surname, currentRecord->Address);

						// Increment the pointer by the size of a record so it points to the next record in the block.
						currentDataBlockPtr += sizeof(Record);
					}

					// Update the current data block to point to the next one.
					currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
				}

				// Increment the pointer by the size of an integer so it points to the next bucket.
				currentBucketBlockPtr += sizeof(int32_t);
			}

			// Update the current bucket block to point to the next one.
			currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;
		}

		return 0;
	}
}

int32_t HashStatistics(char* fileName)
{
	// Open the hash file.
	HT_info* handle = HT_OpenIndex(fileName);
	if (handle == nullptr)
	{
		printf("Could not open hash file!\n");
		return -1;
	}

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(*handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", *handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;
	uint32_t bucketCount = fileHeader->BucketCount;

	// Initialize the statistics.
	uint32_t minRecordCount = UINT32_MAX;
	uint32_t maxRecordCount = 0;
	uint32_t totalRecordCount = 0;
	uint32_t* overflowBlocksPerBucket = (uint32_t*)malloc(bucketCount * sizeof(uint32_t));
	memset(overflowBlocksPerBucket, 0, bucketCount * sizeof(uint32_t));

	// Start from the first bucket block.
	int32_t currentBucketBlockIndex = fileHeader->NextBlockIndex;

	// The index of the bucket NOT relative to the current bucket block.c
	uint32_t globalBucketIndex = 0;

	// Loop through all the bucket blocks.
	while (currentBucketBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current bucket block.
		uint8_t* currentBucketBlockPtr = nullptr;
		if (BF_ReadBlock(*handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", *handle, currentBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this block exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
		BucketBlockHeader* currentBucketBlockHeader = (BucketBlockHeader*)currentBucketBlockPtr;

		// Offset the pointer by the size of the header so it points to the first bucket.
		currentBucketBlockPtr += sizeof(BucketBlockHeader);

		// Calculate the number of buckets in the current bucket block. If it's not the last one there's the
		// max number of buckets. Otherwise it's the remainder.
		int32_t bucketsInCurrentBlock = MAX_BUCKET_COUNT_PER_BLOCK;
		if (currentBucketBlockHeader->NextBlockIndex == INVALID_BLOCK_INDEX)
			bucketsInCurrentBlock = fileHeader->BucketCount % MAX_BUCKET_COUNT_PER_BLOCK;

		// Loop though all the buckets in the block.
		for (uint32_t bucketIndex = 0; bucketIndex < bucketsInCurrentBlock; bucketIndex++)
		{
			// Get the value of the bucket.
			int32_t bucketValue = *(int32_t*)currentBucketBlockPtr;

			// Start from the first data block.
			int32_t currentDataBlockIndex = bucketValue;

			// The number of records in the bucket.
			uint32_t recordCount = 0;

			// The number of blocks in the bucket.
			uint32_t blockCount = 0;

			// Loop through all the data blocks in the bucket.
			while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
			{
				// Retrieve a pointer to the data block.
				uint8_t* currentDataBlockPtr = nullptr;
				if (BF_ReadBlock(*handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
				{
					printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", *handle, currentDataBlockIndex);
					BF_PrintError("");

					return -1;
				}

				// Increment the number of blocks in the bucket.
				blockCount++;

				// Since this block exists we know there is a DataBlockHeader is the first byte so treat is as such.
				DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;

				// Increment the record count by the number of records in the block.
				recordCount += currentDataBlockHeader->RecordCount;

				// Update the current data block to point to the next one.
				currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
			}

			// If the current bucket is not invalid, we need to update the results.
			if (bucketValue != INVALID_BLOCK_INDEX)
			{
				// Update the min record count.
				if (recordCount < minRecordCount)
					minRecordCount = recordCount;

				// Update the max record count.
				if (recordCount > maxRecordCount)
					maxRecordCount = recordCount;

				// Update the total record count.
				totalRecordCount += recordCount;

				// Update the overflow block counts. Subtract one to account for the first block.
				overflowBlocksPerBucket[globalBucketIndex] = blockCount - 1;
			}

			// Increment the global bucket index.
			globalBucketIndex++;

			// Increment the pointer by the size of an integer so it points to the next bucket.
			currentBucketBlockPtr += sizeof(int32_t);
		}

		// Update the current bucket block to point to the next one.
		currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;
	}

	// Retrieve the block count.
	int32_t blockCount = BF_GetBlockCounter(*handle);
	if (blockCount < 0)
	{
		printf("Could not retrieve block count for the hash file! FileHandle: %d\n", *handle);
		BF_PrintError("");

		return -1;
	}

	// Calculate the average record count.
	float averageRecordCount = (float)totalRecordCount / (float)bucketCount;

	// Print the statistics.
	printf("Block Count in the hash file: %d\n", blockCount);
	printf("Max Record Count in a bucket: %d\n", maxRecordCount);
	printf("Min Record Count in a bucket: %d\n", minRecordCount);
	printf("Average Record Count per bucket: %f\n", averageRecordCount);

	// Print the overflow blocks for each bucket and compute the total.
	uint32_t totalOverflowBlockCount = 0;
	for (uint32_t index = 0; index < bucketCount; index++)
	{
		printf("Overflow Block Count for bucket %d: %d\n", index, overflowBlocksPerBucket[index]);
		totalOverflowBlockCount += overflowBlocksPerBucket[index];
	}

	printf("Total Overflow Block Count: %d\n", totalOverflowBlockCount);

	// Free the array.
	free(overflowBlocksPerBucket);

	// Close the hash file.
	if (HT_CloseIndex(handle) == -1)
	{
		printf("Could not close hash file!\n");
		return -1;
	}

	return 0;
}

// TODO: Remove this!
int32_t HT_DebugPrint(HT_info handle)
{
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	printf("Block %d:\n", HEADER_BLOCK_INDEX);
	printf("\tType: %s\n", fileHeader->CommonHeader.Type == HeapFile ? "Heap" : "Hash");
	printf("\tBucketCount: %d\n", fileHeader->BucketCount);
	printf("\tNextBlockIndex: %d\n", fileHeader->NextBlockIndex);

	int32_t currentBucketBlockIndex = fileHeader->NextBlockIndex;
	while (currentBucketBlockIndex != INVALID_BLOCK_INDEX)
	{
		uint8_t* currentBucketBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		BucketBlockHeader* currentBucketBlockHeader = (BucketBlockHeader*)currentBucketBlockPtr;
		currentBucketBlockPtr += sizeof(BucketBlockHeader);

		printf("Block %d:\n", currentBucketBlockIndex);
		printf("\tNextBlockIndex: %d\n", currentBucketBlockHeader->NextBlockIndex);

		int32_t bucketsInCurrentBlock = MAX_BUCKET_COUNT_PER_BLOCK;
		if (currentBucketBlockHeader->NextBlockIndex == INVALID_BLOCK_INDEX)
			bucketsInCurrentBlock = fileHeader->BucketCount % MAX_BUCKET_COUNT_PER_BLOCK;

		for (uint32_t bucketIndex = 0; bucketIndex < bucketsInCurrentBlock; bucketIndex++)
		{
			int32_t bucketValue = *(int32_t*)currentBucketBlockPtr;
			printf("\t\tBucketIndex: %d\n", bucketValue);

			int32_t currentDataBlockIndex = bucketValue;
			while (currentDataBlockIndex != INVALID_BLOCK_INDEX)
			{
				uint8_t* currentDataBlockPtr = nullptr;
				if (BF_ReadBlock(handle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
				{
					printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
					BF_PrintError("");

					return -1;
				}

				DataBlockHeader* currentDataBlockHeader = (DataBlockHeader*)currentDataBlockPtr;
				currentDataBlockPtr += sizeof(DataBlockHeader);

				printf("\t\t\tBlock %d:\n", currentDataBlockIndex);
				printf("\t\t\t\tRecordCount: %d\n", currentDataBlockHeader->RecordCount);
				printf("\t\t\t\tNextBlockIndex: %d\n", currentDataBlockHeader->NextBlockIndex);

				for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->RecordCount; recordIndex++)
				{
					Record* currentRecord = (Record*)currentDataBlockPtr;

					printf("\t\t\t\tRecord %d:\n", recordIndex);
					printf("\t\t\t\t\tID: %d\n", currentRecord->ID);
					printf("\t\t\t\t\tName: %s\n", currentRecord->Name);
					printf("\t\t\t\t\tSurname: %s\n", currentRecord->Surname);
					printf("\t\t\t\t\tAddress: %s\n", currentRecord->Address);

					currentDataBlockPtr += sizeof(Record);
				}

				currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
			}

			currentBucketBlockPtr += sizeof(int32_t);
		}

		currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;
	}

	return 0;
}
