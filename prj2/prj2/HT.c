#include "HT.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "BF/BF.h"

// Calculate at compile time the maximum number of records in a hash data block.
#define MAX_RECORD_COUNT_PER_BLOCK ((BLOCK_SIZE - sizeof(HashDataBlockHeader)) / sizeof(Record))

// Storage for the currently open hash file handle.
static HT_info s_HandleStorage = -1;

// The hash function used to insert keys in the hash table.
// Reference for the algorith: https://burtleburtle.net/bob/hash/integer.html
static int32_t HashFunction(int32_t key, int32_t hashTableSize)
{
	key -= (key << 6);
	key ^= (key >> 17);
	key -= (key << 9);
	key ^= (key << 4);
	key -= (key << 3);
	key ^= (key << 10);
	key ^= (key >> 15);

	return key % hashTableSize;
}

int32_t HT_CreateIndex(char* fileName, char attributeType, char* attributeName, int32_t attributeLength, int32_t bucketCount)
{
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
	HashFileHeader header = { };
	header.CommonHeader.Type = HashFile;
	header.BucketCount = bucketCount;
	header.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the file header into the hash file header block.
	memcpy(headerBlockPtr, &header, sizeof(HashFileHeader));

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
		HashBucketBlockHeader newBucketBlockHeader = { };
		newBucketBlockHeader.NextBlockIndex = INVALID_BLOCK_INDEX;

		// Copy the new bucket block header to he beginning of the new bucket block.
		memcpy(newBucketBlockPtr, &newBucketBlockHeader, sizeof(HashBucketBlockHeader));

		// Offset the new bucket block pointer to point to the first byte of the bucket indices.
		newBucketBlockPtr += sizeof(HashBucketBlockHeader);

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
			HashFileHeader* previousBlockHeader = (HashFileHeader*)previousBucketBlockPtr;
			previousBlockHeader->NextBlockIndex = newBucketBlockIndex;
		}
		else
		{
			HashBucketBlockHeader* previousBlockHeader = (HashBucketBlockHeader*)previousBucketBlockPtr;
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

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	HashFileHeader* fileHeader = (HashFileHeader*)headerBlockPtr;

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
		HashBucketBlockHeader* currentBucketBlockHeader = (HashBucketBlockHeader*)currentBucketBlockPtr;

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
	bucketBlockPtr += sizeof(HashBucketBlockHeader);

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
		HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentDataBlockPtr += sizeof(HashDataBlockHeader);

		// Interate through all the record slots that are occupied in the current data block.
		for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->ElementCount; recordIndex++)
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
		HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

		// If there's space in the current data block, we insert here.
		if (currentDataBlockHeader->ElementCount < MAX_RECORD_COUNT_PER_BLOCK)
		{
			// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
			currentDataBlockPtr += sizeof(HashDataBlockHeader);

			// Offset the block pointer by the size of the record times the number of records so it points to the first byte
			// of the first empty record slot.
			currentDataBlockPtr += currentDataBlockHeader->ElementCount * sizeof(Record);

			// Copy the record into the data block.
			memcpy(currentDataBlockPtr, &record, sizeof(Record));

			// Increment the current data block's record count.
			currentDataBlockHeader->ElementCount++;

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
	HashDataBlockHeader newDataBlockHeader = { };
	newDataBlockHeader.ElementCount = 1;
	newDataBlockHeader.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the new data block header into the new data block.
	memcpy(newDataBlockPtr, &newDataBlockHeader, sizeof(HashDataBlockHeader));

	// Offset the data block pointer by the size of the header so it points to the first byte of the first record slot.
	newDataBlockPtr += sizeof(HashDataBlockHeader);

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
		HashDataBlockHeader* previousDataBlockHeader = (HashDataBlockHeader*)previousDataBlockPtr;
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
		bucketBlockPtr += sizeof(HashBucketBlockHeader);

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
	HashFileHeader* fileHeader = (HashFileHeader*)headerBlockPtr;

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
		HashBucketBlockHeader* currentBucketBlockHeader = (HashBucketBlockHeader*)currentBucketBlockPtr;

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
	bucketBlockPtr += sizeof(HashBucketBlockHeader);

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
		HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentDataBlockPtr += sizeof(HashDataBlockHeader);

		// Interate through all the record slots that are occupied in the current block.
		for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->ElementCount; recordIndex++)
		{
			// Treat the current pointer as a record.
			Record* currentRecord = (Record*)currentDataBlockPtr;

			// If the current records ID is the same as the key, we want to delete it and exit.
			if (currentRecord->ID == key)
			{
				// What we want to do is move the contents of the records after the current record up the size of one record.

				// Calculate the size of the records after the current record.
				uint32_t byteCountOfRecordDataAfterCurrentRecord = (currentDataBlockHeader->ElementCount - (recordIndex + 1)) * sizeof(Record);

				// Copy the records after the current record, to the current record's position in the block.
				memcpy(currentDataBlockPtr, currentDataBlockPtr + sizeof(Record), byteCountOfRecordDataAfterCurrentRecord);

				// Decrement the current block record count;
				currentDataBlockHeader->ElementCount--;

				// Offset the block pointer by the size of a block times the number of records left in the block after the current one.
				// This way the pointer points to the first byte of the empty space in the block.
				currentDataBlockPtr += byteCountOfRecordDataAfterCurrentRecord;

				// Calculate the number of empty bytes in the block.
				uint32_t emptyByteCount = BLOCK_SIZE - (sizeof(HashDataBlockHeader) + currentDataBlockHeader->ElementCount * sizeof(Record));

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

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	HashFileHeader* fileHeader = (HashFileHeader*)headerBlockPtr;

	// The number of blocks that we traversed. Set to one to account for the hash file header block.
	uint32_t blocksTraversed = 1;

	if (key != -1)
	{
		// If key is valid, search for the entry.

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

			// Increment the blocks traversed counter.
			blocksTraversed++;

			// Since this file exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
			HashBucketBlockHeader* currentBucketBlockHeader = (HashBucketBlockHeader*)currentBucketBlockPtr;

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
		bucketBlockPtr += sizeof(HashBucketBlockHeader);

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
			HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

			// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
			currentDataBlockPtr += sizeof(HashDataBlockHeader);

			// Interate through all the record slots that are occupied in the current block.
			for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->ElementCount; recordIndex++)
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
		uint32_t bucketCount = fileHeader->BucketCount;

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

			// Increment the blocks traversed counter.
			blocksTraversed++;

			// Since this block exists, we know there's a BucketBlockHeader in the first bytes of the header block. So we treat the pointer as such.
			HashBucketBlockHeader* currentBucketBlockHeader = (HashBucketBlockHeader*)currentBucketBlockPtr;

			// Offset the pointer by the size of the header so it points to the first bucket.
			currentBucketBlockPtr += sizeof(HashBucketBlockHeader);

			// Update the current bucket block to point to the next one.
			currentBucketBlockIndex = currentBucketBlockHeader->NextBlockIndex;

			// Calculate the number of buckets in the current bucket block. If it's not the last one there's the
			// max number of buckets. Otherwise it's the remainder.
			int32_t bucketsInCurrentBlock = MAX_BUCKET_COUNT_PER_BLOCK;
			if (currentBucketBlockHeader->NextBlockIndex == INVALID_BLOCK_INDEX)
				bucketsInCurrentBlock = bucketCount % MAX_BUCKET_COUNT_PER_BLOCK;

			// Store the values for the buckets because the block will get unloaded.
			int32_t* bucketValues = (int32_t*)malloc(bucketsInCurrentBlock * sizeof(int32_t));
			memcpy(bucketValues, currentBucketBlockPtr, bucketsInCurrentBlock * sizeof(int32_t));

			for (uint32_t bucketIndex = 0; bucketIndex < bucketsInCurrentBlock; bucketIndex++)
			{
				// Start from the first data block.
				int32_t currentDataBlockIndex = bucketValues[bucketIndex];

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

					// Increment the blocks traversed counter.
					blocksTraversed++;

					// Since this block exists we know there is a DataBlockHeader is the first byte so treat is as such.
					HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

					// Offset the pointer by the size of the header so it points to the beginning of the record data.
					currentDataBlockPtr += sizeof(HashDataBlockHeader);

					// Loop through all the records in the block.
					for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->ElementCount; recordIndex++)
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
			}

			free(bucketValues);
		}

		return blocksTraversed;
	}
}
