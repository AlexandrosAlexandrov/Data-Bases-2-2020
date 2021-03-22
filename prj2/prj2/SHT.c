#include "SHT.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "BF/BF.h"

// The memory layout of a "record" in the secondary hash file.
typedef struct DataSegment
{
	// The surname.
	char Surname[25];

	// The block ID that is part of the primary hash file, where the record is stored.
	int32_t BlockID;
} DataSegment;

// Calculate at compile time the maximum number of data segments in a secondary hash data block.
#define MAX_DATA_SEGMENT_COUNT_PER_BLOCK ((BLOCK_SIZE - sizeof(HashDataBlockHeader)) / sizeof(DataSegment))

// Storage for the currently open secondary hash file handle.
static SHT_info s_HandleStorage = -1;

// Utility function that ensures a hash file exists and is valid.
static bool CheckForPrimaryHashFile(const char* fileName)
{
	// Open the block level file.
	HT_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
		return false;

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
		return false;

	// Since this file exists, we know there's a CommonFileHeader in the first bytes of the header block. So we treat the pointer as such.
	CommonFileHeader* commonFileHeader = (CommonFileHeader*)headerBlockPtr;

	// Ensure that the file we open is a indeed hash file.
	if (commonFileHeader->Type != HashFile)
		return false;

	// Close the block level file.
	if (BF_CloseFile(fileHandle) < 0)
		return false;

	return true;
}

// The hash function used to insert keys in the secondary hash table.
// Reference for the algorith: http://www.cse.yorku.ca/~oz/hash.html
uint32_t HashFunction(const char* string, int32_t hashTableSize)
{
	uint64_t hash = 5381;
	int8_t character = *string;

	while (character != '\0')
	{
		hash = ((hash << 5) + hash) + character;
		character = *(++string);
	}

	return hash % hashTableSize;
}

int32_t SHT_CreateSecondaryIndex(char* fileName, char attributeType, char* attributeName, int32_t attributeLength,
	int32_t bucketCount, char* primaryFileName)
{
	// Ensure the primary hash file exists and it's a valid hash file.
	if (!CheckForPrimaryHashFile(primaryFileName))
	{
		printf("The primary hash file was invalid at the time of SHT creation! PrimaryFileName: %s\n", primaryFileName);
		return -1;
	}

	// Create the block level file.
	if (BF_CreateFile(fileName) < 0)
	{
		printf("Could not create block level file for the secondary hash file! FileName: %s\n", fileName);
		BF_PrintError("");

		return -1;
	}

	// Open the block level file.
	SHT_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
	{
		printf("Could not open block level file for the secondary hash file! FileName: %s\n", fileName);
		BF_PrintError("");

		return -1;
	}

	// Allocate the hash file header block.
	if (BF_AllocateBlock(fileHandle) < 0)
	{
		printf("Could not allocate header block for the secondary hash file! FileHandle: %d\n", fileHandle);
		BF_PrintError("");

		return -1;
	}

	// Retrieve a pointer to the header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to secondary hash header block! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Create the hash file header and fill it's data.
	HashFileHeader header = { };
	header.CommonHeader.Type = SecondaryHashFile;
	header.BucketCount = bucketCount;
	header.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the file header into the hash file header block.
	memcpy(headerBlockPtr, &header, sizeof(HashFileHeader));

	// Write the hash file header block to the disk.
	if (BF_WriteBlock(fileHandle, HEADER_BLOCK_INDEX) < 0)
	{
		printf("Could not write secondary hash header block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
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

	// Loop through all the required secondary hash file bucket blocks.
	for (int32_t index = 0; index < requiredBlockCount; index++)
	{
		// Allocate a new bucket block.
		if (BF_AllocateBlock(fileHandle) < 0)
		{
			printf("Could not allocate bucket block for the secondary hash file! FileHandle: %d\n", fileHandle);
			BF_PrintError("");

			return -1;
		}

		// Retrieve the block count of the hash file.
		int32_t blockCount = BF_GetBlockCounter(fileHandle);
		if (blockCount < 0)
		{
			printf("Could not retrieve block count for the secondary hash file! FileHandle: %d\n", fileHandle);
			BF_PrintError("");

			return -1;
		}

		// Calculate the index of the new bucket block.
		int32_t newBucketBlockIndex = blockCount - 1;

		// Retrieve a pointer to the new bucket block.
		uint8_t* newBucketBlockPtr = nullptr;
		if (BF_ReadBlock(fileHandle, newBucketBlockIndex, (void**)&newBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", fileHandle, newBucketBlockIndex);
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
			printf("Could not write secondary hash bucket block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, newBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Now we need to update the previous bucket block's NextBlockIndex.

		// Retrieve a pointer to the previous bucket block.
		uint8_t* previousBucketBlockPtr = nullptr;
		if (BF_ReadBlock(fileHandle, previousBucketBlockIndex, (void**)&previousBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", fileHandle, previousBucketBlockIndex);
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
			printf("Could not write secondary hash bucket block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, previousBucketBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Update the previous bucket block index to the new bucket block index.
		previousBucketBlockIndex = newBucketBlockIndex;
	}

	// Now we need to insert any elements that were already in the primary hash file, into he secondary hash file.
	{
		uint32_t elementsInserted = 0;

		HT_info primaryHashFileHandle = BF_OpenFile(primaryFileName);
		if (primaryHashFileHandle < 0)
		{
			printf("Could not open block level file for the hash file! FileName: %s\n", primaryFileName);
			BF_PrintError("");

			return -1;
		}

		// Retrieve a pointer to the hash file header block.
		uint8_t* primaryHashHeaderBlockPtr = nullptr;
		if (BF_ReadBlock(primaryHashFileHandle, HEADER_BLOCK_INDEX, (void**)&primaryHashHeaderBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash header block! FileHandle: %d, BlockIndex: %d\n", primaryHashFileHandle, HEADER_BLOCK_INDEX);
			BF_PrintError("");

			return -1;
		}

		HashFileHeader* primaryHashFileHeader = (HashFileHeader*)primaryHashHeaderBlockPtr;

		// Start from the first bucket block.
		int32_t currentBucketBlockIndex = primaryHashFileHeader->NextBlockIndex;
		uint32_t bucketCount = primaryHashFileHeader->BucketCount;

		// Loop through all the bucket blocks.
		while (currentBucketBlockIndex != INVALID_BLOCK_INDEX)
		{
			// Retrieve a pointer to the current bucket block.
			uint8_t* currentBucketBlockPtr = nullptr;
			if (BF_ReadBlock(primaryHashFileHandle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
			{
				printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", primaryHashFileHandle, currentBucketBlockIndex);
				BF_PrintError("");

				return -1;
			}

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
					if (BF_ReadBlock(primaryHashFileHandle, currentDataBlockIndex, (void**)&currentDataBlockPtr) < 0)
					{
						printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", primaryHashFileHandle, currentDataBlockIndex);
						BF_PrintError("");

						return -1;
					}

					// Since this block exists we know there is a DataBlockHeader is the first byte so treat is as such.
					HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

					// Offset the pointer by the size of the header so it points to the beginning of the record data.
					currentDataBlockPtr += sizeof(HashDataBlockHeader);

					// Loop through all the records in the block.
					for (uint32_t recordIndex = 0; recordIndex < currentDataBlockHeader->ElementCount; recordIndex++)
					{
						// Get the current record and print it.
						Record* currentRecord = (Record*)currentDataBlockPtr;

						SecondaryRecord secondaryRecord = { };
						secondaryRecord.Record = *currentRecord;
						secondaryRecord.BlockID = currentDataBlockIndex;

						SHT_SecondaryInsertEntry(fileHandle, secondaryRecord);
						elementsInserted++;

						// Increment the pointer by the size of a record so it points to the next record in the block.
						currentDataBlockPtr += sizeof(Record);
					}

					// Update the current data block to point to the next one.
					currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
				}
			}

			free(bucketValues);
		}

		if (elementsInserted > 0)
			printf("Uppon SHT creation there were %d elements inserted that were already in the HT!\n", elementsInserted);
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

SHT_info* SHT_OpenSecondaryIndex(char* fileName)
{
	// Ensure that there are no files currently open.
	if (s_HandleStorage != -1)
	{
		printf("Cannot open secondary hash file since there's another file open!\n");
		return nullptr;
	}

	// Open the block level file.
	SHT_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
	{
		printf("Could not open block level file for the secondary hash file! FileName: %s\n", fileName);
		BF_PrintError("");

		return nullptr;
	}

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to secondary hash header block! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a CommonFileHeader in the first bytes of the header block. So we treat the pointer as such.
	CommonFileHeader* commonFileHeader = (CommonFileHeader*)headerBlockPtr;

	// Ensure that the file we open is a indeed hash file.
	if (commonFileHeader->Type != SecondaryHashFile)
	{
		printf("File specified is not a secondary hash file! FileName: %s\n", fileName);
		return nullptr;
	}

	// Store the handle in a global variable so that we can return a pointer to it.
	s_HandleStorage = fileHandle;

	return &s_HandleStorage;
}

int32_t SHT_CloseSecondaryIndex(SHT_info* handle)
{
	// Ensure that the file we want to close is actually open.
	if (s_HandleStorage != *handle)
	{
		printf("Cannot close secondary hash file since it's not open!\n");
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

int32_t SHT_SecondaryInsertEntry(SHT_info handle, SecondaryRecord record)
{
	// Retrieve a pointer to the secondary hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to secondary hash header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	HashFileHeader* fileHeader = (HashFileHeader*)headerBlockPtr;

	// Hash the record surname and find the bucket index.
	const char* surname = record.Record.Surname;
	int32_t bucketIndex = HashFunction(surname, fileHeader->BucketCount);

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
			printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
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
		printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
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
			printf("Could not retrieve pointer to secondary hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a DataBlockHeader in the first bytes of the block. So we treat the pointer as such.
		HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first data segment slot.
		currentDataBlockPtr += sizeof(HashDataBlockHeader);

		// Interate through all the data segment slots that are occupied in the current data block.
		for (uint32_t dataSegmentIndex = 0; dataSegmentIndex < currentDataBlockHeader->ElementCount; dataSegmentIndex++)
		{
			// Treat the current pointer as a data segment.
			DataSegment* currentDataSegment = (DataSegment*)currentDataBlockPtr;

			// If the current record's key is the same as the one we want to insert, it's already in the hash so we exit.
			if (strcmp(currentDataSegment->Surname, surname) == 0)
			{
				printf("The specified record is already in the secondary hash file! RecordID: %s\n", surname);
				return -1;
			}

			// Offset the block poiter by the size of a data segment so it pointes to the first byte of the next data segment slot.
			currentDataBlockPtr += sizeof(DataSegment);
		}

		// Update the current block index.
		currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
	}

	// If we're here the record is not in the hash so we try to insert it.

	// Create the data segment.
	DataSegment dataSegment = { };
	memcpy(dataSegment.Surname, surname, 25 * sizeof(char));
	dataSegment.BlockID = record.BlockID;

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
			printf("Could not retrieve pointer to secondary hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a DataBlockHeader in the first bytes of the block. So we treat the pointer as such.
		HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

		// If there's space in the current data block, we insert here.
		if (currentDataBlockHeader->ElementCount < MAX_DATA_SEGMENT_COUNT_PER_BLOCK)
		{
			// Offset the block pointer by the size of the header so it points to the first byte of the first data segment slot.
			currentDataBlockPtr += sizeof(HashDataBlockHeader);

			// Offset the block pointer by the size of the data segment times the number of data segments so it points to the first byte
			// of the first empty data segment slot.
			currentDataBlockPtr += currentDataBlockHeader->ElementCount * sizeof(DataSegment);

			// Copy the data segment into the data block.
			memcpy(currentDataBlockPtr, &dataSegment, sizeof(DataSegment));

			// Increment the current data block's data segment count.
			currentDataBlockHeader->ElementCount++;

			// Write the updated contents of the current hash file data block to the disk.
			if (BF_WriteBlock(handle, currentDataBlockIndex) < 0)
			{
				printf("Could not write secondary hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Return a successful code.
			return 0;
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
		printf("Could not allocate data block for the secondary hash file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Get the new block count.
	int32_t blockCount = BF_GetBlockCounter(handle);
	if (blockCount < 0)
	{
		printf("Could not retrieve block count for the secondary hash file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Calculate the new data block index.
	int32_t newDataBlockIndex = blockCount - 1;

	// Retrieve a pointer to the new data block.
	uint8_t* newDataBlockPtr = nullptr;
	if (BF_ReadBlock(handle, newDataBlockIndex, (void**)&newDataBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to secondary hash data block! FileHandle: %d, BlockIndex: %d\n", handle, newDataBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Create the data block header and fill it's data.
	HashDataBlockHeader newDataBlockHeader = { };
	newDataBlockHeader.ElementCount = 1;
	newDataBlockHeader.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the new data block header into the new data block.
	memcpy(newDataBlockPtr, &newDataBlockHeader, sizeof(HashDataBlockHeader));

	// Offset the data block pointer by the size of the header so it points to the first byte of the first data segment slot.
	newDataBlockPtr += sizeof(HashDataBlockHeader);

	// Copy the data segment into the block.
	memcpy(newDataBlockPtr, &dataSegment, sizeof(DataSegment));

	// Write the contents of the new hash data block to the disk.
	if (BF_WriteBlock(handle, newDataBlockIndex) < 0)
	{
		printf("Could not write secondary hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, newDataBlockIndex);
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
			printf("Could not retrieve pointer to secondary hash data block! FileHandle: %d, BlockIndex: %d\n", handle, previousDataBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Get the header and update the next block index.
		HashDataBlockHeader* previousDataBlockHeader = (HashDataBlockHeader*)previousDataBlockPtr;
		previousDataBlockHeader->NextBlockIndex = newDataBlockIndex;

		// Write the contents of the previous hash data block to the disk.
		if (BF_WriteBlock(handle, previousDataBlockIndex) < 0)
		{
			printf("Could not write secondary hash data block to disk! FileHandle: %d, BlockIndex: %d\n", handle, previousDataBlockIndex);
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
			printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
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
			printf("Could not write secondary hash bucket block to disk! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
			BF_PrintError("");

			return -1;
		}
	}

	return 0;
}

int32_t SHT_SecondaryGetAllEntries(SHT_info handle, HT_info primaryHandle, void* keyValue)
{
	// Key value can be nullptr. If it's not get the actual value otherwise use a dummy.
	char key[25];
	memset(key, 0, 25 * sizeof(char));
	bool printAll = true;

	if (keyValue != nullptr)
	{
		memcpy(key, keyValue, (strlen((const char*)keyValue) + 1) * sizeof(char));
		printAll = false;
	}

	// Retrieve a pointer to the hash file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to secondary hash header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	HashFileHeader* fileHeader = (HashFileHeader*)headerBlockPtr;

	// The number of blocks that we traversed. Set to one to account for the hash file header block.
	uint32_t blocksTraversed = 1;

	if (!printAll)
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
				printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
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
			printf("Could not retrieve pointer to secondary hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, bucketBlockIndex);
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
				printf("Could not retrieve pointer to secondary hash data block! FileHandle: %d, BlockIndex: %d\n", handle, currentDataBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Increment the blocks traversed counter.
			blocksTraversed++;

			// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
			HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

			// Offset the block pointer by the size of the header so it points to the first byte of the first data segment slot.
			currentDataBlockPtr += sizeof(HashDataBlockHeader);

			// Interate through all the data segment slots that are occupied in the current block.
			for (uint32_t dataSegmentIndex = 0; dataSegmentIndex < currentDataBlockHeader->ElementCount; dataSegmentIndex++)
			{
				// Treat the current pointer as a data segment.
				DataSegment* currentDataSegment = (DataSegment*)currentDataBlockPtr;

				// If the current records ID is the same as the key, we want to print it and exit.
				if (strcmp(currentDataSegment->Surname, key) == 0)
				{
					// Now we want to look for it in the primary hash file.

					// Retrieve a pointer to the current hash data block.
					uint8_t* primaryHashDataBlockPtr = nullptr;
					if (BF_ReadBlock(primaryHandle, currentDataSegment->BlockID, (void**)&primaryHashDataBlockPtr) < 0)
					{
						printf("Could not retrieve pointer to hash data block! FileHandle: %d, BlockIndex: %d\n", primaryHandle, currentDataBlockIndex);
						BF_PrintError("");

						return -1;
					}

					// Increment the blocks traversed counter.
					blocksTraversed++;

					// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
					HashDataBlockHeader* primaryHashDataBlockHeader = (HashDataBlockHeader*)primaryHashDataBlockPtr;

					// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
					primaryHashDataBlockPtr += sizeof(HashDataBlockHeader);

					// Interate through all the record slots that are occupied in the current block.
					for (uint32_t recordIndex = 0; recordIndex < primaryHashDataBlockHeader->ElementCount; recordIndex++)
					{
						// Treat the current pointer as a record.
						Record* currentRecord = (Record*)primaryHashDataBlockPtr;

						// If the current records ID is the same as the key, we want to print it and exit.
						if (strcmp(currentRecord->Surname, key) == 0)
						{
							printf("ID: %d, Name: %s, Surname: %s, Address: %s\n", currentRecord->ID, currentRecord->Name, currentRecord->Surname, currentRecord->Address);
							return blocksTraversed;
						}

						// Offset the block poiter by the size of a record so it pointer to the first byte of the next record slot.
						primaryHashDataBlockPtr += sizeof(Record);
					}

					// Return an error since the record was not found in the primary index.
					return -1;
				}

				// Offset the block poiter by the size of a data segment so it pointer to the first byte of the next data segment slot.
				currentDataBlockPtr += sizeof(DataSegment);
			}

			// Update the current block index.
			currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
		}

		// If we are here, it means that the record with the specified key was not found in the hash file.
		printf("Could not find record with key %s!\n", key);
		return -1;
	}
	else
	{
		printf("Invalid key!\n");
		return -1;
	}

	return -1;
}
