#include "Common.h"

#include "BF/BF.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

int32_t HashStatistics(char* fileName)
{
	// Open the hash file.
	int32_t handle = BF_OpenFile(fileName);
	if (handle == nullptr)
	{
		printf("Could not open hash file!\n");
		return -1;
	}

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

	if (fileHeader->CommonHeader.Type != HashFile && fileHeader->CommonHeader.Type != SecondaryHashFile)
	{
		printf("The file provided is not a hash file! FileName: %s\n", fileName);
		return -1;
	}

	uint32_t bucketCount = fileHeader->BucketCount;

	// Initialize the statistics.
	uint32_t minElementCount = UINT32_MAX;
	uint32_t maxElementCount = 0;
	uint32_t totalElementCount = 0;
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
		if (BF_ReadBlock(handle, currentBucketBlockIndex, (void**)&currentBucketBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to hash bucket block! FileHandle: %d, BlockIndex: %d\n", handle, currentBucketBlockIndex);
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

		// Loop though all the buckets in the block.
		for (uint32_t bucketIndex = 0; bucketIndex < bucketsInCurrentBlock; bucketIndex++)
		{
			// Start from the first data block.
			int32_t currentDataBlockIndex = bucketValues[bucketIndex];

			// The number of elements in the bucket.
			uint32_t elementCount = 0;

			// The number of blocks in the bucket.
			uint32_t blockCount = 0;

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

				// Increment the number of blocks in the bucket.
				blockCount++;

				// Since this block exists we know there is a DataBlockHeader is the first byte so treat is as such.
				HashDataBlockHeader* currentDataBlockHeader = (HashDataBlockHeader*)currentDataBlockPtr;

				// Increment the record count by the number of records in the block.
				elementCount += currentDataBlockHeader->ElementCount;

				// Update the current data block to point to the next one.
				currentDataBlockIndex = currentDataBlockHeader->NextBlockIndex;
			}

			// If the current bucket is not invalid, we need to update the results.
			if (bucketValues[bucketIndex] != INVALID_BLOCK_INDEX)
			{
				// Update the min record count.
				if (elementCount < minElementCount)
					minElementCount = elementCount;

				// Update the max record count.
				if (elementCount > maxElementCount)
					maxElementCount = elementCount;

				// Update the total record count.
				totalElementCount += elementCount;

				// Update the overflow block counts. Subtract one to account for the first block.
				overflowBlocksPerBucket[globalBucketIndex] = blockCount - 1;
			}

			// Increment the global bucket index.
			globalBucketIndex++;
		}

		free(bucketValues);
	}

	// Retrieve the block count.
	int32_t blockCount = BF_GetBlockCounter(handle);
	if (blockCount < 0)
	{
		printf("Could not retrieve block count for the hash file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Calculate the average record count.
	float averageElementCount = (float)totalElementCount / (float)bucketCount;

	// Print the statistics.
	printf("Block Count in the hash file: %d\n", blockCount);
	printf("Max Record Count in a bucket: %d\n", maxElementCount);
	printf("Min Record Count in a bucket: %d\n", minElementCount);
	printf("Average Record Count per bucket: %f\n", averageElementCount);

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
	if (BF_CloseFile(handle) == -1)
	{
		printf("Could not close hash file!\n");
		return -1;
	}

	return 0;
}
