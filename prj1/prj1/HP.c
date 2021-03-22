#include "HP.h"

#include <string.h>
#include <stdio.h>

#include "BF/BF.h"

// The index of the heap file header block.
#define HEADER_BLOCK_INDEX 0

// An invalid block index.
#define INVALID_BLOCK_INDEX -1

// The memory layout of the heap file header block. This structure is stored only on the first block of a heap file.
typedef struct FileHeader
{
	// The common header that every file in this application has.
	CommonFileHeader CommonHeader;

	// The index of the next block in the heap file.
	int32_t NextBlockIndex;
} FileHeader;

// The memory layout of a heap file block. This structure is stored in all heap file blocks except of the first one.
typedef struct BlockHeader
{
	// The number of records in the current block.
	uint8_t RecordCount;

	// The index of the next block in the heap file.
	int32_t NextBlockIndex;
} BlockHeader;

// Calculate at compile time the maximum number of records in a heap block.
#define MAX_RECORD_COUNT_PER_BLOCK ((BLOCK_SIZE - sizeof(BlockHeader)) / sizeof(Record))

// Storage for the currently open heap file handle.
static HP_info s_HandleStorage = -1;

int32_t HP_CreateFile(char* fileName, char attributeType, char* attributeName, int32_t attributeLength)
{
	// Initialize the block level.
	BF_Init();

	// Create the block level file.
	if (BF_CreateFile(fileName) < 0)
	{
		printf("Could not create block level file for the heap file! FileName: %s\n", fileName);
		BF_PrintError("");

		return -1;
	}

	// Open the block level file.
	HP_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
	{
		printf("Could not open block level file for the heap file! FileName: %s\n", fileName);
		BF_PrintError("");

		return -1;
	}

	// Allocate the heap file header block.
	if (BF_AllocateBlock(fileHandle) < 0)
	{
		printf("Could not allocate header block for the heap file! FileHandle: %d\n", fileHandle);
		BF_PrintError("");

		return -1;
	}

	// Retrieve a pointer to the heap file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file header block! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Create the heap file header and fill it's data.
	FileHeader header = { };
	header.CommonHeader.Type = HeapFile;
	header.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the file header into the heap file header block.
	memcpy(headerBlockPtr, &header, sizeof(FileHeader));

	// Write the contents of the heap file header block to the disk.
	if (BF_WriteBlock(fileHandle, HEADER_BLOCK_INDEX) < 0)
	{
		printf("Could not write heap file header block to disk! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
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

HP_info* HP_OpenFile(char* fileName)
{
	// Ensure that there are no files currently open.
	if (s_HandleStorage != -1)
	{
		printf("Cannot open heap file since there's another file open!\n");
		return nullptr;
	}

	// Open the block level file.
	HP_info fileHandle = BF_OpenFile(fileName);
	if (fileHandle < 0)
	{
		printf("Could not open block level file for the heap file! FileName: %s\n", fileName);
		BF_PrintError("");

		return nullptr;
	}

	// Retrieve a pointer to the heap file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(fileHandle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file header block! FileHandle: %d, BlockIndex: %d\n", fileHandle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return nullptr;
	}

	// Since this file exists, we know there's a CommonFileHeader in the first bytes of the header block. So we treat the pointer as such.
	CommonFileHeader* commonFileHeader = (CommonFileHeader*)headerBlockPtr;

	// Ensure that the file we open is a indeed heap file.
	if (commonFileHeader->Type != HeapFile)
	{
		printf("File specified is not a heap file! FileName: %s\n", fileName);
		return nullptr;
	}

	// Store the handle in a global variable so that we can return a pointer to it.
	s_HandleStorage = fileHandle;

	return &s_HandleStorage;
}

int32_t HP_CloseFile(HP_info* handle)
{
	// Ensure that the file we want to close is actually open.
	if (s_HandleStorage != *handle)
	{
		printf("Cannot close heap file since it's not open!\n");
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

int32_t HP_InsertEntry(HP_info handle, Record record)
{
	// Retrieve a pointer to the heap file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	// First we need to look for the record and make sure it's not already in the heap file.

	// Start from the first actual block.
	int32_t currentBlockIndex = fileHeader->NextBlockIndex;

	// Loop until the end of the allocated blocks.
	while (currentBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current heap file block.
		uint8_t* currentBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBlockIndex, (void**)&currentBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
		BlockHeader* currentBlockHeader = (BlockHeader*)currentBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentBlockPtr += sizeof(BlockHeader);

		// Interate through all the record slots that are occupied in the current block.
		for (uint32_t recordIndex = 0; recordIndex < currentBlockHeader->RecordCount; recordIndex++)
		{
			// Treat the current pointer as a record.
			Record* currentRecord = (Record*)currentBlockPtr;

			// If the current record's key is the same as the one we want to insert, it's already in the heap so we exit.
			if (currentRecord->ID == record.ID)
			{
				printf("The specified record is already in the heap file! RecordID: %d\n", record.ID);
				return -1;
			}

			// Offset the block poiter by the size of a record so it pointes to the first byte of the next record slot.
			currentBlockPtr += sizeof(Record);
		}

		// Update the current block index.
		currentBlockIndex = currentBlockHeader->NextBlockIndex;
	}

	// Reset the current block index and prepare for insertion.
	currentBlockIndex = fileHeader->NextBlockIndex;

	// The previous block is the header block initially.
	int32_t previousBlockIndex = HEADER_BLOCK_INDEX;

	// Loop until the end of the allocated blocks.
	while (currentBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current heap file block.
		uint8_t* currentBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBlockIndex, (void**)&currentBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
		BlockHeader* currentBlockHeader = (BlockHeader*)currentBlockPtr;

		// If there's space in the current block, we insert here.
		if (currentBlockHeader->RecordCount < MAX_RECORD_COUNT_PER_BLOCK)
		{
			// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
			currentBlockPtr += sizeof(BlockHeader);

			// Offset the block pointer by the size of the record times the number of records so it points to the first byte
			// of the first empty record slot.
			currentBlockPtr += currentBlockHeader->RecordCount * sizeof(Record);

			// Copy the record into the block.
			memcpy(currentBlockPtr, &record, sizeof(Record));

			// Increment the current block's record count.
			currentBlockHeader->RecordCount++;

			// Write the updated contents of the current heap file block to the disk.
			if (BF_WriteBlock(handle, currentBlockIndex) < 0)
			{
				printf("Could not write heap file block to disk! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
				BF_PrintError("");

				return -1;
			}

			// Return the current block's index.
			return currentBlockIndex;
		}

		// Update the previous and current block indices.
		previousBlockIndex = currentBlockIndex;
		currentBlockIndex = currentBlockHeader->NextBlockIndex;
	}

	// If we are here, a new block needs to be created. Either because this is the first entry in the heap file or because we ran
	// out of space in all of the currently allocated blocks.

	// Allocate a new block.
	if (BF_AllocateBlock(handle) < 0)
	{
		printf("Could not allocate block for the heap file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Get the new block count.
	int32_t blockCount = BF_GetBlockCounter(handle);
	if (blockCount < 0)
	{
		printf("Could not retrieve block count for the heap file! FileHandle: %d\n", handle);
		BF_PrintError("");

		return -1;
	}

	// Calculate the new block index.
	int32_t newBlockIndex = blockCount - 1;

	// Retrieve a pointer to the new heap file block.
	uint8_t* newBlockPtr = nullptr;
	if (BF_ReadBlock(handle, newBlockIndex, (void**)&newBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, newBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Create the block header and fill it's data.
	BlockHeader newBlockHeader = { };
	newBlockHeader.RecordCount = 1;
	newBlockHeader.NextBlockIndex = INVALID_BLOCK_INDEX;

	// Copy the new block header into the new block.
	memcpy(newBlockPtr, &newBlockHeader, sizeof(BlockHeader));

	// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
	newBlockPtr += sizeof(BlockHeader);

	// Copy the record into the block.
	memcpy(newBlockPtr, &record, sizeof(Record));

	// Write the contents of the new heap file block to the disk.
	if (BF_WriteBlock(handle, newBlockIndex) < 0)
	{
		printf("Could not write heap file block to disk! FileHandle: %d, BlockIndex: %d\n", handle, newBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Now we need to update the previous block's NextBlockIndex.

	// Retrieve a pointer to the previous heap file block.
	uint8_t* previousBlockPtr = nullptr;
	if (BF_ReadBlock(handle, previousBlockIndex, (void**)&previousBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, previousBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// The head has a different header structure so we treat it differently.
	if (previousBlockIndex == HEADER_BLOCK_INDEX)
	{
		FileHeader* previousBlockHeader = (FileHeader*)previousBlockPtr;
		previousBlockHeader->NextBlockIndex = newBlockIndex;
	}
	else
	{
		BlockHeader* previousBlockHeader = (BlockHeader*)previousBlockPtr;
		previousBlockHeader->NextBlockIndex = newBlockIndex;
	}

	// Write the contents of the previous heap file block to the disk.
	if (BF_WriteBlock(handle, previousBlockIndex) < 0)
	{
		printf("Could not write heap file block to disk! FileHandle: %d, BlockIndex: %d\n", handle, previousBlockIndex);
		BF_PrintError("");

		return -1;
	}

	// Return the index of the new block.
	return newBlockIndex;
}

int32_t HP_DeleteEntry(HP_info handle, void* keyValue)
{
	// Extract the key from the key value pointer.
	int32_t key = *(int32_t*)keyValue;

	// Retrieve a pointer to the heap file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	// Start from the first actual block.
	int32_t currentBlockIndex = fileHeader->NextBlockIndex;

	// Loop until the end of the allocated blocks.
	while (currentBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current heap file block.
		uint8_t* currentBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBlockIndex, (void**)&currentBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
		BlockHeader* currentBlockHeader = (BlockHeader*)currentBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentBlockPtr += sizeof(BlockHeader);

		// Interate through all the record slots that are occupied in the current block.
		for (uint32_t recordIndex = 0; recordIndex < currentBlockHeader->RecordCount; recordIndex++)
		{
			// Treat the current pointer as a record.
			Record* currentRecord = (Record*)currentBlockPtr;

			// If the current records ID is the same as the key, we want to delete it and exit.
			if (currentRecord->ID == key)
			{
				// What we want to do is move the contents of the records after the current record up the size of one record.

				// Calculate the size of the records after the current record.
				uint32_t byteCountOfRecordDataAfterCurrentRecord = (currentBlockHeader->RecordCount - (recordIndex + 1)) * sizeof(Record);

				// Copy the records after the current record, to the current record's position in the block.
				memcpy(currentBlockPtr, currentBlockPtr + sizeof(Record), byteCountOfRecordDataAfterCurrentRecord);

				// Decrement the current block record count;
				currentBlockHeader->RecordCount--;

				// Offset the block pointer by the size of a block times the number of records left in the block after the current one.
				// This way the pointer points to the first byte of the empty space in the block.
				currentBlockPtr += byteCountOfRecordDataAfterCurrentRecord;

				// Calculate the number of empty bytes in the block.
				uint32_t emptyByteCount = BLOCK_SIZE - (sizeof(BlockHeader) + currentBlockHeader->RecordCount * sizeof(Record));

				// Set the empty bytes to zero.
				memset(currentBlockPtr, 0, emptyByteCount);

				// Write the updated contents of the current heap file block to the disk.
				if (BF_WriteBlock(handle, currentBlockIndex) < 0)
				{
					printf("Could not write heap file block to disk! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
					BF_PrintError("");

					return -1;
				}

				// Exit the function since we deleted.
				return 0;
			}

			// Offset the block poiter by the size of a record so it pointer to the first byte of the next record slot.
			currentBlockPtr += sizeof(Record);
		}

		// Update the current block index.
		currentBlockIndex = currentBlockHeader->NextBlockIndex;
	}

	// If we're here, the record with key keyValue was not found.
	printf("Could not find record with key %d!\n", key);
	return -1;
}

int32_t HP_GetAllEntries(HP_info handle, void* keyValue)
{
	// Retrieve a pointer to the heap file header block.
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	// Since this file exists, we know there's a FileHeader in the first bytes of the header block. So we treat the pointer as such.
	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	// Start from the first actual block.
	int32_t currentBlockIndex = fileHeader->NextBlockIndex;

	// The number of blocks that we traversed. Set to one to account for the heap file header block.
	uint32_t blocksTraversed = 1;

	// Loop until the end of the allocated blocks.
	while (currentBlockIndex != INVALID_BLOCK_INDEX)
	{
		// Retrieve a pointer to the current heap file block.
		uint8_t* currentBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBlockIndex, (void**)&currentBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
			BF_PrintError("");

			return -1;
		}

		// Increment the blocks traversed counter.
		blocksTraversed++;

		// Since this file exists, we know there's a BlockHeader in the first bytes of the block. So we treat the pointer as such.
		BlockHeader* currentBlockHeader = (BlockHeader*)currentBlockPtr;

		// Offset the block pointer by the size of the header so it points to the first byte of the first record slot.
		currentBlockPtr += sizeof(BlockHeader);

		// Interate through all the record slots that are occupied in the current block.
		for (uint32_t recordIndex = 0; recordIndex < currentBlockHeader->RecordCount; recordIndex++)
		{
			// Treat the current pointer as a record.
			Record* currentRecord = (Record*)currentBlockPtr;

			if (keyValue == nullptr)
			{
				// If the key value is nullptr, we print the record regardless.
				printf("ID: %d, Name: %s, Surname: %s, Address: %s\n", currentRecord->ID, currentRecord->Name, currentRecord->Surname, currentRecord->Address);
			}
			else
			{
				// Otherwise, extract the key from the key value pointer.
				int32_t key = *(int32_t*)keyValue;

				// If the current records ID is the same as the key, we want to print it and exit.
				if (currentRecord->ID == key)
				{
					printf("ID: %d, Name: %s, Surname: %s, Address: %s\n", currentRecord->ID, currentRecord->Name, currentRecord->Surname, currentRecord->Address);
					return blocksTraversed;
				}
			}

			// Offset the block poiter by the size of a record so it pointer to the first byte of the next record slot.
			currentBlockPtr += sizeof(Record);
		}

		// Update the current block index.
		currentBlockIndex = currentBlockHeader->NextBlockIndex;
	}
	
	// If we are here and the key value is not nullptr, it means that the record with the specified key was not found in the heap file.
	if (keyValue != nullptr)
	{
		int32_t key = *(int32_t*)keyValue;
		printf("Could not find record with key %d!\n", key);
		return -1;
	}

	// Otherwise, we printed all the records.
	return 0;
}

// TODO: Remove this!
int32_t HP_DebugPrint(HP_info handle)
{
	uint8_t* headerBlockPtr = nullptr;
	if (BF_ReadBlock(handle, HEADER_BLOCK_INDEX, (void**)&headerBlockPtr) < 0)
	{
		printf("Could not retrieve pointer to heap file header block! FileHandle: %d, BlockIndex: %d\n", handle, HEADER_BLOCK_INDEX);
		BF_PrintError("");

		return -1;
	}

	FileHeader* fileHeader = (FileHeader*)headerBlockPtr;

	printf("Block %d:\n", HEADER_BLOCK_INDEX);
	printf("\tType: %s\n", fileHeader->CommonHeader.Type == HeapFile ? "Heap" : "Hash");
	printf("\tNextBlockIndex: %d\n", fileHeader->NextBlockIndex);

	int32_t currentBlockIndex = fileHeader->NextBlockIndex;
	while (currentBlockIndex != INVALID_BLOCK_INDEX)
	{
		uint8_t* currentBlockPtr = nullptr;
		if (BF_ReadBlock(handle, currentBlockIndex, (void**)&currentBlockPtr) < 0)
		{
			printf("Could not retrieve pointer to heap file block! FileHandle: %d, BlockIndex: %d\n", handle, currentBlockIndex);
			BF_PrintError("");

			return -1;
		}

		BlockHeader* currentBlockHeader = (BlockHeader*)currentBlockPtr;
		currentBlockPtr += sizeof(BlockHeader);

		printf("Block %d:\n", currentBlockIndex);
		printf("\tRecordCount: %d\n", currentBlockHeader->RecordCount);
		printf("\tNextBlockIndex: %d\n", currentBlockHeader->NextBlockIndex);

		for (uint32_t recordIndex = 0; recordIndex < currentBlockHeader->RecordCount; recordIndex++)
		{
			Record* currentRecord = (Record*)currentBlockPtr;

			printf("\tRecord %d:\n", recordIndex);
			printf("\t\tID: %d\n", currentRecord->ID);
			printf("\t\tName: %s\n", currentRecord->Name);
			printf("\t\tSurname: %s\n", currentRecord->Surname);
			printf("\t\tAddress: %s\n", currentRecord->Address);

			currentBlockPtr += sizeof(Record);
		}

		currentBlockIndex = currentBlockHeader->NextBlockIndex;
	}

	return 0;
}
