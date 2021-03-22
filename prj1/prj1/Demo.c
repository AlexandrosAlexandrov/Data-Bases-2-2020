#if HEAP_FILE
#include "HP.h"

#include <stdio.h>

// Entry Point
int main()
{
	// Create a test heap file.
	if (HP_CreateFile("TestHeapFile", 'i', "TestHeapFile", 7) == -1)
	{
		printf("Could not create heap file!\n");
		return -1;
	}

	// Open the test heap file.
	HP_info* heapFileHandle = HP_OpenFile("TestHeapFile");
	if (heapFileHandle == nullptr)
	{
		printf("Could not open heap file!\n");
		return -1;
	}

	printf("File created. Press enter to insert 17 elements...\n");
	getchar();

	// Insert 17 test elements.
	uint32_t recordCount = 17;
	for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
	{
		Record record = { };
		record.ID = recordIndex + 1;
		sprintf(record.Name, "Name %d", recordIndex + 1);
		sprintf(record.Surname, "Surname %d", recordIndex + 1);
		sprintf(record.Address, "Address %d", recordIndex + 1);

		if (HP_InsertEntry(*heapFileHandle, record) == -1)
		{
			printf("Could not insert record with index %d!\n", recordIndex);
			return -1;
		}
	}

	printf("Elements inserted. Press enter to print them...\n");
	getchar();

	// Print all the elements.
	int32_t blocksTraversed = HP_GetAllEntries(*heapFileHandle, nullptr);
	if (blocksTraversed == -1)
	{
		printf("Could not get heap entries!\n");
		return -1;
	}

	printf("Printed all entries, traversed %d blocks! Press enter to remove odds...\n", blocksTraversed);
	getchar();

	// Remove odd IDs.
	for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
	{
		if ((recordIndex + 1) % 2 == 1)
		{
			uint32_t id = recordIndex + 1;

			if (HP_DeleteEntry(*heapFileHandle, (void*)&id) == -1)
			{
				printf("Could not delete heap entry with key %d!\n", recordIndex);
				return -1;
			}
		}
	}

	// Print all elements.
	blocksTraversed = HP_GetAllEntries(*heapFileHandle, nullptr);
	if (blocksTraversed == -1)
	{
		printf("Could not get heap entries!\n");
		return -1;
	}

	printf("Odds deleted! Press enter to re-add all elements...\n");
	getchar();

	// Re-add odds.
	for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
	{
		if ((recordIndex + 1) % 2 == 1)
		{
			Record record = { };
			record.ID = recordIndex + 1;
			sprintf(record.Name, "Name %d", recordIndex + 1);
			sprintf(record.Surname, "Surname %d", recordIndex + 1);
			sprintf(record.Address, "Address %d", recordIndex + 1);

			if (HP_InsertEntry(*heapFileHandle, record) == -1)
			{
				printf("Could not insert record with index %d!\n", recordIndex);
				return -1;
			}
		}
	}

	// Print all elements.
	blocksTraversed = HP_GetAllEntries(*heapFileHandle, nullptr);
	if (blocksTraversed == -1)
	{
		printf("Could not get heap entries!\n");
		return -1;
	}

	// Close the test heap file.
	if (HP_CloseFile(heapFileHandle) == -1)
	{
		printf("Could not close heap file!\n");
		return -1;
	}

	return 0;
}
#else
#include "HT.h"

#include <stdio.h>

// Entry Point
int main()
{
	// Create a test hash file.
	int32_t bucketCount = 12;
	if (HT_CreateIndex("TestHashFile", 'i', "TestHashFile", 7, bucketCount) == -1)
	{
		printf("Could not create hash file!\n");
		return -1;
	}

	// Open the test hash file.
	HT_info* hashFileHandle = HT_OpenIndex("TestHashFile");
	if (hashFileHandle == nullptr)
	{
		printf("Could not open hash file!\n");
		return -1;
	}

	printf("File created. Press enter to insert 17 elements...\n");
	getchar();

	// Insert 17 test elements.
	uint32_t recordCount = 17;
	for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
	{
		Record record = { };
		record.ID = recordIndex + 1;
		sprintf(record.Name, "Name %d", recordIndex + 1);
		sprintf(record.Surname, "Surname %d", recordIndex + 1);
		sprintf(record.Address, "Address %d", recordIndex + 1);

		if (HT_InsertEntry(*hashFileHandle, record) == -1)
		{
			printf("Could not insert record with index %d!\n", recordIndex);
			return -1;
		}
	}

	printf("Elements inserted. Press enter to print them...\n");
	getchar();

	// Print all the elements.
	int32_t blocksTraversed = HT_GetAllEntries(*hashFileHandle, nullptr);
	if (blocksTraversed == -1)
	{
		printf("Could not get hash entries!\n");
		return -1;
	}

#if 0
	printf("Printed all entries, traversed %d blocks! Press enter to remove odds...\n", blocksTraversed);
	getchar();

	// Remove odd IDs.
	for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
	{
		if ((recordIndex + 1) % 2 == 1)
		{
			uint32_t id = recordIndex + 1;

			if (HT_DeleteEntry(*hashFileHandle, (void*)&id) == -1)
			{
				printf("Could not delete hash entry with key %d!\n", recordIndex);
				return -1;
			}
		}
	}

	// Print all elements.
	blocksTraversed = HT_GetAllEntries(*hashFileHandle, nullptr);
	if (blocksTraversed == -1)
	{
		printf("Could not get hash entries!\n");
		return -1;
	}

	printf("Odds deleted! Press enter to re-add all elements...\n");
	getchar();

	// Re-add odds.
	for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
	{
		if ((recordIndex + 1) % 2 == 1)
		{
			Record record = { };
			record.ID = recordIndex + 1;
			sprintf(record.Name, "Name %d", recordIndex + 1);
			sprintf(record.Surname, "Surname %d", recordIndex + 1);
			sprintf(record.Address, "Address %d", recordIndex + 1);

			if (HT_InsertEntry(*hashFileHandle, record) == -1)
			{
				printf("Could not insert record with index %d!\n", recordIndex);
				return -1;
			}
		}
	}

	// Print all elements.
	blocksTraversed = HT_GetAllEntries(*hashFileHandle, nullptr);
	if (blocksTraversed == -1)
	{
		printf("Could not get hash entries!\n");
		return -1;
	}
#endif
	// Close the test hash file.
	if (HT_CloseIndex(hashFileHandle) == -1)
	{
		printf("Could not close hash file!\n");
		return -1;
	}

	printf("All elements restored! Press enter to see hash statistics...\n");
	getchar();

	// Calculate the hash statistics.
	if (HashStatistics("TestHashFile") == -1)
	{
		printf("Could not calculate hash statistics!\n");
		return -1;
	}

	return 0;
}
#endif
