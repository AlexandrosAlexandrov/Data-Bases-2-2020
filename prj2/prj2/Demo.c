#include <stdio.h>

#include "SHT.h"

#include "BF/BF.h"

// This function showcases the secondary hash file.
static int32_t DemoSHT(int32_t primaryBucketCount, int32_t secondaryBucketCount, int32_t recordCount)
{
	printf("==================================\n");
	printf("==== SECONDARY HASH FILE DEMO ====\n");
	printf("==================================\n");
	printf("\n");

	HT_info* primaryHashFileHandle = nullptr;

	// Hash File Creation and Opening
	{
		// Create a test hash file.
		if (HT_CreateIndex("TestPrimaryHashFile", 'i', "TestPrimaryHashFile", 7, primaryBucketCount) == -1)
		{
			printf("Could not create primary hash file!\n");
			return -1;
		}

		// Open the test hash file.
		primaryHashFileHandle = HT_OpenIndex("TestPrimaryHashFile");
		if (primaryHashFileHandle == nullptr)
		{
			printf("Could not open primary hash file!\n");
			return -1;
		}

		printf("Created primary hash file! Press enter to insert some elements...\n");
	}

	getchar();
	printf("\n");

	// Hash File Insertion
	{
		// Iterate through all the records, generate them and insert them.
		for (uint32_t recordIndex = 0; recordIndex < recordCount; recordIndex++)
		{
			Record record = { };
			record.ID = recordIndex;
			sprintf(record.Name, "Name%d", recordIndex);
			sprintf(record.Surname, "Surname%d", recordIndex);
			sprintf(record.Address, "Address%d", recordIndex);

			if (HT_InsertEntry(*primaryHashFileHandle, record) == -1)
			{
				printf("Could not insert record with index %d to the primary hash file!\n", recordIndex);
				return -1;
			}
		}

		printf("Inserted %d elements into the primary hash file! Press enter to print them...\n", recordCount);
	}

	getchar();
	printf("\n");

	// Printing of all the elements in the hash file.
	{
		// Print all the elements.
		int32_t blocksTraversed = HT_GetAllEntries(*primaryHashFileHandle, nullptr);
		if (blocksTraversed == -1)
		{
			printf("Could not get hash entries!\n");
			return -1;
		}

		// Print the number of blocks traversed.
		printf("\n");
		printf("Traversed %d blocks!\n", blocksTraversed);
		printf("\n");

		printf("Press enter to create the secondary hash file...\n");
	}

	getchar();
	printf("\n");

	// Creating the secondary hash file.

	SHT_info* secondaryHashFileHandle = nullptr;

	// Hash File Creation and Opening
	{
		// Create a test secondary hash file.
		if (SHT_CreateSecondaryIndex("TestSecondaryHashFile", 'c', "TestSecondaryHashFile", 25, secondaryBucketCount, "TestPrimaryHashFile") == -1)
		{
			printf("Could not create secondary hash file!\n");
			return -1;
		}

		// Open the test hash file.
		secondaryHashFileHandle = SHT_OpenSecondaryIndex("TestSecondaryHashFile");
		if (secondaryHashFileHandle == nullptr)
		{
			printf("Could not open secondary hash file!\n");
			return -1;
		}

		printf("Created secondary hash file! Press enter to insert some elements...\n");
	}

	printf("\n");
	getchar();

	// Secondary Hash File Insertion
	{
		// Iterate through all the records, generate them and insert them.
		for (uint32_t recordIndex = recordCount; recordIndex < recordCount + 10; recordIndex++)
		{
			Record record = { };
			record.ID = recordIndex;
			sprintf(record.Name, "Name%d", recordIndex);
			sprintf(record.Surname, "Surname%d", recordIndex);
			sprintf(record.Address, "Address%d", recordIndex);

			int32_t blockID = HT_InsertEntry(*primaryHashFileHandle, record);
			if (blockID == -1)
			{
				printf("Could not insert record with index %d to the primary hash!\n", recordIndex);
				return -1;
			}

			SecondaryRecord secondaryRecord = { };
			secondaryRecord.Record = record;
			secondaryRecord.BlockID = blockID;

			if (SHT_SecondaryInsertEntry(*secondaryHashFileHandle, secondaryRecord) == -1)
			{
				printf("Could not insert record with index %d to the secondary hash!\n", recordIndex);
				return -1;
			}
		}

		printf("Inserted %d elements into the secondary hash file! Press enter to look up one of them by ID...\n", 10);
	}

	printf("\n");
	getchar();

	// Look up and print of a specific element.
	{
		printf("Give an element ID to look-up: ");

		int32_t idToLookUp = 0;
		scanf("%d", &idToLookUp);
		while (getchar() == 0);

		printf("\n");

		// Look up the element.
		int32_t blocksTraversed = HT_GetAllEntries(*primaryHashFileHandle, (void*)&idToLookUp);
		if (blocksTraversed != -1)
		{
			// Print the number of blocks traversed.
			printf("\n");
			printf("Traversed %d blocks!\n", blocksTraversed);
			printf("\n");
		}

		printf("Searched for element with ID %d! Press enter to look up one of them by surname...\n", idToLookUp);
	}

	printf("\n");
	getchar();

	// Look up and print of a specific element.
	{
		char surname[25];
		sprintf(surname, "Surname%d", recordCount + 2);

		// Look up the element.
		int32_t blocksTraversed = SHT_SecondaryGetAllEntries(*secondaryHashFileHandle, *primaryHashFileHandle, surname);
		if (blocksTraversed != -1)
		{
			// Print the number of blocks traversed.
			printf("\n");
			printf("Traversed %d blocks!\n", blocksTraversed);
			printf("\n");
		}

		printf("Searched for element with surname %s! Press enter to calculate the hash statistics for hte secondary hash file...\n", surname);
	}

	printf("\n");
	getchar();

	// Evaluation of the hash function.
	{
		// Calculate the hash statistics.
		if (HashStatistics("TestSecondaryHashFile") == -1)
		{
			printf("Could not calculate hash statistics!\n");
			return -1;
		}

		printf("\n");
		printf("Press enter to close the hash files...\n");
	}

	printf("\n");
	getchar();

	// Hash File Closure
	{
		// Close the test hash file.
		if (HT_CloseIndex(primaryHashFileHandle) == -1)
		{
			printf("Could not close primary hash file!\n");
			return -1;
		}

		// Close the test secondary hash file.
		if (SHT_CloseSecondaryIndex(secondaryHashFileHandle) == -1)
		{
			printf("Could not close secondary hash file!\n");
			return -1;
		}

		printf("Both hash files have been closed! This was the end of the SHT demo.\n");
	}

	return 0;
}

// Entry point.
int32_t main()
{
	// Initialize the block level.
	BF_Init();

	// Demo the heap file.
	printf("\n");
	printf("Press enter to start the SHT demo...\n");
	printf("\n");
	getchar();

	int32_t primaryHashBucketCount = 20;
	int32_t secondaryHashBucketCount = 25;
	int32_t hashRecordCount = 150;

	if (DemoSHT(primaryHashBucketCount, secondaryHashBucketCount, hashRecordCount) == -1)
		return -1;

	return 0;
}
