#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "AM.h"
#include "defn.h"

int AM_errno = AME_OK;

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {      \
    BF_PrintError(code);    \
    return AME_EOF;         \
  }                         \
}

void AM_Init() {
  BF_Init(LRU);
}

int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {

  int fileDesc;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &fileDesc));

  CALL_BF(BF_AllocateBlock(fileDesc, mBlock));
  data = BF_Block_GetData(mBlock);

  memset(data, 0, BF_BLOCK_SIZE);
  memcpy(data, "B+", 2*sizeof(char));
  *(data + 2*sizeof(char)) = 1;

  *(data + 2*sizeof(char) + sizeof(int)) = 0;
  memcpy(data + 2*sizeof(char) + 2*sizeof(int), &attrType1, sizeof(char));
  memcpy(data + 3*sizeof(char) + 2*sizeof(int), &attrLength1, sizeof(int));
  memcpy(data + 3*sizeof(char) + 3*sizeof(int), &attrType2, sizeof(char));
  memcpy(data + 4*sizeof(char) + 3*sizeof(int), &attrLength2, sizeof(int));

  *(data + 4*sizeof(char) + 4*sizeof(int)) = (BF_BLOCK_SIZE - 2*sizeof(int)) / (attrLength1 + attrLength2);
  *(data + 4*sizeof(char) + 5*sizeof(int)) = (BF_BLOCK_SIZE - 2*sizeof(int)) / (sizeof(int) + attrLength1);
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_AllocateBlock(fileDesc, mBlock));
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE);
  *(data + sizeof(int)) = 2;
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_AllocateBlock(fileDesc, mBlock));
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE);
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_CloseFile(fileDesc));
  BF_Block_Destroy(&mBlock);

  return AME_OK;
}

int AM_DestroyIndex(char *fileName) {

  return AME_OK;
}

int AM_OpenIndex (char *fileName) {

  int fileDesc;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_OpenFile(fileName, &fileDesc));
  CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);

  if(strncmp(data, "B+", 2*sizeof(char))) {
    CALL_BF(BF_UnpinBlock(mBlock));
    BF_Block_Destroy(&mBlock);
    printf("WRONG CODE\n");
    return AME_EOF;
  }

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);

  return fileDesc;
}

int AM_CloseIndex (int fileDesc) {

  CALL_BF(BF_CloseFile(fileDesc));

  return AME_OK;
}

int AM_InsertEntry(int fileDesc, void *value1, void *value2) {

  int block_num, root, depth, sizeOfKey, sizeOfEntry, maxRecords, maxKeys, count = 0;
  char typeOfKey, typeOfEntry;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  root = *(data + 2*sizeof(char));
  depth = *(data + 2*sizeof(char) + sizeof(int));
  typeOfKey = *(data + 2*sizeof(char) + 2*sizeof(int));
  sizeOfKey = *(data + 3*sizeof(char) + 2*sizeof(int));
  typeOfEntry = *(data +3*sizeof(char) + 3*sizeof(int));
  sizeOfEntry = *(data + 4*sizeof(char) + 3*sizeof(int));
  maxRecords = *(data + 4*sizeof(char) + 4*sizeof(int));
  maxKeys = *(data + 4*sizeof(char) + 5*sizeof(int));
  CALL_BF(BF_UnpinBlock(mBlock));

  block_num = getDataBlock(fileDesc, root,  depth, value1, typeOfKey, sizeOfKey);
  CALL_BF(BF_GetBlock(fileDesc, block_num, mBlock));
  data = BF_Block_GetData(mBlock);

  if(*data < maxRecords) {
    insertEntryAndSort(&data, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, value1, value2);
  }
  else {
    reBalance(fileDesc, depth, root, maxKeys, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, value1);
    CALL_BF(BF_UnpinBlock(mBlock));
    block_num = getDataBlock(fileDesc, root,  depth, value1, typeOfKey, sizeOfKey);
    CALL_BF(BF_GetBlock(fileDesc, block_num, mBlock));
    data = BF_Block_GetData(mBlock);
    insertEntryAndSort(&data, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, value1, value2);
  }

  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);

  return AME_OK;
}

int reBalance(int fileDesc, int depth, int root, int maxKeys, char typeOfKey, int sizeOfKey, char typeOfEntry, int sizeOfEntry, void* Key) {

  void* newKey;
  int block_num = 0;
  char *data, *newData;
  BF_Block *mBlock, *newBlock;
  BF_Block_Init(&mBlock);
  BF_Block_Init(&newBlock);


  CALL_BF(BF_GetBlock(fileDesc, root, mBlock));
  data = BF_Block_GetData(mBlock);

  if(depth == 0) {
    CALL_BF(BF_AllocateBlock(fileDesc, newBlock));
    CALL_BF(BF_GetBlockCounter(fileDesc, &block_num));
    newData = BF_Block_GetData(newBlock);
    memset(newData, 0, BF_BLOCK_SIZE);

    if(*data < maxKeys) {

      char *oldData;
      BF_Block *oldBlock;
      BF_Block_Init(&oldBlock);

      CALL_BF(BF_GetBlock(fileDesc, getBlockNumber(data, Key, typeOfKey, sizeOfKey), oldBlock));
      oldData = BF_Block_GetData(oldBlock);

      memcpy(newData + 2*sizeof(int), oldData + 2*sizeof(int) + ((*oldData)/2)*(sizeOfEntry + sizeOfKey), (*oldData - (*oldData)/2)*(sizeOfEntry + sizeOfKey));
      *newData = *oldData - *oldData/2;
      memset(oldData + 2*sizeof(int) + ((*oldData)/2)*(sizeOfEntry + sizeOfKey), 0, *newData*(sizeOfKey + sizeOfEntry));
      *oldData = (*oldData - *newData);

      newKey = (newData + 2*sizeof(int));
      block_num--;

      for(int i=0; i < *data; i++) {
        if(compareKeys(newKey, data + (i + 2)*sizeof(int) + i*sizeOfKey, typeOfKey, sizeOfKey)) {
          for(int j=*data; j>=i; j--) {
            memcpy(data + (j + 2)*sizeof(int) + j*sizeOfKey, data + (j + 1)*sizeof(int) + (j-1)*sizeOfKey, sizeof(int) + sizeOfKey);
          }
          memcpy(data + (i+2)*sizeof(int) + i*sizeOfKey, newKey, sizeOfKey);
          memcpy(data + (i+2)*sizeof(int) + (i+1)*sizeOfKey, &block_num, sizeof(int));
          break;
        }
        else {
          if(i == *data - 1) {
            memcpy(data + (i+3)*sizeof(int) + (i+1)*sizeOfKey, newKey, sizeOfKey);
            memcpy(data + (i+3)*sizeof(int) + (i+2)*sizeOfKey, &block_num, sizeof(int));
          }
        }
      }

      if(*data == 0) {
        memcpy(data + 2*sizeof(int), newKey, sizeOfKey);
        memcpy(data + 2*sizeof(int) + sizeOfKey, &block_num, sizeof(int));
      }
      *data = *data + 1;

      BF_Block_SetDirty(newBlock);
      BF_Block_SetDirty(oldBlock);
      CALL_BF(BF_UnpinBlock(oldBlock));
      BF_Block_Destroy(&oldBlock);
    }
    else {

      void* rootKey;
      char *rootData;
      BF_Block *rootBlock;
      BF_Block_Init(&rootBlock);

      printIndexblock(fileDesc, 1);

      memcpy(newData + sizeof(int), data + (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2 + 1)*sizeOfKey, (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2 + 1)*sizeOfKey);
      *newData = maxKeys - maxKeys/2 - 1;
      rootKey = (data + (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2)*sizeOfKey);
      memset(data + (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2)*sizeOfKey, 0, (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2 + 1)*sizeOfKey);
      *data = *data - *newData - 1;
      BF_Block_SetDirty(newBlock);
      BF_Block_SetDirty(mBlock);

      CALL_BF(BF_UnpinBlock(mBlock));
      CALL_BF(BF_UnpinBlock(newBlock));

      printIndexblock(fileDesc, 1);
      printIndexblock(fileDesc, block_num-1);

      BF_Block_Destroy(&rootBlock);

      return 0;
    }

    CALL_BF(BF_UnpinBlock(mBlock));
    CALL_BF(BF_UnpinBlock(newBlock));
    BF_Block_Destroy(&mBlock);
    BF_Block_Destroy(&newBlock);

    return 0;
  }

  block_num = reBalance(fileDesc, depth-1, getBlockNumber(data, Key, typeOfKey, sizeOfKey), maxKeys, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, Key);
  if(!block_num) {

  }

  CALL_BF(BF_UnpinBlock(mBlock));

  BF_Block_Destroy(&mBlock);
  BF_Block_Destroy(&newBlock);
}

void insertEntryAndSort(char** data, char typeOfKey, int sizeOfKey, char typeOfEntry, int sizeOfEntry, void* value1, void* value2) {

  void* currKey;

  for(int i=0; i < **data; i++) {
    if(typeOfKey == STRING) {
      currKey = (char*)(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry));
      if(strcmp((char*)value1, (char*)currKey) < 0) {
        for(int j=**data; j >= i; j--) {
            memcpy(*data + 2*sizeof(int) + j*(sizeOfKey + sizeOfEntry), *data + 2*sizeof(int) + (j-1)*(sizeOfKey + sizeOfEntry), sizeOfKey+sizeOfEntry);
        }
        memcpy(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry), (char*)value1, sizeOfKey);
        memcpy(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey, value2, sizeOfEntry);
        **data = **data + 1;
        return;
      }
    }
    else if(typeOfKey == FLOAT) {
      currKey = (*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry));
      if(*(float*)value1 < *(float*)currKey) {
        for(int j=**data; j >= i; j--) {
            memcpy(*data + 2*sizeof(int) + j*(sizeOfKey + sizeOfEntry), *data + 2*sizeof(int) + (j-1)*(sizeOfKey + sizeOfEntry), sizeOfKey+sizeOfEntry);
        }
        memcpy(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry), (float*)value1, sizeOfKey);
        memcpy(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey, value2, sizeOfEntry);
        **data = **data + 1;
        return;
      }
    }
    else {
      currKey = (*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry));
      if(*(int*)value1 < *(int*)currKey) {
        for(int j=**data; j >= i; j--) {
            memcpy(*data + 2*sizeof(int) + j*(sizeOfKey + sizeOfEntry), *data + 2*sizeof(int) + (j-1)*(sizeOfKey + sizeOfEntry), sizeOfKey+sizeOfEntry);
        }
        memcpy(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry), (int*)value1, sizeOfKey);
        memcpy(*data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey, value2, sizeOfEntry);
        **data = **data + 1;
          return;
      }
    }
  }
  memcpy(*data + 2*sizeof(int) + (**data)*(sizeOfKey + sizeOfEntry), value1, sizeOfKey);
  memcpy(*data + 2*sizeof(int) + (**data)*(sizeOfKey + sizeOfEntry) + sizeOfKey, value2, sizeOfEntry);
  **data = **data + 1;
}

int getDataBlock(int fileDesc, int root, int depth, void* key, char typeOfKey, int sizeOfKey) {

  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  for(int i=0; i<= depth; i++) {
    CALL_BF(BF_GetBlock(fileDesc, root, mBlock));
    data = BF_Block_GetData(mBlock);
    root = getBlockNumber(data, key, typeOfKey, sizeOfKey);
    CALL_BF(BF_UnpinBlock(mBlock));
  }

  BF_Block_Destroy(&mBlock);

  return root;
}

int getBlockNumber(char* data, void* key, char typeOfKey, int sizeOfKey) {

  void* tmpKey;
  int block_num;

  for(int i=0; i<=*data; i++) {
    block_num = *(data + (i + 1)*sizeof(int) + i*sizeOfKey);
    if(*data) {
      if(i == *data) {
        break;
      }
      tmpKey = (data + (i + 2)*sizeof(int) + i*sizeOfKey);
      if(compareKeys(key, tmpKey, typeOfKey, sizeOfKey)) {
        break;
      }
    }
  }

  return block_num;
}

int compareKeys(void* key, void* mKey, char typeOfKey, int sizeOfKey) {

  if(typeOfKey == STRING) {
    if(strcmp((char*)key, (char*)mKey) < 0) {
      return 1;
    }
    return 0;
  }
  else if(typeOfKey == FLOAT) {
    if(*(float*)key < *(float*)mKey) {
      return 1;
    }
    return 0;
  }
  else {
    if(*(int*)key < *(int*)mKey) {
      return 1;
    }
    return 0;
  }
}

int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  return AME_OK;
}


void *AM_FindNextEntry(int scanDesc) {
	
}


int AM_CloseIndexScan(int scanDesc) {
  return AME_OK;
}


void AM_PrintError(char *errString) {
  return;
}

void AM_Close() {
  BF_Close();
  return;
}

int printIndexblock(int fileDesc, int block_num) {
  int sizeOfKey;
  char typeOfKey;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  typeOfKey = *(data + 2*sizeof(char) + 2*sizeof(int));
  sizeOfKey = *(data + 3*sizeof(char) + 2*sizeof(int));
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_GetBlock(fileDesc, block_num, mBlock));
  data = BF_Block_GetData(mBlock);

  printf("INDEX: %d\n", *(data + sizeof(int)));
  for(int i=0; i < *data; i++) {
    if(typeOfKey == STRING) {
      printf("KEY: %s\n", (char*)(data + (i + 2)*sizeof(int) + i*sizeOfKey));
    }
    else if (typeOfKey == FLOAT) {
      printf("KEY: %f\n", *(float*)(data + (i + 2)*sizeof(int) + i*sizeOfKey));
    }
    else {
      printf("KEY: %d\n", *(int*)(data + (i + 2)*sizeof(int) + (i)*sizeOfKey));
    }
    printf("INDEX: %d\n", *(data + (i + 2)*sizeof(int) + (i+1)*sizeOfKey));
  }
  printf("\n");

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
}

int printBlock(int fileDesc, int block_num) {

  int sizeOfKey, sizeOfEntry;
  char typeOfKey, typeOfEntry;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  typeOfKey = *(data + 2*sizeof(char) + 2*sizeof(int));
  sizeOfKey = *(data + 3*sizeof(char) + 2*sizeof(int));
  typeOfEntry = *(data +3*sizeof(char) + 3*sizeof(int));
  sizeOfEntry = *(data + 4*sizeof(char) + 3*sizeof(int));
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_GetBlock(fileDesc, block_num, mBlock));
  data = BF_Block_GetData(mBlock);

  for(int i=0; i < *data; i++) {
    if(typeOfKey == STRING) {
      printf("KEY: %s\n", (char*)(data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry)));
    }
    else if (typeOfKey == FLOAT) {
      printf("KEY: %f\n", *(float*)(data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry)));
    }
    else {
      printf("KEY: %d\n", *(int*)(data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry)));
    }
    if(typeOfEntry == STRING) {
      printf("ENTRY: %s\n", (char*)(data +  + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey));
    }
    else if (typeOfEntry == FLOAT) {
      printf("ENTRY: %f\n", *(float*)(data +  + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey));
    }
    else {
      printf("ENTRY: %d\n", *(int*)(data +  + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey));
    }
    
  }
  printf("\n");

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);
}