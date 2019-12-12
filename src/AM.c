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

  int fileDesc, joker = 1;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_CreateFile(fileName));
  CALL_BF(BF_OpenFile(fileName, &fileDesc));

  CALL_BF(BF_AllocateBlock(fileDesc, mBlock));
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE); // optional
  memcpy(data, "B+", 2*sizeof(char));
  memcpy(data + 2*sizeof(char), &joker, sizeof(int)); // blockNumber of the root block
  joker = 0;
  memcpy(data + 2*sizeof(char) + sizeof(int), &joker, sizeof(int)); //storing the depth of the tree
  memcpy(data + 2*sizeof(char) + 2*sizeof(int), &attrType1, sizeof(char));
  memcpy(data + 3*sizeof(char) + 2*sizeof(int), &attrLength1, sizeof(int));
  memcpy(data + 3*sizeof(char) + 3*sizeof(int), &attrType2, sizeof(char));
  memcpy(data + 4*sizeof(char) + 3*sizeof(int), &attrLength2, sizeof(int));
  joker = (BF_BLOCK_SIZE - 2*sizeof(int)) / (attrLength1 + attrLength2);
  // printf("MAXRECORDS: %d\n", joker);
  memcpy(data + 4*sizeof(char) + 4*sizeof(int), &joker, sizeof(int));
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_AllocateBlock(fileDesc, mBlock));
  data = BF_Block_GetData(mBlock);
  joker = 2;
  memset(data, 0, BF_BLOCK_SIZE);
  memcpy(data + sizeof(int), &joker, sizeof(int));
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_AllocateBlock(fileDesc, mBlock));
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE); // optional
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

  int block_num, root, depth, sizeOfKey, sizeOfEntry, maxRecords, joker;
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
  CALL_BF(BF_UnpinBlock(mBlock));

  // printf("DEPTH: %d\n", depth);
  // printf("ROOT: %d\n", root);
  block_num = getDataBlock(fileDesc, root,  depth, value1, typeOfKey, sizeOfKey);
  // printf("BLOCK_NUM: %d\n", block_num);

  CALL_BF(BF_GetBlock(fileDesc, block_num, mBlock));
  data = BF_Block_GetData(mBlock);
  joker = *data;

  if(joker < maxRecords) {    // <-- EPILOGI DALIANI TO <, BATMOLOGISTE ALALOGOS
    memcpy(data + 2*sizeof(int) + joker*(sizeOfKey + sizeOfEntry), (int*)value1, sizeOfKey);
    memcpy(data + 2*sizeof(int) + joker*(sizeOfKey + sizeOfEntry) + sizeOfKey, (char*)value2, sizeOfEntry);
    joker = *(data) + 1;
    memcpy(data, &joker, sizeof(int));
  }
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  BF_Block_Destroy(&mBlock);

  // printf("In Insert, block_num: %d\n", block_num);
  printBlock2(fileDesc);
  return AME_OK;
}

int printBlock2(int fileDesc) {

  int sizeOfKey, sizeOfEntry, joker;
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

  CALL_BF(BF_GetBlock(fileDesc, 2, mBlock));
  data = BF_Block_GetData(mBlock);
  joker = *data;

  for(int i=0; i < joker; i++) {
    printf("KEY: %d\n", *(int*)(data + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry)));
    printf("ENTRY: %s\n", (char*)(data +  + 2*sizeof(int) + i*(sizeOfKey + sizeOfEntry) + sizeOfKey));
  }
  printf("\n");
  CALL_BF(BF_UnpinBlock(mBlock));

  BF_Block_Destroy(&mBlock);
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
  int block_num, joker;

  joker = *data;
  for(int i=0; i<=joker; i++) {
    block_num = *(data + (i + 1)*sizeof(int) + i*sizeOfKey);
    // if(joker) {
    //   tmpKey = (data + (i + 2)*(sizeof(int) + i*sizeOfKey));   //dont know if its right
    //   if(compareKeys(key, tmpKey, typeOfKey, sizeOfKey)) {
    //     break;
    //   }
    // }
  }

  return block_num;
}

int compareKeys(void* key, void* mKey, char typeOfKey, int sizeOfKey) {

  if(typeOfKey == STRING) {
    printf("%s", (char*)mKey);
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