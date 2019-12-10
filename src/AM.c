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
	return;
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
  joker = 0;
  memset(data, 0, BF_BLOCK_SIZE); // optional
  memcpy(data, "B+", 2*sizeof(char));
  memcpy(data + 2*sizeof(char), &joker, sizeof(int)); // blockNumber of root bock
  memcpy(data + 2*sizeof(char) + sizeof(int), &joker, sizeof(int)); //storing the depth of the tree
  memcpy(data + 2*sizeof(char) + 2*sizeof(int), &attrType1, sizeof(char));
  memcpy(data + 3*sizeof(char) + 2*sizeof(int), &attrLength1, sizeof(int));
  memcpy(data + 3*sizeof(char) + 3*sizeof(int), &attrType2, sizeof(char));
  memcpy(data + 4*sizeof(char) + 3*sizeof(int), &attrLength2, sizeof(int));
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
  char *data, *code;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_OpenFile(fileName, &fileDesc));
  BF_GetBlock(fileDesc, 0, mBlock);
  data = BF_Block_GetData(mBlock);
  memcpy(code, data, 2*sizeof(char));
  if(strcmp(code, "B+")) {
    CALL_BF(BF_UnpinBlock(mBlock));
    BF_Block_Destroy(&mBlock);
    return AME_EOF;
  }
  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);

  return AME_OK;
}


int AM_CloseIndex (int fileDesc) {

  CALL_BF(BF_CloseFile(fileDesc));

  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {

  int block_num, root;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);
  // root* *(data + 2*sizeof(char));
  CALL_BF(BF_UnpinBlock(mBlock));

  // CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  // data = BF_Block_GetData(mBlock);



  BF_Block_Destroy(&mBlock);

  return AME_OK;
}

int getDataBlock(int fileDesc, int block_num, int depth, void* key) {

  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  for(int i=0; i<= depth; i++) {
    CALL_BF(BF_GetBlock(fileDesc, block_num, mBlock));
    data = BF_Block_GetData(mBlock);
    block_num = getBlockNumber(data, key);
  }

  BF_Block_Destroy(&mBlock);

  return block_num;
}

int getBlockNumber(char* data, void* key) {

  int block_num, joker = *(data);
  BF_Block* mBlock;
  BF_Block_Init(&mBlock);

  for(int i=0; i<=joker; i++) {
    
    block_num = *(data + i*sizeof(key) + i*sizeof(int));
  }

  BF_Block_Init(&mBlock);
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
  
}

void AM_Close() {
  
}