#include <stdio.h>
#include <stdlib.h>
#include "bf.h"
#include "AM.h"

int AM_errno = AME_OK;

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

  CALL_BF(BF_AllocateBlock(fileName, mBlock));
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE); // optional
  memcpy(data, "B+", 2*sizeof(char));
  memcpy(data + 2*sizeof(char), &joker, sizeof(int));
  memcpy(data + 2*sizeof(char) + sizeof(int), &attrType1, sizeof(char));
  memcpy(data + 3*sizeof(char) + sizeof(int), &attrLength1, sizeof(int));
  memcpy(data + 3*sizeof(char) + 2*sizeof(int), &attrType2, sizeof(char));
  memcpy(data + 4*sizeof(char) + 2*sizeof(int), &attrLength2, sizeof(int));
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_AllocateBlock(fileName, mBlock));
  data = BF_Block_GetData(mBlock);
  joker = 2;
  memcpy(data, &joker, sizeof(int));
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  CALL_BF(BF_AllocateBlock(fileName, mBlock));
  data = BF_Block_GetData(mBlock);
  memset(data, 0, BF_BLOCK_SIZE); // optional
  BF_Block_SetDirty(mBlock);
  CALL_BF(BF_UnpinBlock(mBlock));

  BF_Block_Destroy(&mBlock);

  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
  return AME_OK;
}


int AM_OpenIndex (char *fileName) {
  return AME_OK;
}


int AM_CloseIndex (int fileDesc) {
  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  return AME_OK;
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
