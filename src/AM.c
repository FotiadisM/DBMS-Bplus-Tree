#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "AM.h"
#include "defn.h"

#define MAXOPENFILES 20

int AM_errno = AME_OK;

int opnFilesCounter = 0, scnFilescounter = 0;
opnFileInfoPtr* opnArray = NULL;
scnFileInfoPtr* scnArray = NULL;

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

  opnArray = malloc(MAXOPENFILES*sizeof(opnFileInfoPtr));
  scnArray = malloc(MAXOPENFILES*sizeof(scnFileInfoPtr));

  for(int i=0; i < MAXOPENFILES; i++) {
    opnArray[i] = NULL;
    scnArray[i] = NULL;
  }
}

int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {

  int fileDesc;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  if(attrType1 == INTEGER) {
    if(sizeof(int) != attrLength1) {
      BF_Block_Destroy(&mBlock);
      return AME_EOF;
    }
  }
  if(attrType1 == FLOAT) {
    if(sizeof(float) != attrLength1) {
      BF_Block_Destroy(&mBlock);
      return AME_EOF;
    }
  }
  if(attrType2 == INTEGER) {
    if(sizeof(int) != attrLength2) {
      BF_Block_Destroy(&mBlock);
      return AME_EOF;
    }
  }
  if(attrType2 == FLOAT) {
    if(sizeof(float) != attrLength2) {
      BF_Block_Destroy(&mBlock);
      return AME_EOF;
    }
  }

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

  remove(fileName);

  return AME_OK;
}

int AM_OpenIndex (char *fileName) {

  int fileDesc;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  if(opnFilesCounter ==  MAXOPENFILES) {
    return AME_EOF;
  }

  CALL_BF(BF_OpenFile(fileName, &fileDesc));
  CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
  data = BF_Block_GetData(mBlock);

  if(strncmp(data, "B+", 2*sizeof(char))) {
    CALL_BF(BF_UnpinBlock(mBlock));
    BF_Block_Destroy(&mBlock);
    printf("WRONG CODE\n");
    return AME_EOF;
  }

  opnArray[fileDesc] = malloc(sizeof(opnFileInfo));
  opnArray[fileDesc]->fileDesc = fileDesc;
  opnArray[fileDesc]->root = 1;
  opnArray[fileDesc]->depth = 0;
  opnArray[fileDesc]->maxEntries = *(data + 4*sizeof(char) + 4*sizeof(int));
  opnArray[fileDesc]->maxKeys = *(data + 4*sizeof(char) + 5*sizeof(int));
  opnArray[fileDesc]->typeOfKey = *(data + 2*sizeof(char) + 2*sizeof(int));
  opnArray[fileDesc]->sizeOfKey = *(data + 3*sizeof(char) + 2*sizeof(int));
  opnArray[fileDesc]->typeOfEntry = *(data +3*sizeof(char) + 3*sizeof(int));
  opnArray[fileDesc]->sizeOfEntry = *(data + 4*sizeof(char) + 3*sizeof(int));
  opnFilesCounter++;

  CALL_BF(BF_UnpinBlock(mBlock));
  BF_Block_Destroy(&mBlock);

  return fileDesc;
}

int AM_CloseIndex (int fileDesc) {

  if(opnArray[fileDesc] != NULL) {
    free(opnArray[fileDesc]);
    opnFilesCounter--;
    CALL_BF(BF_CloseFile(fileDesc));
    return AME_OK;
  }

  return AME_EOF;
}

int AM_InsertEntry(int fileDesc, void *value1, void *value2) {

  int block_num, root, depth, sizeOfKey, sizeOfEntry, maxRecords, maxKeys, count = 0;
  char typeOfKey, typeOfEntry;
  char *data;
  rblncInfo returnInfo;
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
    returnInfo = reBalance(fileDesc, depth, depth, root, maxKeys, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, value1);
    if(returnInfo.block_num) {
      root = returnInfo.block_num;
      CALL_BF(BF_UnpinBlock(mBlock));
      CALL_BF(BF_GetBlock(fileDesc, 0, mBlock));
      data = BF_Block_GetData(mBlock);
      *(data + 2*sizeof(char)) = root;
      opnArray[fileDesc]->root = root;
      CALL_BF(BF_UnpinBlock(mBlock));
    }
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

rblncInfo reBalance(int fileDesc, int depth, int initialDepth, int root, int maxKeys, char typeOfKey, int sizeOfKey, char typeOfEntry, int sizeOfEntry, void* Key) {

  void* newKey;
  int block_num = 0;
  char *oldData, *newData;
  rblncInfo returnInfo;
  BF_Block *oldBlock, *newBlock;
  BF_Block_Init(&oldBlock);
  BF_Block_Init(&newBlock);


  BF_GetBlock(fileDesc, root, oldBlock);
  oldData = BF_Block_GetData(oldBlock);

  if(depth == -1) {
    BF_AllocateBlock(fileDesc, newBlock);
    BF_GetBlockCounter(fileDesc, &block_num);
    block_num--;
    newData = BF_Block_GetData(newBlock);
    memset(newData, 0, BF_BLOCK_SIZE);

    memcpy(newData + 2*sizeof(int), oldData + 2*sizeof(int) + ((*oldData)/2)*(sizeOfEntry + sizeOfKey), (*oldData - (*oldData)/2)*(sizeOfEntry + sizeOfKey));
    *newData = *oldData - *oldData/2;
    memset(oldData + 2*sizeof(int) + ((*oldData)/2)*(sizeOfEntry + sizeOfKey), 0, *newData*(sizeOfKey + sizeOfEntry));
    *oldData = (*oldData - *newData);

    *(newData + sizeof(int)) = *(oldData + sizeof(int));
    *(oldData + sizeof(int)) = block_num;
    
    returnInfo.newKey = (newData + 2*sizeof(int));
    returnInfo.block_num = block_num;

    BF_Block_SetDirty(newBlock);
    BF_Block_SetDirty(oldBlock);
    BF_UnpinBlock(oldBlock);
    BF_UnpinBlock(newBlock);
    BF_Block_Destroy(&oldBlock);
    BF_Block_Destroy(&newBlock);

    return returnInfo;
  }

  returnInfo = reBalance(fileDesc, depth-1, initialDepth, getBlockNumber(oldData, Key, typeOfKey, sizeOfKey), maxKeys, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, Key);
  if(returnInfo.block_num != 0) {

    newKey = returnInfo.newKey;
    block_num = returnInfo.block_num;
    if(*oldData < maxKeys) { //if there is space in the block

      for(int i=0; i < *oldData; i++) {   //if key's position >1  < last position
        if(compareKeys(newKey, oldData + (i + 2)*sizeof(int) + i*sizeOfKey, typeOfKey, sizeOfKey)) {
          for(int j=*oldData; j>=i; j--) {
            memcpy(oldData + (j + 2)*sizeof(int) + j*sizeOfKey, oldData + (j + 1)*sizeof(int) + (j-1)*sizeOfKey, sizeof(int) + sizeOfKey);
          }
          memcpy(oldData + (i+2)*sizeof(int) + i*sizeOfKey, newKey, sizeOfKey);
          memcpy(oldData + (i+2)*sizeof(int) + (i+1)*sizeOfKey, &block_num, sizeof(int));
          break;
        }
        else {      //if key's position last
          if(i == *oldData - 1) {
            memcpy(oldData + (i+3)*sizeof(int) + (i+1)*sizeOfKey, newKey, sizeOfKey);
            memcpy(oldData + (i+3)*sizeof(int) + (i+2)*sizeOfKey, &block_num, sizeof(int));
          }
        }
      }

      if(*oldData == 0) {   //if key's position first
        memcpy(oldData + 2*sizeof(int), newKey, sizeOfKey);
        memcpy(oldData + 2*sizeof(int) + sizeOfKey, &block_num, sizeof(int));
      }
      *oldData = *oldData + 1;

      returnInfo.block_num = 0;
      returnInfo.newKey = NULL;

      BF_Block_SetDirty(oldBlock);
      BF_UnpinBlock(oldBlock);
      BF_Block_Destroy(&oldBlock);
      BF_Block_Destroy(&newBlock);

      return returnInfo;
    }
    else {        //if key cant fit in the block
      BF_AllocateBlock(fileDesc, newBlock);
      BF_GetBlockCounter(fileDesc, &block_num);
      block_num--;
      newData = BF_Block_GetData(newBlock);
      memset(newData, 0, BF_BLOCK_SIZE);

      memcpy(newData + sizeof(int), oldData + (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2 + 1)*sizeOfKey, (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2 + 1)*sizeOfKey);
      *newData = maxKeys - maxKeys/2 - 1;

      newKey = returnInfo.newKey;
      memcpy(returnInfo.newKey, oldData + (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2)*sizeOfKey, sizeOfKey);

      memset(oldData + (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2)*sizeOfKey, 0, (maxKeys/2 + 2)*sizeof(int) + (maxKeys/2 + 1)*sizeOfKey);
      *oldData = *oldData - *newData - 1;

      if(compareKeys(newKey, oldData + (maxKeys/2 + 1)*sizeof(int) + (maxKeys/2 -1)*sizeOfKey, typeOfKey, sizeOfKey)) {
        // key should be inserted in the old block
        for(int i=0; i < *oldData; i++) {
          if(compareKeys(newKey, oldData + (i + 2)*sizeof(int) + i*sizeOfKey, typeOfKey, sizeOfKey)) {
            for(int j=*oldData; j>=i; j--) {
              memcpy(oldData + (j + 2)*sizeof(int) + j*sizeOfKey, oldData + (j + 1)*sizeof(int) + (j-1)*sizeOfKey, sizeof(int) + sizeOfKey);
            }
            memcpy(oldData + (i+2)*sizeof(int) + i*sizeOfKey, newKey, sizeOfKey);
            memcpy(oldData + (i+2)*sizeof(int) + (i+1)*sizeOfKey, &returnInfo.block_num, sizeof(int));
            break;
          }
          else {      //if key's position last
            if(i == *oldData - 1) {
              memcpy(oldData + (i+3)*sizeof(int) + (i+1)*sizeOfKey, newKey, sizeOfKey);
              memcpy(oldData + (i+3)*sizeof(int) + (i+2)*sizeOfKey, &returnInfo.block_num, sizeof(int));
            }
          }
        }

        if(*oldData == 0) {   //if key's position first
          memcpy(oldData + 2*sizeof(int), newKey, sizeOfKey);
          memcpy(oldData + 2*sizeof(int) + sizeOfKey, &returnInfo.block_num, sizeof(int));
        }
        *oldData = *oldData + 1;
      }
      else {
        for(int i=0; i < *newData; i++) {   //if key's position >1  < last position
          if(compareKeys(newKey, newData + (i + 2)*sizeof(int) + i*sizeOfKey, typeOfKey, sizeOfKey)) {
            for(int j=*newData; j>=i; j--) {
              memcpy(newData + (j + 2)*sizeof(int) + j*sizeOfKey, newData + (j + 1)*sizeof(int) + (j-1)*sizeOfKey, sizeof(int) + sizeOfKey);
            }
            memcpy(newData + (i+2)*sizeof(int) + i*sizeOfKey, newKey, sizeOfKey);
            memcpy(newData + (i+2)*sizeof(int) + (i+1)*sizeOfKey, &returnInfo.block_num, sizeof(int));
            break;
          }
          else {      //if key's position last
            if(i == *newData - 1) {
              memcpy(newData + (i+3)*sizeof(int) + (i+1)*sizeOfKey, newKey, sizeOfKey);
              memcpy(newData + (i+3)*sizeof(int) + (i+2)*sizeOfKey, &returnInfo.block_num, sizeof(int));
            }
          }
        }

        if(*newData == 0) {   //if key's position first
          memcpy(newData + 2*sizeof(int), newKey, sizeOfKey);
          memcpy(newData + 2*sizeof(int) + sizeOfKey, &returnInfo.block_num, sizeof(int));
        }
        *newData = *newData + 1;
      }

      returnInfo.block_num = block_num;

      BF_Block_SetDirty(oldBlock);
      BF_Block_SetDirty(newBlock);
      BF_UnpinBlock(oldBlock);
      BF_UnpinBlock(newBlock);

      if(depth == initialDepth) {
        BF_AllocateBlock(fileDesc, newBlock);
        BF_GetBlockCounter(fileDesc, &block_num);
        block_num--;
        newData = BF_Block_GetData(newBlock);
        memset(newData, 0, BF_BLOCK_SIZE);

        *(newData) = 1;
        *(newData + sizeof(int)) = root;
        // printf("NEW KEY %s\n", (char*)returnInfo.newKey);
        memcpy(newData + 2*sizeof(int), returnInfo.newKey, sizeOfKey);
        *(newData + 2*sizeof(int) + sizeOfKey) = returnInfo.block_num;

        returnInfo.block_num = block_num;
        returnInfo.newKey = NULL;
        
        BF_Block_SetDirty(newBlock);
        BF_UnpinBlock(newBlock);
        BF_Block_Destroy(&oldBlock);
        BF_Block_Destroy(&newBlock);

        return returnInfo;
      }

      BF_Block_Destroy(&oldBlock);
      BF_Block_Destroy(&newBlock);

      return returnInfo;
    }
  }
  else {    //no need to rebalance upper levels
    returnInfo.block_num = 0;
    returnInfo.newKey = NULL;
    
    BF_UnpinBlock(oldBlock);
    BF_Block_Destroy(&oldBlock);
    BF_Block_Destroy(&newBlock);

    return returnInfo;
  }
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

  int root, depth, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, block_num;
  char *data;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  root = opnArray[fileDesc]->root;
  depth = opnArray[fileDesc]->depth;
  typeOfKey = opnArray[fileDesc]->typeOfKey;
  sizeOfKey = opnArray[fileDesc]->sizeOfKey;
  typeOfEntry = opnArray[fileDesc]->typeOfEntry;
  sizeOfEntry = opnArray[fileDesc]->sizeOfEntry;

  if(scnFilescounter == MAXOPENFILES) {
    return AME_EOF;
  }

  if(opnArray[fileDesc] == NULL) {
    return AME_EOF;
  }

  scnArray[fileDesc] = malloc(sizeof(scnFileInfo));
  scnArray[fileDesc]->scanDesc = fileDesc;
  scnArray[fileDesc]->value = value;
  scnArray[fileDesc]->op = op;
  scnArray[fileDesc]->printedCounter = 0;

  block_num = getDataBlock(fileDesc, root, depth, value, typeOfKey, sizeOfKey);
  BF_GetBlock(fileDesc, block_num, mBlock);
  data = BF_Block_GetData(mBlock);

  if(op == EQUAL) {
    scnArray[fileDesc]->starting_block = block_num;
    scnArray[fileDesc]->ending_block = *(data + sizeof(int));
  }
  else if(op == NOT_EQUAL) {
    scnArray[fileDesc]->starting_block = 2;
    scnArray[fileDesc]->ending_block = 0;
  }
  else if(op==LESS_THAN || op==LESS_THAN_OR_EQUAL) {
    scnArray[fileDesc]->starting_block = 2;
    scnArray[fileDesc]->ending_block = *(data + sizeof(int));;
  }
  else {
    scnArray[fileDesc]->starting_block = block_num;
    scnArray[fileDesc]->ending_block = 0;
  }

  return fileDesc;
}


void* AM_FindNextEntry(int scanDesc) {

  void *value, *tmpKey, *tmpEntry;
  int root, depth, typeOfKey, sizeOfKey, typeOfEntry, sizeOfEntry, op, block_num, key_block, tmpCounter = 0;
  char *data;
  scnFileInfoPtr curr_scan;
  BF_Block *mBlock;
  BF_Block_Init(&mBlock);

  if(scnArray[scanDesc] == NULL) {
    BF_Block_Destroy(&mBlock);
    AM_errno = AME_EOF;
    return NULL;
  }

  curr_scan = scnArray[scanDesc];

  root = opnArray[scanDesc]->root;
  depth = opnArray[scanDesc]->depth;
  typeOfKey = opnArray[scanDesc]->typeOfKey;
  sizeOfKey = opnArray[scanDesc]->sizeOfKey;
  typeOfEntry = opnArray[scanDesc]->typeOfEntry;
  sizeOfEntry = opnArray[scanDesc]->sizeOfEntry;
  op = scnArray[scanDesc]->op;
  value = scnArray[scanDesc]->value;

  key_block = getDataBlock(scanDesc, root, depth, value, typeOfKey, sizeOfKey);

  block_num = curr_scan->starting_block;

  AM_errno = AME_OK;

  while(block_num != curr_scan->ending_block) {
    BF_GetBlock(scanDesc, block_num, mBlock);
    data = BF_Block_GetData(mBlock);
    for(int i=0; i< *data; i++) {
      if(os_comparison(op, value, data + 2*sizeof(int) + i*sizeOfKey + i*sizeOfEntry, typeOfKey) != 0) {
        if(tmpCounter == scnArray[scanDesc]->printedCounter) {
          BF_UnpinBlock(mBlock);
          BF_Block_Destroy(&mBlock);
          scnArray[scanDesc]->printedCounter++;

          return (data + 2*sizeof(int) + (i+1)*sizeOfKey + i*sizeOfEntry);
        }
        else {
          tmpCounter++;
        }
      }
    }
    BF_UnpinBlock(mBlock);
    block_num = *(data + sizeof(int));
  }

  BF_UnpinBlock(mBlock);
  BF_Block_Destroy(&mBlock);

  AM_errno = AME_EOF;

  return NULL;
}

int os_comparison(int op, void* key, void* mKey, int typeOfKey) {

  int result = compareKeys2(key, mKey, typeOfKey);

  if(op == EQUAL) {
    if(result == 0) {
      return 1;
    }
    else {
      return 0;
    }
  }
  if(op == NOT_EQUAL) {
    if(result != 0) {
      return 1;
    }
    else {
      return 0;
    }
  }
  if(op == GREATER_THAN) {
    if(result == 1) {
      return 1;
      }
    else {return 0;}
  }
  if(op == GREATER_THAN_OR_EQUAL) {
    if(result == 1 || result == 0) {return 1;}
    else {return 0;}
  }
  if(op == LESS_THAN) {
    if(result == -1) {return 1;}
    else {return 0;}
  }
  if(op == LESS_THAN_OR_EQUAL) {
    if(result == -1 || result == 0) {return 1;}
    else {return 0;}
  }
}

int compareKeys2(void* key, void* mKey, int typeOfKey) {

  if(typeOfKey == STRING) {
    return strcmp((char*)key,(char*)mKey);
  }
  else if (typeOfKey == INTEGER) {
    if(*(int*)key < *(int*)mKey) {return -1;}
    if (*(int*)key == *(int*)key) {return 0;}
    return 1;
  }
  else {
    if(*(float*)key < *(float*)key) {return -1;}
    if (*(float*)key == *(float*)key) {return 0;}
    return 1;
  }
}

int AM_CloseIndexScan(int scanDesc) {

  if(scnArray[scanDesc] != NULL) {
    free(scnArray[scanDesc]);
    scnFilescounter--;
    return AME_OK;
  }

  return AME_EOF;
}


void AM_PrintError(char *errString) {

  if(errString != NULL) printf("%s\n",errString);
  return;
}

void AM_Close() {
  BF_Close();

  free(opnArray);
  free(scnArray);

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

  printf("NEXT BLOCK: %d\n", *(data + sizeof(int)));
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