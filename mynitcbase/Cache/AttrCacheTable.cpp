#include "AttrCacheTable.h"

#include <cstring>
AttrCacheEntry* AttrCacheTable::attrCache[MAX_OPEN];

/* returns the attrOffset-th attribute for the relation corresponding to relId
NOTE: this function expects the caller to allocate memory for `*attrCatBuf`
*/
int AttrCacheTable::getAttrCatEntry(int relId, int attrOffset, AttrCatEntry* attrCatBuf) {
  // check if 0 <= relId < MAX_OPEN and return E_OUTOFBOUND otherwise
    if(relId<0||relId>=MAX_OPEN)
    return E_OUTOFBOUND;


  // check if attrCache[relId] == nullptr and return E_RELNOTOPEN if true
    if(attrCache[relId]==nullptr)
    return E_RELNOTOPEN;


  // traverse the linked list of attribute cache entries
  for (AttrCacheEntry* entry = attrCache[relId]; entry != nullptr; entry = entry->next) {
    if (entry->attrCatEntry.offset == attrOffset) {

      // copy entry->attrCatEntry to *attrCatBuf and return SUCCESS;
        *attrCatBuf=entry->attrCatEntry;
        return SUCCESS;
    }
  }

  // there is no attribute at this offset
  return E_ATTRNOTEXIST;
}
int AttrCacheTable::getAttrCatEntry(int relId, char attrName[ATTR_SIZE], AttrCatEntry *attrCatBuf)
{
  if(relId<0||relId>=MAX_OPEN)
  return E_OUTOFBOUND;

  if(attrCache[relId]==nullptr)
  return E_RELNOTOPEN;

  for(AttrCacheEntry* i=attrCache[relId];i!=nullptr;i=i->next)
  {
    if(strcmp(i->attrCatEntry.attrName,attrName)==0)
    {
      *attrCatBuf=i->attrCatEntry;
      return SUCCESS;
    }
  }
  return E_ATTRNOTEXIST;
}
/* Converts a attribute catalog record to AttrCatEntry struct
    We get the record as Attribute[] from the BlockBuffer.getRecord() function.
    This function will convert that to a struct AttrCatEntry type.
*/
void AttrCacheTable::recordToAttrCatEntry(union Attribute record[ATTRCAT_NO_ATTRS],AttrCatEntry* attrCatEntry) {
  strcpy(attrCatEntry->relName, record[ATTRCAT_REL_NAME_INDEX].sVal);

  strcpy(attrCatEntry->attrName,record[ATTRCAT_ATTR_NAME_INDEX].sVal);

  attrCatEntry->attrType=record[ATTRCAT_ATTR_TYPE_INDEX].nVal;

  attrCatEntry->offset=record[ATTRCAT_OFFSET_INDEX].nVal;

  attrCatEntry->primaryFlag=record[ATTRCAT_PRIMARY_FLAG_INDEX].nVal;

  attrCatEntry->rootBlock=record[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
  // copy the rest of the fields in the record to the attrCacheEntry struct
}


int AttrCacheTable::getSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {

    // Check relId bounds
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Check if relation is open (entry exists)
    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Traverse attribute list for this relation
    AttrCacheEntry *entry = AttrCacheTable::attrCache[relId];

    while (entry != nullptr) {

        // Compare attribute names
        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {

            // Copy searchIndex
            *searchIndex = entry->searchIndex;

            return SUCCESS;
        }

        entry = entry->next;
    }

    return E_ATTRNOTEXIST;
}
int AttrCacheTable::getSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {

    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *entry = AttrCacheTable::attrCache[relId];

    while (entry != nullptr) {

        if (entry->attrCatEntry.offset == attrOffset) {

            *searchIndex = entry->searchIndex;
            return SUCCESS;
        }

        entry = entry->next;
    }

    return E_ATTRNOTEXIST;
}
int AttrCacheTable::setSearchIndex(int relId, char attrName[ATTR_SIZE], IndexId *searchIndex) {

    // Check relId bounds
    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    // Check if relation is open
    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    // Traverse attribute list
    AttrCacheEntry *entry = AttrCacheTable::attrCache[relId];

    while (entry != nullptr) {

        // Match attribute name
        if (strcmp(entry->attrCatEntry.attrName, attrName) == 0) {

            // Copy input searchIndex into cache
            entry->searchIndex = *searchIndex;

            return SUCCESS;
        }

        entry = entry->next;
    }

    return E_ATTRNOTEXIST;
}
int AttrCacheTable::setSearchIndex(int relId, int attrOffset, IndexId *searchIndex) {

    if (relId < 0 || relId >= MAX_OPEN) {
        return E_OUTOFBOUND;
    }

    if (AttrCacheTable::attrCache[relId] == nullptr) {
        return E_RELNOTOPEN;
    }

    AttrCacheEntry *entry = AttrCacheTable::attrCache[relId];

    while (entry != nullptr) {

        if (entry->attrCatEntry.offset == attrOffset) {

            entry->searchIndex = *searchIndex;

            return SUCCESS;
        }

        entry = entry->next;
    }

    return E_ATTRNOTEXIST;
}
int AttrCacheTable::resetSearchIndex(int relId, char attrName[ATTR_SIZE]) {

  // declare an IndexId having value {-1, -1}
  // set the search index to {-1, -1} using AttrCacheTable::setSearchIndex
  // return the value returned by setSearchIndex
  IndexId temp={-1,-1};
  int x=AttrCacheTable::setSearchIndex(relId,attrName,&temp);
  return x;
}
int AttrCacheTable::resetSearchIndex(int relId, int attrOffset) {

  // declare an IndexId having value {-1, -1}
  // set the search index to {-1, -1} using AttrCacheTable::setSearchIndex
  // return the value returned by setSearchIndex
  IndexId temp={-1,-1};
  int x=AttrCacheTable::setSearchIndex(relId,attrOffset,&temp);
  return x;
}











