#include "BlockAccess.h"
#include <cstring>
#include<stdio.h>


int BlockAccess::comparisons=0;

RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op) {
    // get the previous search index of the relation relId from the relation cache
    // (use RelCacheTable::getSearchIndex() function)
    RecId prevRecId;
    int ret=RelCacheTable::getSearchIndex(relId,&prevRecId);
    if(ret!=SUCCESS)
    return RecId{-1,-1};
    // let block and slot denote the record id of the record being currently checked
    int block,slot;
    // if the current search index record is invalid(i.e. both block and slot = -1)
    RelCatEntry relCatEntry;
    if (prevRecId.block == -1 && prevRecId.slot == -1)
    {
        // (no hits from previous search; search should start from the
        // first record itself)
        
        // get the first record block of the relation from the relation cache
        // (use RelCacheTable::getRelCatEntry() function of Cache Layer)
        RelCacheTable::getRelCatEntry(relId,&relCatEntry);
        block=relCatEntry.firstBlk;
        slot=0;
        // block = first record block of the relation
        // slot = 0
    }
    else
    {
        // (there is a hit from previous search; search should start from
        // the record next to the search index record)
        block=prevRecId.block;
        slot=prevRecId.slot+1;
        // block = search index's block
        // slot = search index's slot + 1
    }

    /* The following code searches for the next record in the relation
       that satisfies the given condition
       We start from the record id (block, slot) and iterate over the remaining
       records of the relation
    */
    while (block != -1)
    {
        /* create a RecBuffer object for block (use RecBuffer Constructor for
           existing block) */
           RecBuffer recBuffer(block);
           HeadInfo header;
           recBuffer.getHeader(&header);
           unsigned char slotMap[header.numSlots];
           recBuffer.getSlotMap(slotMap);
           // get the record with id (block, slot) using RecBuffer::getRecord()
           // get header of the block using RecBuffer::getHeader() function
           // get slot map of the block using RecBuffer::getSlotMap() function
           
           // If slot >= the number of slots per block(i.e. no more slots in this block)
           if(slot>=header.numSlots)
           {   block=header.rblock;
            slot=0;
            // update block = right block of block
            // update slot = 0
            continue;  // continue to the beginning of this while loop
        }
        
        // if slot is free skip the loop
        if(slotMap[slot]==SLOT_UNOCCUPIED)
        // (i.e. check if slot'th entry in slot map of block contains SLOT_UNOCCUPIED)
        {   slot++;
            continue;
            // increment slot and continue to the next record slot
        }
        
        // compare record's attribute value to the the given attrVal as below:
        /*
        firstly get the attribute offset for the attrName attribute
        from the attribute cache entry of the relation using
        AttrCacheTable::getAttrCatEntry()
        */
       Attribute record[header.numAttrs];
       recBuffer.getRecord(record,slot);

        AttrCatEntry attrCatEntry;
       AttrCacheTable::getAttrCatEntry(relId,attrName,&attrCatEntry);
     
        Attribute val;
       val=record[attrCatEntry.offset];
    //    printf("DEBUG: attr=%s offset=%d type=%d\n",
    //    attrCatEntry.attrName,
    //    attrCatEntry.offset,
    //    attrCatEntry.attrType);

        /* use the attribute offset to get the value of the attribute from
           current record */
           comparisons++;

        int cmpVal;  // will store the difference between the attributes
        // set cmpVal using compareAttrs()
        // printf("DEBUG: record value = %s , condition value = %s\n",
    //    record[attrCatEntry.offset].sVal,
    //    attrVal.sVal);

        cmpVal=compareAttrs(val,attrVal,attrCatEntry.attrType);
        /* Next task is to check whether this record satisfies the given condition.
           It is determined based on the output of previous comparison and
           the op value received.
           The following code sets the cond variable if the condition is satisfied.
        */
    //    printf("DEBUG: cmpVal = %d\n", cmpVal);

        if (
            (op == NE && cmpVal != 0) ||    // if op is "not equal to"
            (op == LT && cmpVal < 0) ||     // if op is "less than"
            (op == LE && cmpVal <= 0) ||    // if op is "less than or equal to"
            (op == EQ && cmpVal == 0) ||    // if op is "equal to"
            (op == GT && cmpVal > 0) ||     // if op is "greater than"
            (op == GE && cmpVal >= 0)       // if op is "greater than or equal to"
        ) {
            /*
            set the search index in the relation cache as
            the record id of the record that satisfies the given condition
            (use RelCacheTable::setSearchIndex function)
            */
            RecId newrecId;
            newrecId.block=block;
            newrecId.slot=slot;
            RelCacheTable::setSearchIndex(relId,&newrecId);
            return newrecId;
        }

        slot++;
    }

    // no record in the relation with Id relid satisfies the given condition
    return RecId{-1, -1};
}


int BlockAccess::renameRelation(char oldname[ATTR_SIZE],char newname[ATTR_SIZE])
{
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute newrelname;
    strcpy(newrelname.sVal,newname);
    
    RecId recid=BlockAccess::linearSearch(RELCAT_RELID,RELCAT_ATTR_RELNAME,newrelname,EQ);
    if(recid.block!=-1&&recid.slot!=-1)
    return E_RELEXIST;

    Attribute oldrelname;
    strcpy(oldrelname.sVal,oldname);

    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    recid=BlockAccess::linearSearch(RELCAT_RELID,RELCAT_ATTR_RELNAME,oldrelname,EQ);
    if(recid.block==-1&&recid.slot==-1)
    return E_RELNOTEXIST;

    RecBuffer relcat(RELCAT_BLOCK);

    Attribute record[RELCAT_NO_ATTRS];
    relcat.getRecord(record,recid.slot);

    strcpy(record[RELCAT_REL_NAME_INDEX].sVal,newrelname.sVal);
    relcat.setRecord(record,recid.slot);

    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    for(int i=0;i<record[RELCAT_NO_ATTRIBUTES_INDEX].nVal;i++)
    {
        RecId attrrecid=BlockAccess::linearSearch(ATTRCAT_RELID,ATTRCAT_ATTR_RELNAME,oldrelname,EQ);
        if(attrrecid.block==-1)
        break;
        RecBuffer attrbuffer(attrrecid.block);
        Attribute attrRecord[ATTRCAT_NO_ATTRS];
        attrbuffer.getRecord(attrRecord,attrrecid.slot);
        strcpy(attrRecord[ATTRCAT_REL_NAME_INDEX].sVal,newrelname.sVal);
        attrbuffer.setRecord(attrRecord,attrrecid.slot);
    }
    return SUCCESS;
}   

int BlockAccess::renameAttribute(char relname[ATTR_SIZE],char oldname[ATTR_SIZE],char newname[ATTR_SIZE])
{
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal,relname);
    RecId recid=BlockAccess::linearSearch(RELCAT_RELID,RELCAT_ATTR_RELNAME,relNameAttr,EQ);
    
    if(recid.block==-1&&recid.slot==-1)
    return E_RELNOTEXIST;

    RecId attrrecid;
    Attribute attrrecord[ATTRCAT_NO_ATTRS];
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    RecId reqrecid={-1,-1};
    while(true)
    {
        attrrecid=BlockAccess::linearSearch(ATTRCAT_RELID,ATTRCAT_ATTR_RELNAME,relNameAttr,EQ);
        if(attrrecid.block==-1 && attrrecid.slot==-1)
        break;

        RecBuffer attrcatbuffer(attrrecid.block);
        attrcatbuffer.getRecord(attrrecord,attrrecid.slot);

        // if(strcpy(attrrecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldname)==0)
        if(strcmp(attrrecord[ATTRCAT_ATTR_NAME_INDEX].sVal,oldname)==0)
        {
            reqrecid=attrrecid;
        }
        if(strcmp(attrrecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newname)==0)
        return E_ATTREXIST;
    }
    if(reqrecid.block==-1&&reqrecid.slot==-1)
    return E_ATTRNOTEXIST;

     RecBuffer attrcatbuffer(reqrecid.block);
     attrcatbuffer.getRecord(attrrecord,reqrecid.slot);
    strcpy(attrrecord[ATTRCAT_ATTR_NAME_INDEX].sVal,newname);
    attrcatbuffer.setRecord(attrrecord,reqrecid.slot);
    return SUCCESS;
}

int BlockAccess::insert(int relId,Attribute *record)
{
    RelCatEntry relCatEntry;
    int ret=RelCacheTable::getRelCatEntry(relId,&relCatEntry);

    int blockNum=relCatEntry.firstBlk;

    RecId recId={-1,-1};

    int numOfSlots=relCatEntry.numSlotsPerBlk;
    int numOfAttributes=relCatEntry.numAttrs;

    int prevBlockNum=-1;
    while(blockNum!=-1)
    {
        RecBuffer recBuffer(blockNum);
        struct HeadInfo head;
        recBuffer.getHeader(&head);
        unsigned char slotMap[head.numSlots];
        recBuffer.getSlotMap(slotMap);

        for(int i=0;i<head.numSlots;i++)
        {
            if(slotMap[i]==SLOT_UNOCCUPIED)
            {
                recId.block=blockNum;
                recId.slot=i;
                break;
            }
        }
        if(recId.block!=-1&&recId.slot!=-1)
        break;
        prevBlockNum=blockNum;
        blockNum=head.rblock;

    }
    if(recId.block==-1&&recId.slot==-1)
    {
        if(relId==RELCAT_RELID)
        {
            return E_MAXRELATIONS;
        }
        // RecBuffer recBuffer();
       RecBuffer recBuffer;
      int ret=recBuffer.getBlockNum();
        if(ret==E_DISKFULL)
        return E_DISKFULL;

        recId.block=ret;
        recId.slot=0;

        struct HeadInfo newBlockHead;
        newBlockHead.blockType=REC;
        newBlockHead.rblock=-1;
        newBlockHead.lblock=prevBlockNum;
        newBlockHead.pblock=-1;
        newBlockHead.numAttrs=numOfAttributes;
        newBlockHead.numEntries=0;
        newBlockHead.numSlots=numOfSlots;
        recBuffer.setHeader(&newBlockHead);
        unsigned char slotMap[numOfSlots];
        for(int i=0;i<numOfSlots;i++)
        {
            slotMap[i]=SLOT_UNOCCUPIED;
        }
        recBuffer.setSlotMap(slotMap);
        if(prevBlockNum!=-1)
        {
            RecBuffer prevRecBuffer(prevBlockNum);
            struct HeadInfo prevHead;
            prevRecBuffer.getHeader(&prevHead);
            prevHead.rblock=recId.block;
            prevRecBuffer.setHeader(&prevHead);
        }
        else{
            relCatEntry.firstBlk=recId.block;
            RelCacheTable::setRelCatEntry(relId,&relCatEntry);

        }
        relCatEntry.lastBlk=recId.block;
        RelCacheTable::setRelCatEntry(relId,&relCatEntry);

    }
    RecBuffer recBuffer(recId.block);
    recBuffer.setRecord(record,recId.slot);
    unsigned char slotMap[numOfSlots];
    recBuffer.getSlotMap(slotMap);
    slotMap[recId.slot]=SLOT_OCCUPIED;
    recBuffer.setSlotMap(slotMap);
    struct HeadInfo head;
    recBuffer.getHeader(&head);
    head.numEntries++;
    recBuffer.setHeader(&head);

    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId,&relCatEntry);
    return SUCCESS;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op) {
    // Declare a variable called recid to store the searched record
    RecId recId;
    
    /* get the attribute catalog entry from the attribute cache corresponding
       to the relation with Id=relid and with attribute_name=attrName  */
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    
    // if this call returns an error, return the appropriate error code
    if (ret != SUCCESS) {
        return ret;  // Return error code (e.g., E_ATTRNOTEXIST, E_RELNOTOPEN, etc.)
    }
    
    // get rootBlock from the attribute catalog entry
    int rootBlock = attrCatEntry.rootBlock;
    
    /* if Index does not exist for the attribute (check rootBlock == -1) */
    if (rootBlock == INVALID_BLOCKNUM) {
        /* search for the record id (recid) corresponding to the attribute with
           attribute name attrName, with value attrval and satisfying the
           condition op using linearSearch()
        */
        recId = linearSearch(relId, attrName, attrVal, op);
    }
    /* else */ 
    else {
        // (index exists for the attribute)
        /* search for the record id (recid) corresponding to the attribute with
           attribute name attrName and with value attrval and satisfying the
           condition op using BPlusTree::bPlusSearch() */
        recId = BPlusTree::bPlusSearch(relId, attrName, attrVal, op);
    }
    
    // if there's no record satisfying the given condition (recId = {-1, -1})
    if (recId.block == -1 && recId.slot == -1) {
        return E_NOTFOUND;  // No matching record found
    }
    
    /* Copy the record with record id (recId) to the record buffer (record).
       For this, instantiate a RecBuffer class object by passing the recId.block
       and call the appropriate method to fetch the record
    */
    // Create a RecBuffer object for the block containing the record
    RecBuffer recBuffer(recId.block);
    
    // Fetch the record from the specified slot into the record buffer
    ret = recBuffer.getRecord(record, recId.slot);
    
    // If getRecord() fails, return the error code
    if (ret != SUCCESS) {
        return ret;
    }
    
    // Record successfully retrieved and copied to the record buffer
    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]){
    if(strcmp(relName,RELCAT_RELNAME)==0||strcmp(relName,ATTRCAT_RELNAME)==0)
    return E_NOTPERMITTED;
    int retVal;
retVal=RelCacheTable::resetSearchIndex(RELCAT_RELID);
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal,relName);

    RecId recId=linearSearch(RELCAT_RELID,RELCAT_ATTR_RELNAME,relNameAttr,EQ);

    if(recId.block==-1&&recId.slot==-1)
    return E_RELNOTEXIST;

    Attribute relCatEntry[RELCAT_NO_ATTRS];
    RecBuffer recBuffer(recId.block);
   retVal=recBuffer.getRecord(relCatEntry,recId.slot);
    if(retVal!=SUCCESS)
    return retVal;

    int firstBlk=relCatEntry[RELCAT_FIRST_BLOCK_INDEX].nVal;
    int blockNum=firstBlk;
    while(blockNum!=-1)
    {
        RecBuffer recBlock(blockNum);
        struct HeadInfo head;
        retVal=recBlock.getHeader(&head);
        if(retVal!=SUCCESS)
        return retVal;
        blockNum=head.rblock;
        recBlock.releaseBlock();
    }
   retVal=RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
   if(retVal!=SUCCESS)
   return retVal;
    int numOfAttrsDel=0;
    Attribute attrRelName;
   strcpy(attrRelName.sVal,relName);
    while(true)
    {  
        RecId attrCatRecId=linearSearch(ATTRCAT_RELID,ATTRCAT_ATTR_RELNAME,attrRelName,EQ);
        if(attrCatRecId.block==-1&&attrCatRecId.slot==-1)
        break;

        numOfAttrsDel++;
        RecBuffer attrCatRecBuffer(attrCatRecId.block);
        struct HeadInfo attrCatHead;
        retVal=attrCatRecBuffer.getHeader(&attrCatHead);
        if(retVal!=SUCCESS)
        return retVal;

        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        retVal=attrCatRecBuffer.getRecord(attrCatRecord,attrCatRecId.slot);
        if(retVal!=SUCCESS)
        return retVal;

        int rootBlock=attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        unsigned char slotMap[attrCatHead.numSlots];

        retVal=attrCatRecBuffer.getSlotMap(slotMap);
        if(retVal!=SUCCESS)
        return retVal;

        slotMap[attrCatRecId.slot]=SLOT_UNOCCUPIED;

        retVal=attrCatRecBuffer.setSlotMap(slotMap);
        if(retVal!=SUCCESS)
        return retVal;


        attrCatHead.numEntries--;
        retVal=attrCatRecBuffer.setHeader(&attrCatHead);
        if(retVal!=SUCCESS)
        return retVal;


        if (attrCatHead.numEntries == 0) {

    // Guard: only update left block's rblock if a left block actually exists.
    // If lblock == -1, this is the first attrcat block — there is no left neighbour.
    if (attrCatHead.lblock != -1) {
        RecBuffer attrCatLeftBlock(attrCatHead.lblock);
        struct HeadInfo attrCatLeftHead;
        retVal = attrCatLeftBlock.getHeader(&attrCatLeftHead);
        if (retVal != SUCCESS) return retVal;

        attrCatLeftHead.rblock = attrCatHead.rblock;

        retVal = attrCatLeftBlock.setHeader(&attrCatLeftHead);
        if (retVal != SUCCESS) return retVal;
    } else {
        // This was the first attrcat block — update ATTRCAT's firstBlk
        // in the relation catalog cache to point to the next block.
        // (rblock becomes the new first block of the attrcat chain)
        RelCatEntry attrCatRelEntry;
        RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &attrCatRelEntry);
        attrCatRelEntry.firstBlk = attrCatHead.rblock;
        RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &attrCatRelEntry);
    }

    if (attrCatHead.rblock != -1) {
        struct HeadInfo attrCatRightHead;
        RecBuffer attrCatRightBlock(attrCatHead.rblock);
        retVal = attrCatRightBlock.getHeader(&attrCatRightHead);
        if (retVal != SUCCESS) return retVal;

        attrCatRightHead.lblock = attrCatHead.lblock;

        retVal = attrCatRightBlock.setHeader(&attrCatRightHead);
        if (retVal != SUCCESS) return retVal;
    } else {
        // This was also the last attrcat block — update ATTRCAT's lastBlk.
        // After release, the attrcat chain has no blocks at all.
        RecBuffer relCatBuffer(RELCAT_BLOCK);
        Attribute relCatRecord[RELCAT_NO_ATTRS];
        retVal = relCatBuffer.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
        if (retVal != SUCCESS) return retVal;

        relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = attrCatHead.lblock;

        retVal = relCatBuffer.setRecord(relCatRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
        if (retVal != SUCCESS) return retVal;
    }

    attrCatRecBuffer.releaseBlock();
}
    }
    struct HeadInfo relCatHead;
    RecBuffer relCatBuffer(RELCAT_BLOCK);
    retVal=relCatBuffer.getHeader(&relCatHead);
    if(retVal!=SUCCESS)
    return retVal;
    relCatHead.numEntries--;
    retVal=relCatBuffer.setHeader(&relCatHead);
    if(retVal!=SUCCESS)
    return retVal;
    unsigned char slotMap[relCatHead.numSlots];
    retVal=relCatBuffer.getSlotMap(slotMap);
    if(retVal!=SUCCESS)
    return retVal;
    slotMap[recId.slot]=SLOT_UNOCCUPIED;
    retVal=relCatBuffer.setSlotMap(slotMap);
    if(retVal!=SUCCESS)
    return retVal;
    RelCatEntry relCatEntryCache;
  RelCacheTable::getRelCatEntry(RELCAT_RELID,&relCatEntryCache);
  relCatEntryCache.numRecs--;
  RelCacheTable::setRelCatEntry(RELCAT_RELID,&relCatEntryCache);

    RelCatEntry attrCatEntryCache;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID,&attrCatEntryCache);
    attrCatEntryCache.numRecs=attrCatEntryCache.numRecs-numOfAttrsDel;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID,&attrCatEntryCache);

    return SUCCESS;
}   
/*
NOTE: the caller is expected to allocate space for the argument `record` based
      on the size of the relation. This function will only copy the result of
      the projection onto the array pointed to by the argument.
*/
int BlockAccess::project(int relId, Attribute *record) {

    // Get the previous search index of the relation relId from the relation cache.
    // This tells us where the last project/search operation left off.
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    // block and slot will hold the starting position for this iteration.
    int block, slot;

    /* If the current search index is invalid (i.e. = {-1, -1}),
       this means the caller has reset the search index, indicating
       a fresh/new project operation — start from the very beginning. */
    if (prevRecId.block == INVALID_BLOCKNUM && prevRecId.slot == -1) {

        // Fetch the relation catalog entry to get metadata about this relation.
        RelCatEntry relCatBuf;
        RelCacheTable::getRelCatEntry(relId, &relCatBuf);

        // Start scanning from the very first record block of the relation, slot 0.
        block = relCatBuf.firstBlk;
        slot = 0;

    } else {
        // A project/search operation is already in progress.
        // Resume from just after the previously returned record.
        block = prevRecId.block;
        slot = prevRecId.slot + 1; // move to the next slot
    }

    // The following code finds the next occupied record in the relation
    // starting from (block, slot), scanning block by block.

    /* Iterate over blocks (linked via rblock) until we either find an
       occupied slot or exhaust all blocks (block becomes INVALID_BLOCKNUM). */
    while (block != INVALID_BLOCKNUM) {

        // Create a RecBuffer for the current block to read its contents.
        RecBuffer recBuffer(block);

        // Read the header to get metadata: numSlots, rblock, etc.
        HeadInfo header;
        recBuffer.getHeader(&header);

        // Read the slot map to know which slots are occupied or free.
        unsigned char slotMap[header.numSlots];
        recBuffer.getSlotMap(slotMap);

        if (slot >= header.numSlots) {
            // No more slots in the current block — move to the next block.
            // rblock holds the block number of the next record block
            // (INVALID_BLOCKNUM = -1 if this is the last block, ending the loop).
            block = header.rblock;
            slot = 0; // reset slot to the beginning of the new block
        }
        else if (slotMap[slot] == SLOT_UNOCCUPIED) {
            // This slot is free/deleted — skip it and check the next slot.
            slot++;
        }
        else {
            // Found an occupied slot — this is our next record to return.
            break;
        }
    }

    // If block is INVALID_BLOCKNUM, we've scanned all records without finding one.
    if (block == INVALID_BLOCKNUM) {
        // All records have been exhausted — signal end of scan to the caller.
        return E_NOTFOUND;
    }

    // Store the RecId of the found record.
    RecId nextRecId{block, slot};

    // Update the search index in the relation cache to this record's position,
    // so the next call to project() will continue from the record after this one.
    RelCacheTable::setSearchIndex(relId, &nextRecId);

    // Instantiate a RecBuffer for the found block and fetch the record at `slot`
    // into the caller-provided `record` buffer.
    RecBuffer recBuffer(block);
    recBuffer.getRecord(record, slot);

    return SUCCESS;
}