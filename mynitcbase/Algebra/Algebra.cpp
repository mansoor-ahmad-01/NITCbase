#include "Algebra.h"

#include <cstring>
#include<stdio.h>
#include<cstdlib>
#include<iostream>
/* used to select all the records that satisfy a condition.
the arguments of the function are
- srcRel - the source relation we want to select from
- targetRel - the relation we want to select into. (ignore for now)
- attr - the attribute that the condition is checking
- op - the operator of the condition
- strVal - the value that we want to compare against (represented as a string)
*/
bool isNumber(char *str);
// int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
//   int srcRelId = OpenRelTable::getRelId(srcRel);      // we'll implement this later
//   if (srcRelId == E_RELNOTOPEN) {
//     return E_RELNOTOPEN;
//   }

//   AttrCatEntry attrCatEntry;
//   // get the attribute catalog entry for attr, using AttrCacheTable::getAttrcatEntry()
//   //    return E_ATTRNOTEXIST if it returns the error
  
//  int ret= AttrCacheTable::getAttrCatEntry(srcRelId,attr,&attrCatEntry);
//  if(ret!=SUCCESS)
//  return ret;
//   /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/
//   int type = attrCatEntry.attrType;
//   Attribute attrVal;
//   if (type == NUMBER) {
//     if (isNumber(strVal)) {       // the isNumber() function is implemented below
//       attrVal.nVal = atof(strVal);
//     } else {
//       return E_ATTRTYPEMISMATCH;
//     }
//   } else if (type == STRING) {
//     strcpy(attrVal.sVal, strVal);
//   }

//   /*** Selecting records from the source relation ***/

//   // Before calling the search function, reset the search to start from the first hit
//   // using RelCacheTable::resetSearchIndex()
//   RelCacheTable::resetSearchIndex(srcRelId);

//   RelCatEntry relCatEntry;
//  int r= RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);
//  if(r!=SUCCESS)
//  return r;
// //  printf("DEBUG: numAttrs = %d\n", relCatEntry.numAttrs);
//   // get relCatEntry using RelCacheTable::getRelCatEntry()

//   /************************
//   The following code prints the contents of a relation directly to the output
//   console. Direct console output is not permitted by the actual the NITCbase
//   specification and the output can only be inserted into a new relation. We will
//   be modifying it in the later stages to match the specification.
//   ************************/

//   printf("|");
//   for (int i = 0; i < relCatEntry.numAttrs; ++i) {
//     AttrCatEntry attrCatEntry;
//     // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
//     AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
//     printf(" %s |", attrCatEntry.attrName);
//   }
//   printf("\n");

//   while (true) {
//     RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);

//     if (searchRes.block != -1 && searchRes.slot != -1) {

//       // get the record at searchRes using BlockBuffer.getRecord

//       // print the attribute values in the same format as above
//       RecBuffer recBuffer(searchRes.block);
//       Attribute record[relCatEntry.numAttrs];
//       recBuffer.getRecord(record,searchRes.slot);
//       printf("|");
//   for (int i = 0; i < relCatEntry.numAttrs; ++i) {
//     AttrCatEntry attrCatEntry;
//     // get attrCatEntry at offset i using AttrCacheTable::getAttrCatEntry()
//     AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
//     if(attrCatEntry.attrType==NUMBER)
//     printf(" %0.0f |",record[i].nVal);
//     else
//     printf(" %s |",record[i].sVal);
//   }
//   printf("\n");
//     } else {

//       // (all records over)
//       break;
//     }
//   }

//   return SUCCESS;
// }
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]) {
    // get the srcRel's rel-id (let it be srcRelid), using OpenRelTable::getRelId()
    int srcRelid=OpenRelTable::getRelId(srcRel);
    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    if(srcRelid==E_RELNOTOPEN)
    {
      return E_RELNOTOPEN;
    }

    // get the attr-cat entry for attr, using AttrCacheTable::getAttrCatEntry()
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelid,attr,&attrCatEntry);
    // if getAttrcatEntry() call fails return E_ATTRNOTEXIST
    if(ret != SUCCESS)
    {
        return E_ATTRNOTEXIST;
    }

    /*** Convert strVal to an attribute of data type NUMBER or STRING ***/

    Attribute attrVal;
    int type = attrCatEntry.attrType;

    if (type == NUMBER)
    {
        // if the input argument strVal can be converted to a number
        // (check this using isNumber() function)
        if(isNumber(strVal))
        {
            // convert strVal to double and store it at attrVal.nVal using atof()
            attrVal.nVal=atof(strVal);
        }
        else
        {
            return E_ATTRTYPEMISMATCH;
        }
    }
    else if (type == STRING)
    {
        // copy strVal to attrVal.sVal
        strcpy(attrVal.sVal,strVal);
    }

    /*** Creating and opening the target relation ***/
    // Prepare arguments for createRel() in the following way:
    // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relcatentry;
    RelCacheTable::getRelCatEntry(srcRelid,&relcatentry);
    int src_nAttrs = relcatentry.numAttrs;


    /* let attr_names[src_nAttrs][ATTR_SIZE] be a 2D array of type char
        (will store the attribute names of rel). */
        char attr_names[src_nAttrs][ATTR_SIZE];
    // let attr_types[src_nAttrs] be an array of type int
        int attr_types[src_nAttrs];

    /*iterate through 0 to src_nAttrs-1 :
        get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
        fill the attr_names, attr_types arrays that we declared with the entries
        of corresponding attributes
    */
    for(int i=0;i<src_nAttrs;i++)
    {
      AttrCatEntry attrcatentry;
      AttrCacheTable::getAttrCatEntry(srcRelid,i,&attrcatentry);
      strcpy(attr_names[i],attrcatentry.attrName);
      attr_types[i]=attrcatentry.attrType;
    }


    /* Create the relation for target relation by calling Schema::createRel()
       by providing appropriate arguments */
    // if the createRel returns an error code, then return that value.
    ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if(ret != SUCCESS)
    {
        return ret;
    }

    /* Open the newly created target relation by calling OpenRelTable::openRel()
       method and store the target relid */
    /* If opening fails, delete the target relation by calling Schema::deleteRel()
       and return the error value returned from openRel() */
    int targetRelId = OpenRelTable::openRel(targetRel);
    if(targetRelId < 0)
    {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Selecting and inserting records into the target relation ***/
    /* Before calling the search function, reset the search to start from the
       first using RelCacheTable::resetSearchIndex() */

    Attribute record[src_nAttrs];

    /*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
    */
    RelCacheTable::resetSearchIndex(srcRelid);
    AttrCacheTable::resetSearchIndex(srcRelid, attr);

    BlockAccess::comparisons = 0;
  // Check if index exists to determine search method
    int rootBlock = attrCatEntry.rootBlock;
    bool usingIndex = (rootBlock != INVALID_BLOCKNUM);
    
    // Count records inserted
    int recordCount = 0;
    // read every record that satisfies the condition by repeatedly calling
    // BlockAccess::search() until there are no more records to be read

    while (BlockAccess::search(srcRelid, record, attr,attrVal, op) == SUCCESS) {

        // ret = BlockAccess::insert(targetRelId, record);
        ret = BlockAccess::insert(targetRelId, record);

        // if (insert fails) {
        //     close the targetrel(by calling Schema::closeRel(targetrel))
        //     delete targetrel (by calling Schema::deleteRel(targetrel))
        //     return ret;
        // }
        if(ret != SUCCESS)
        {
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
        recordCount++;
    }

    if (usingIndex) {
        std::cout << " B+ Tree Index Search" << std::endl;
    } else {
        std::cout << "Linear Search" << std::endl;
    }
    std::cout << "Total Comparisons: " << BlockAccess::comparisons << std::endl;
    std::cout << "Records Found: " << recordCount << std::endl;
   
    

    // Close the targetRel by calling closeRel() method of schema layer
    Schema::closeRel(targetRel);

    // return SUCCESS.
    return SUCCESS;
}

// will return if a string can be parsed as a floating point number
bool isNumber(char *str) {
  int len;
  float ignore;
  /*
    sscanf returns the number of elements read, so if there is no float matching
    the first %f, ret will be 0, else it'll be 1

    %n gets the number of characters read. this scanf sequence will read the
    first float ignoring all the whitespace before and after. and the number of
    characters read that far will be stored in len. if len == strlen(str), then
    the string only contains a float with/without whitespace. else, there's other
    characters.
  */
  int ret = sscanf(str, "%f %n", &ignore, &len);
  return ret == 1 && len == strlen(str);
}
int Algebra::insert(char relName[ATTR_SIZE],int nAttrs,char record[][ATTR_SIZE])
{
  if(strcmp(relName,RELCAT_RELNAME)==0)
  return E_NOTPERMITTED;
  if(strcmp(relName,ATTRCAT_RELNAME)==0)
  return E_NOTPERMITTED;

  int relId=OpenRelTable::getRelId(relName);
  if(relId==E_RELNOTOPEN)
  return relId;

  RelCatEntry relCatEntry;
  RelCacheTable::getRelCatEntry(relId,&relCatEntry);
  if(relCatEntry.numAttrs!=nAttrs)
  return E_NATTRMISMATCH;

  Attribute recordValues[nAttrs];

  for(int i=0;i<nAttrs;i++)
  { AttrCatEntry attrCatBuf;
    AttrCacheTable::getAttrCatEntry(relId,i,&attrCatBuf);
    int type=attrCatBuf.attrType;
    if(type==NUMBER)
    {
      if(isNumber(record[i]))
      {
        recordValues[i].nVal=atof(record[i]);
      }
      else
      return E_ATTRTYPEMISMATCH;
    }
    else if(type==STRING){
        strcpy(recordValues[i].sVal,record[i]);
    }
  }
  int retVal=BlockAccess::insert(relId,recordValues);
  return retVal;
}
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    // Get the rel-id of the source relation from the Open Relation Table.
    int srcRelId = OpenRelTable::getRelId(srcRel);

    // If srcRel is not open, we cannot proceed with any cache or block operations.
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    // Fetch the relation catalog entry of srcRel to get its metadata,
    // particularly the number of attributes it contains.
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

    // Extract the number of attributes — needed to size our arrays and
    // to iterate through all attribute catalog entries.
    int numAttrs = srcRelCatEntry.numAttrs;

    // These arrays will hold the schema of the source relation,
    // which will be reused as-is for the target relation (full projection).
    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    // Populate attrNames and attrTypes by reading each attribute's
    // catalog entry using its offset (positional index).
    for (int i = 0; i < numAttrs; i++) {
        AttrCatEntry attrCatEntry;
        // Fetch the i-th attribute's entry from the attribute cache.
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);

        // Store the attribute name and type in our schema arrays.
        strcpy(attrNames[i], attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }

    /*** Creating and opening the target relation ***/

    // Create the target relation with the exact same schema as the source.
    // (project with no attribute list = copy all attributes)
    int createRet = Schema::createRel(targetRel, numAttrs, attrNames, attrTypes);

    // If creation fails (e.g. relation already exists, disk full),
    // propagate the error immediately — nothing to clean up yet.
    if (createRet != SUCCESS) {
        return createRet;
    }

    // Open the newly created target relation to obtain its rel-id,
    // which is required for BlockAccess::insert().
    int targetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, delete the target relation we just created
    // to avoid leaving an empty orphaned relation on disk.
    if (targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Reset the search index of srcRel in the relation cache so that
    // BlockAccess::project() starts scanning from the very first record.
    RelCacheTable::resetSearchIndex(srcRelId);

    // Buffer to hold one record fetched from the source relation at a time.
    Attribute record[numAttrs];

    // Repeatedly fetch the next record from srcRel via BlockAccess::project().
    // Each call fills `record` with the next tuple and advances the search index.
    // The loop exits when E_NOTFOUND is returned (all records exhausted).
    while (BlockAccess::project(srcRelId, record) == SUCCESS) {

        // Insert the fetched record into the target relation.
        int ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS) {
            // Insertion failed (e.g. disk full mid-way). Clean up:
            // close the target relation and delete it to avoid a
            // partially filled relation being left behind.
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // All records have been projected and inserted successfully.
    // Close the target relation so its cache entry is written back to disk.
    Schema::closeRel(targetRel);

    return SUCCESS;
}
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_Attrs[][ATTR_SIZE]) {

    // Get the rel-id of the source relation from the Open Relation Table.
    int srcRelId = OpenRelTable::getRelId(srcRel);

    // If srcRel is not open, we cannot access its cache entries or records.
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    // Fetch the relation catalog entry of srcRel to get its metadata.
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

    // Number of attributes in the source relation.
    // Needed to size the full record buffer when reading from srcRel.
    int src_nAttrs = srcRelCatEntry.numAttrs;

    // attr_offset[i] will store the offset (position in a source record)
    // of the i-th target attribute, so we can extract it from each fetched record.
    int attr_offset[tar_nAttrs];

    // attr_types[i] will store the type (NUMBER or STRING) of the i-th
    // target attribute, used when creating the target relation's schema.
    int attr_types[tar_nAttrs];

    /*** Validate that every requested target attribute exists in srcRel
         and record its offset and type for later use ***/

    for (int i = 0; i < tar_nAttrs; i++) {
        AttrCatEntry attrCatEntry;

        // Look up the attribute by name in the source relation's attribute cache.
        int getAttrRet = AttrCacheTable::getAttrCatEntry(srcRelId, tar_Attrs[i], &attrCatEntry);

        // If the attribute name is not found in srcRel, the projection is invalid.
        if (getAttrRet != SUCCESS) {
            return E_ATTRNOTEXIST;
        }

        // Record the offset of this attribute within a source record.
        // This offset will be used to pick the right field out of each full record.
        attr_offset[i] = attrCatEntry.offset;

        // Record the type of this attribute for schema creation of the target.
        attr_types[i] = attrCatEntry.attrType;
    }

    /*** Creating and opening the target relation ***/

    // Create the target relation using only the projected attributes
    // (tar_Attrs as names, attr_types as types).
    int createRet = Schema::createRel(targetRel, tar_nAttrs, tar_Attrs, attr_types);

    // If creation fails (e.g. relation already exists, disk full),
    // propagate the error — nothing to clean up yet.
    if (createRet != SUCCESS) {
        return createRet;
    }

    // Open the newly created target relation to get its rel-id,
    // required for BlockAccess::insert().
    int targetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, delete the just-created target relation to
    // avoid leaving an orphaned empty relation on disk.
    if (targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Inserting projected records into the target relation ***/

    // Reset the search index of srcRel so that BlockAccess::project()
    // starts scanning from the very first record of the relation.
    RelCacheTable::resetSearchIndex(srcRelId);

    // Buffer to hold one full record from the source relation.
    // Must be sized to src_nAttrs since BlockAccess::project() fills
    // all attributes of a source record, not just the projected ones.
    Attribute record[src_nAttrs];

    // Fetch records from srcRel one by one until all are exhausted.
    while (BlockAccess::project(srcRelId, record) == SUCCESS) {

        // Build the projected record by picking only the target attributes
        // from the full source record, using the precomputed offsets.
        Attribute proj_record[tar_nAttrs];
        for (int i = 0; i < tar_nAttrs; i++) {
            // attr_offset[i] gives the position of this attribute in the
            // source record, which directly maps to the right Attribute slot.
            proj_record[i] = record[attr_offset[i]];
        }

        // Insert the projected (subset) record into the target relation.
        int ret = BlockAccess::insert(targetRelId, proj_record);

        if (ret != SUCCESS) {
            // Insertion failed (e.g. disk full mid-way). Clean up:
            // close and delete the partially filled target relation to
            // avoid leaving corrupted or incomplete data on disk.
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    // All records have been projected and inserted successfully.
    // Close the target relation so its cache entry is flushed back to disk.
    Schema::closeRel(targetRel);

    return SUCCESS;
}