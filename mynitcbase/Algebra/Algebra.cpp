#include "Algebra.h"

#include <cstring>
#include<stdio.h>
#include<cstdlib>
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

    // Get the rel-id of srcRel from the Open Relation Table.
    // This is needed for all cache and block access operations.
    int srcRelId = OpenRelTable::getRelId(srcRel);

    // If srcRel is not currently open, we cannot proceed.
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    // Fetch the attribute catalog entry for the given attribute `attr`
    // to determine its type (NUMBER or STRING) and other metadata.
    AttrCatEntry attrCatEntry;
    int attrCatRet = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);

    // If the attribute does not exist in the relation, return error.
    if (attrCatRet != SUCCESS) {
        return E_ATTRNOTEXIST;
    }

    /*** Convert strVal to an Attribute value of the appropriate type ***/

    Attribute attrVal;
    int type = attrCatEntry.attrType;

    if (type == NUMBER) {
        // Check if strVal is a valid number string before converting.
        if (isNumber(strVal)) {
            // Convert the string to a double and store in the nVal field.
            attrVal.nVal = atof(strVal);
        } else {
            // strVal cannot be interpreted as a number — type mismatch.
            return E_ATTRTYPEMISMATCH;
        }
    } else if (type == STRING) {
        // Copy the string value directly into the sVal field of the attribute.
        strcpy(attrVal.sVal, strVal);
    }

    /*** Creating and opening the target relation ***/

    // Fetch the relation catalog entry of srcRel to know the number of
    // attributes and their layout, which the target relation will mirror.
    RelCatEntry srcRelCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &srcRelCatEntry);

    // The target relation will have the same schema as the source relation.
    int src_nAttrs = srcRelCatEntry.numAttrs;

    // Arrays to hold attribute names and types for Schema::createRel().
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];

    // Populate attr_names and attr_types by reading each attribute's
    // catalog entry from the attribute cache of the source relation.
    for (int i = 0; i < src_nAttrs; i++) {
        AttrCatEntry iAttrCatEntry;
        // Fetch the i-th attribute by offset index.
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &iAttrCatEntry);

        // Copy attribute name and type into the arrays.
        strcpy(attr_names[i], iAttrCatEntry.attrName);
        attr_types[i] = iAttrCatEntry.attrType;
    }

    // Create the target relation with the same schema as the source.
    int createRet = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);

    // If creation fails (e.g. relation already exists, disk full), propagate error.
    if (createRet != SUCCESS) {
        return createRet;
    }

    // Open the newly created target relation to get its rel-id for insertion.
    int targetRelId = OpenRelTable::openRel(targetRel);

    // If opening fails, clean up by deleting the just-created target relation
    // before returning the error — we don't want an orphaned relation on disk.
    if (targetRelId < 0) {
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Selecting and inserting matching records into the target relation ***/

    // Buffer to hold one record fetched from the source relation.
    Attribute record[src_nAttrs];

    // Reset the search index of the source relation in the relation cache
    // so that BlockAccess::search() starts scanning from the first record.
    RelCacheTable::resetSearchIndex(srcRelId);

    // Reset the search index of the select-condition attribute in the
    // attribute cache. This is required for B+ tree search correctness;
    // for linear search it is a no-op but kept for uniformity.
   // AttrCacheTable::resetSearchIndex(srcRelId, attr); check this-----------------------------------------------------------

    // Repeatedly search for records satisfying (attr op attrVal).
    // BlockAccess::search() fills `record` and returns SUCCESS each time
    // it finds a match; returns E_NOTFOUND when no more matches exist.
    while (BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS) {

        // Insert the matched record into the target relation.
        int insertRet = BlockAccess::insert(targetRelId, record);

        if (insertRet != SUCCESS) {
            // Insertion failed (e.g. disk full). Clean up:
            // close and delete the partially populated target relation,
            // then propagate the error to the caller.
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return insertRet;
        }
    }

    // All matching records have been copied. Close the target relation
    // through the Schema layer so cache entries are written back to disk.
    Schema::closeRel(targetRel);

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