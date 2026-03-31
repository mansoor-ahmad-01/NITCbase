#include "Frontend.h"

#include <cstring>
#include <iostream>

int Frontend::create_table(char relname[ATTR_SIZE], int no_attrs, char attributes[][ATTR_SIZE],
                           int type_attrs[]) {
  // Schema::createRel
    return Schema::createRel(relname, no_attrs, attributes, type_attrs);

  return SUCCESS;
}

int Frontend::drop_table(char relname[ATTR_SIZE]) {
  // Schema::deleteRel
    return Schema::deleteRel(relname);

  return SUCCESS;
}

int Frontend::open_table(char relname[ATTR_SIZE]) {
  // Schema::openRel
  return Schema::openRel(relname);
  return SUCCESS;
}

int Frontend::close_table(char relname[ATTR_SIZE]) {
  // Schema::closeRel
  return Schema::closeRel(relname);
  return SUCCESS;
}

int Frontend::alter_table_rename(char relname_from[ATTR_SIZE], char relname_to[ATTR_SIZE]) {
  // Schema::renameRel
 return Schema::renameRel(relname_from,relname_to);
  return SUCCESS;
}

int Frontend::alter_table_rename_column(char relname[ATTR_SIZE], char attrname_from[ATTR_SIZE],
                                        char attrname_to[ATTR_SIZE]) {
  // Schema::renameAttr
  return Schema::renameAttr(relname,attrname_from,attrname_to);
  return SUCCESS;
}

int Frontend::create_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::createIndex
  return SUCCESS;
}

int Frontend::drop_index(char relname[ATTR_SIZE], char attrname[ATTR_SIZE]) {
  // Schema::dropIndex
  return SUCCESS;
}

int Frontend::insert_into_table_values(char relname[ATTR_SIZE], int attr_count, char attr_values[][ATTR_SIZE]) {
  // Algebra::insert
   return Algebra::insert(relname, attr_count, attr_values);
  return SUCCESS;
}

int Frontend::select_from_table(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE]) {

    
    return Algebra::project(relname_source, relname_target);
}

int Frontend::select_attrlist_from_table(char relname_source[ATTR_SIZE],
                                         char relname_target[ATTR_SIZE],
                                         int attr_count,
                                         char attr_list[][ATTR_SIZE]) {

   
    return Algebra::project(relname_source, relname_target, attr_count, attr_list);
}
int Frontend::select_from_table_where(char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
                                      char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {
  return Algebra::select(relname_source, relname_target, attribute, op, value);
}

int Frontend::select_attrlist_from_table_where(
    char relname_source[ATTR_SIZE], char relname_target[ATTR_SIZE],
    int attr_count, char attr_list[][ATTR_SIZE],
    char attribute[ATTR_SIZE], int op, char value[ATTR_SIZE]) {

    // Step 1: Perform the WHERE filtering first via Algebra::select().
    // This creates a temporary relation TEMP containing ALL attributes of
    // srcRel but only the records that satisfy the condition (attribute op value).
    // We use TEMP as an intermediate relation before applying the attribute projection.
    int selectRet = Algebra::select(relname_source, TEMP, attribute, op, value);

    // If the select fails (e.g. E_RELNOTOPEN, E_ATTRNOTEXIST, disk full),
    // propagate the error — TEMP was not created so nothing to clean up.
    if (selectRet != SUCCESS) {
        return selectRet;
    }

    // Step 2: Open the TEMP relation to make it available for the project operation.
    // TEMP was created and closed by Algebra::select(), so we need to reopen it.
    int tempRelId = OpenRelTable::openRel(TEMP);

    // If opening TEMP fails, delete it to avoid leaving an orphaned relation
    // on disk, then propagate the error.
    if (tempRelId < 0) {
        Schema::deleteRel(TEMP);
        return tempRelId;
    }

    // Step 3: Project the desired attributes from TEMP into the actual target relation.
    // This applies the attribute list filter on the already-filtered TEMP relation,
    // producing a relation with only the rows AND columns the user requested.
    int projectRet = Algebra::project(TEMP, relname_target, attr_count, attr_list);

    // Step 4: Close TEMP regardless of whether project succeeded or failed.
    // We must release the open relation table entry before deleting.
    OpenRelTable::closeRel(tempRelId);

    // Step 5: Delete TEMP — it was only an intermediate relation and should
    // not persist after this operation completes.
    Schema::deleteRel(TEMP);

    // Return the result of the project step (SUCCESS or an error code).
    return projectRet;
}

int Frontend::select_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                     char relname_target[ATTR_SIZE],
                                     char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE]) {
  // Algebra::join
  return SUCCESS;
}

int Frontend::select_attrlist_from_join_where(char relname_source_one[ATTR_SIZE], char relname_source_two[ATTR_SIZE],
                                              char relname_target[ATTR_SIZE],
                                              char join_attr_one[ATTR_SIZE], char join_attr_two[ATTR_SIZE],
                                              int attr_count, char attr_list[][ATTR_SIZE]) {
  // Algebra::join + project
  return SUCCESS;
}

int Frontend::custom_function(int argc, char argv[][ATTR_SIZE]) {
  // argc gives the size of the argv array
  // argv stores every token delimited by space and comma

  // implement whatever you desire

  return SUCCESS;
}