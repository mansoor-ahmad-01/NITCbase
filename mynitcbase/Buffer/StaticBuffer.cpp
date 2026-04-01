#include "StaticBuffer.h"
unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];
unsigned char StaticBuffer::blockAllocMap[DISK_BLOCKS];
StaticBuffer::StaticBuffer()
{   

    for(int i=0;i<BLOCK_ALLOCATION_MAP_SIZE;i++)
    {
        Disk::readBlock(blockAllocMap+(i*BLOCK_SIZE),i);
    }

    for(int i=0;i<BUFFER_CAPACITY;i++){
        metainfo[i].dirty=false;
        metainfo[i].free=true;
        metainfo[i].timeStamp=-1;
        metainfo[i].blockNum=-1;
    }

}

StaticBuffer::~StaticBuffer(){
    for(int i=0;i<BLOCK_ALLOCATION_MAP_SIZE;i++)
    {
        Disk::writeBlock(blockAllocMap+(i*BLOCK_SIZE),i);
    }
    for(int i=0;i<BUFFER_CAPACITY;i++){
        if(metainfo[i].free==false&&metainfo[i].dirty==true)
        {
            Disk::writeBlock(blocks[i],metainfo[i].blockNum);
        }
    }
}

// int StaticBuffer::getStaticBlockType(int blockNum){
//     if(blockNum<0||blockNum>=DISK_BLOCKS){
//         return E_OUTOFBOUND;
//     }

//     int blockType=(int)blockAllocMap[blockNum];
//     return blockType;
// }

int StaticBuffer::getBufferNum(int blockNum){
    if(blockNum<0||blockNum>=DISK_BLOCKS)
    return E_OUTOFBOUND;

    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
        if(metainfo[i].blockNum==blockNum)
        return i;
    }
    return E_BLOCKNOTINBUFFER;
}
// int StaticBuffer::setDirtyBit(int blockNum){
//    int index=getBufferNum(blockNum);
   
//    if(index==E_OUTOFBOUND)
//    return E_OUTOFBOUND;

//    if(index==E_BLOCKNOTINBUFFER)
//    return E_BLOCKNOTINBUFFER;

//    metainfo[index].dirty=true;
//    return SUCCESS;
// }


int StaticBuffer::getFreeBuffer(int blockNum) {
    if(blockNum<0||blockNum>=DISK_BLOCKS)
    return E_OUTOFBOUND;

    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
        if(metainfo[i].free==false)
        {
            metainfo[i].timeStamp++;
        }
    }

    int buffernum=-1;
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
        if(metainfo[i].free==true)
        {
            buffernum=i;
            break;
        }
    }

    int lar=-1;
    int index=-1;
    if(buffernum==-1){ 
    for(int i=0;i<BUFFER_CAPACITY;i++)
    {
        if(metainfo[i].free==false&&metainfo[i].timeStamp>lar)
        {
            lar=metainfo[i].timeStamp;
            index=i;
        }
    }

    if(metainfo[index].dirty==true)
    {
        Disk::writeBlock(blocks[index],metainfo[index].blockNum);
    }
    buffernum=index;
}
    metainfo[buffernum].blockNum=blockNum;
    metainfo[buffernum].dirty=false;
    metainfo[buffernum].free=false;
    metainfo[buffernum].timeStamp=0;
    return buffernum;
}

int StaticBuffer::setDirtyBit(int blockNum)
{
    int buffernum=getBufferNum(blockNum);
   if(buffernum==E_OUTOFBOUND)
    return buffernum;

    if(buffernum==E_BLOCKNOTINBUFFER)
    return E_BLOCKNOTINBUFFER;

    metainfo[buffernum].dirty=true;
    return SUCCESS;
}
int StaticBuffer::getStaticBlockType(int blockNum) {
    // Check if blockNum is valid (non-zero and less than number of disk blocks)
    // blockNum must be in range [0, DISK_BLOCKS-1] where DISK_BLOCKS = 8192 (from constants.h)
    // Note: Block 0 is valid (it's typically used for metadata)
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;  // Block number is out of valid range
    }
    
    // Access the entry in block allocation map corresponding to the blockNum argument
    // blockAllocMap is a static array that stores the type of each block on disk
    // Each entry is an unsigned char representing the block type
    unsigned char blockType = blockAllocMap[blockNum];
    
    // Return the block type after type casting to integer
    // Possible block types (from constants.h enum BlockType):
    //   REC = 0 (record block)
    //   IND_INTERNAL = 1 (internal index block)
    //   IND_LEAF = 2 (leaf index block)
    //   UNUSED_BLK = 3 (unused block)
    //   BMAP = 4 (block allocation map)
    return (int)blockType;
}