#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    int numPinned = 0;

    while (true) {
        advanceClock();
        BufDesc *buf = &(bufTable[clockHand]);

        if (!buf->valid) {
            // Frame is not valid, use it directly
            frame = clockHand;
            return OK;
        }

        if (buf->pinCnt > 0) {
            // Frame is pinned, cannot replace
            numPinned++;
            if (numPinned >= numBufs) {
                // All frames are pinned
                return BUFFEREXCEEDED;
            }
            continue;
        }

        if (buf->refbit) {
            // Recently referenced, give it a second chance
            buf->refbit = false;
            continue;
        }

        // Found a frame to replace
        if (buf->dirty) {
            // Write dirty page back to disk
            Status status = buf->file->writePage(buf->pageNo, &(bufPool[buf->frameNo]));
            if (status != OK) {
                return UNIXERR;  // Return UNIXERR if writePage fails
            }
            bufStats.diskwrites++;
        }

        // Remove the page from the hash table
        Status status = hashTable->remove(buf->file, buf->pageNo);
        if (status != OK) {
            return HASHTBLERROR;
        }

        // Clear the frame for new use
        buf->Clear();

        frame = clockHand;
        return OK;
    }
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status;

    // Check if the page is already in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);

    if (status == OK) {
        // Page is in the buffer pool
        BufDesc *buf = &(bufTable[frameNo]);

        // Increment pin count and set refbit
        buf->pinCnt++;
        buf->refbit = true;

        // Return a pointer to the page
        page = &(bufPool[frameNo]);

        bufStats.accesses++;

        return OK;
    } else if (status == HASHNOTFOUND) {
        // Page is not in the buffer pool
        int frame;

        // Allocate a buffer frame
        status = allocBuf(frame);
        if (status != OK) {
            return status;  // Return BUFFEREXCEEDED if no frames are available
        }

        // Read the page from disk into the buffer pool
        status = file->readPage(PageNo, &(bufPool[frame]));
        if (status != OK) {
            return UNIXERR;  // Return UNIXERR if readPage fails
        }

        bufStats.diskreads++;
        bufStats.accesses++;

        // Insert the page into the hash table
        status = hashTable->insert(file, PageNo, frame);
        if (status != OK) {
            return HASHTBLERROR;
        }

        // Set up the BufDesc entry for the frame
        bufTable[frame].Set(file, PageNo);

        // Return a pointer to the page
        page = &(bufPool[frame]);

        return OK;
    } else {
        // An error occurred during lookup
        return HASHTBLERROR;
    }
}



const Status BufMgr::unPinPage(File* file, const int PageNo, 
                               const bool dirty) 
{
    int frameNo;
    Status status;

    // Check if the page is in the buffer pool
    status = hashTable->lookup(file, PageNo, frameNo);

    if (status != OK) {
        return HASHNOTFOUND;  // Page not found in buffer pool
    }

    BufDesc *buf = &(bufTable[frameNo]);

    if (buf->pinCnt == 0) {
        return PAGENOTPINNED;  // Page is not pinned
    }

    // Decrement the pin count
    buf->pinCnt--;

    // Set the dirty bit if necessary
    if (dirty) {
        buf->dirty = true;
    }

    return OK;
}


const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status;

    // Allocate a new page in the file
    status = file->allocatePage(pageNo);
    if (status != OK) {
        return UNIXERR;  // Return UNIXERR if allocatePage fails
    }

    // Allocate a buffer frame
    int frame;
    status = allocBuf(frame);
    if (status != OK) {
        return status;  // Return BUFFEREXCEEDED if no frames are available
    }

    // Initialize the page in the buffer pool
    memset(&(bufPool[frame]), 0, sizeof(Page));

    // Insert the page into the hash table
    status = hashTable->insert(file, pageNo, frame);
    if (status != OK) {
        return HASHTBLERROR;
    }

    // Set up the BufDesc entry for the frame
    bufTable[frame].Set(file, pageNo);

    // Return the page number and pointer to the page
    page = &(bufPool[frame]);

    bufStats.accesses++;

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


