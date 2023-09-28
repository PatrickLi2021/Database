package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	config "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/config"
	list "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/list"

	directio "github.com/ncw/directio"
)

// Page size - defaults to 4kb.
const PAGESIZE = int64(directio.BlockSize)

// Number of pages.
const NUMPAGES = config.NumPages

// Pagers manage pages of data read from a file.
type Pager struct {
	file         *os.File             // File descriptor.
	maxPageNum   int64                // The number of pages used by this database (number of pages in the current pager)
	ptMtx        sync.Mutex           // Page table mutex (this is a lock for the entire pager struct)
	freeList     *list.List           // Free page list.
	unpinnedList *list.List           // Unpinned page list.
	pinnedList   *list.List           // Pinned page list.
	pageTable    map[int64]*list.Link // Page table.
}

// Construct a new Pager.
func NewPager() *Pager {
	var pager *Pager = &Pager{}
	pager.pageTable = make(map[int64]*list.Link)
	pager.freeList = list.NewList()
	pager.unpinnedList = list.NewList()
	pager.pinnedList = list.NewList()
	frames := directio.AlignedBlock(int(PAGESIZE * NUMPAGES))
	for i := 0; i < NUMPAGES; i++ {
		frame := frames[i*int(PAGESIZE) : (i+1)*int(PAGESIZE)]
		page := Page{
			pager:    pager,
			pagenum:  NOPAGE,
			pinCount: 0,
			dirty:    false,
			data:     &frame,
		}
		pager.freeList.PushTail(&page)
	}
	return pager
}

// HasFile checks if the pager is backed by disk.
func (pager *Pager) HasFile() bool {
	return pager.file != nil
}

// GetFileName returns the file name.
func (pager *Pager) GetFileName() string {
	return pager.file.Name()
}

// GetNumPages returns the number of pages.
func (pager *Pager) GetNumPages() int64 {
	return pager.maxPageNum
}

// GetFreePN returns the next available page number.
func (pager *Pager) GetFreePN() int64 {
	// Assign the first page number beyond the end of the file.
	return pager.maxPageNum
}

// Open initializes our page with a given database file.
func (pager *Pager) Open(filename string) (err error) {
	// Create the necessary prerequisite directories.
	if idx := strings.LastIndex(filename, "/"); idx != -1 {
		err = os.MkdirAll(filename[:idx], 0775)
		if err != nil {
			return err
		}
	}
	// Open or create the db file.
	pager.file, err = directio.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	// Get info about the size of the pager.
	var info os.FileInfo
	var len int64
	if info, err = pager.file.Stat(); err == nil {
		len = info.Size()
		if len%PAGESIZE != 0 {
			return errors.New("open: DB file has been corrupted")
		}
	}
	// Set the number of pages and hand off initialization to someone else.
	pager.maxPageNum = len / PAGESIZE
	return nil
}

// Close signals our pager to flush all dirty pages to disk.
func (pager *Pager) Close() (err error) {
	// Prevent new data from being paged in.
	pager.ptMtx.Lock()
	// Check if all refcounts are 0.
	curLink := pager.pinnedList.PeekHead()
	if curLink != nil {
		fmt.Println("ERROR: pages are still pinned on close")
	}
	// Cleanup.
	pager.FlushAllPages()
	if pager.file != nil {
		err = pager.file.Close()
	}
	pager.ptMtx.Unlock()
	return err
}

// Populate a page's data field, given a pagenumber.
func (pager *Pager) ReadPageFromDisk(page *Page, pagenum int64) error {
	if _, err := pager.file.Seek(pagenum*PAGESIZE, 0); err != nil {
		return err
	}
	if _, err := pager.file.Read(*page.data); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// NewPage returns an unused buffer from the free or unpinned list
// the ptMtx should be locked on entry
func (pager *Pager) NewPage(pagenum int64) (*Page, error) {
	// Synchronize function
	pager.ptMtx.Lock()
	defer pager.ptMtx.Unlock()
	// Evict page from free list
	if (pager.freeList != nil || pager.freeList.PeekHead() != nil) {
		free_page := pager.freeList.PeekHead().GetKey().(*Page)
		free_page.pagenum = pagenum
		free_page.pager = pager
		*(free_page.data) = []byte{}
		return free_page, nil
	} else if (pager.unpinnedList.PeekHead() != nil && pager.HasFile()) {
		// Evict page from unpinned list
		evicted_page := pager.unpinnedList.PeekHead().GetKey().(*Page)
		pager.FlushPage(evicted_page)
		evicted_page.pagenum = pagenum
		evicted_page.pager = pager
		*(evicted_page.data) = []byte{}
		return evicted_page, nil
	} else {
		return nil, errors.New("Could not create new page")
	}
	// Input pagenum is the new pagenum for this new page, key is page number?
	// Check to see if there are still pages in FreeList. If there are, then return the page you found in FreeList and 
	// evict it from FreeList since it is no longer free. If there are no more pages in FreeList, then you want to evict
	// a page from unpinned list
}

// Whenever you want to get a new memory address to be mapped to a new logical page. Then if the pager is backed by disk, we check the unpinned list. You could use that memory to load the contents from another logical page. If you are backed by disk, you can flush the contents of the unpinned page to disk. If you're not backed by disk, you no longer have the option to move the data somewhere else, so you have to just leave it in the unpinned list.

// All pages in unpinned list have a pin count of 0

// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	if (pagenum < 0) {
		return nil, errors.New("Invalid page")
	} else if (pagenum >= 0 && pagenum <= pager.maxPageNum) {
		list_containing_page := page.pager.pageTable[pagenum].GetList()
		// If page is in unpinned list
		if (list_containing_page == page.pager.unpinnedList) {
		  page.Get()
			page.pager.pinnedList.PushTail(page)
		} else if (list_containing_page == page.pager.pinnedList) {
			page.Get()	
			return page, nil
		} else {
			return nil, errors.New("Page number is valid, but could not locate page in pager                 ")
		}
	} else if (pagenum > page.pager.maxPageNum) {
		page.pager.maxPageNum++
		new_page, _ := pager.NewPage(pagenum)
		new_page.Get()
		new_page.pager.pinnedList.PushTail(new_page)
		return new_page, nil
	} else {
		return nil, errors.New("Couldn't retrieve page")
	}

	// move page into pinned list
	// We check if a page is invalid if pagenum is <= 0 or pagenum is not in the page table
	// If the page is not in the page table at first, but it is a legal page number, we want to call newpage and increment maxpagenum
	// If logical page (AKA pagenum) is less than maxpage, then we want to get it from file
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	if (page.dirty && page.pager.HasFile()) {
		page.pager.file.WriteAt(*page.data, page.pagenum * 4096)
		page.Put()
		page.dirty = false
	}
	// To unpin pages, use the Put() function in page.go
}

// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	while (pager.unpinnedList.PeekHead() != nil) {
		current_head := pager.unpinnedList.PeekHead()..GetKey().(*Page)
		current_head
	}
	// Flush from both unpinned list and pinned list
}

// We should not be closing a pager that still has pinned pages