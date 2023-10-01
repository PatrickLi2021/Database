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
	// Evict page from free list
	if (pager.freeList != nil && pager.freeList.PeekHead() != nil) {
		free_page := pager.freeList.PeekHead().GetKey().(*Page)
		free_page.pagenum = pagenum
		free_page.pager = pager
		free_page.pager.pageTable[pagenum] = pager.freeList.PeekHead()
		// Remove this link from freeList
		free_page.pager.freeList.PeekHead().PopSelf()
		return free_page, nil
	} else if (pager.unpinnedList.PeekHead() != nil && pager.HasFile()) {
		// Evict page from unpinned list
		evicted_page := pager.unpinnedList.PeekHead().GetKey().(*Page)
		pager.FlushPage(evicted_page)
		delete(pager.pageTable, evicted_page.pagenum)
		evicted_page.pagenum = pagenum
		evicted_page.pager = pager
		evicted_page.pager.pageTable[pagenum] = pager.unpinnedList.PeekHead()
		return evicted_page, nil
	} else {
		return nil, errors.New("Could not create new page")
	}
}

// getPage returns the page corresponding to the given pagenum.
func (pager *Pager) GetPage(pagenum int64) (page *Page, err error) {
	if pagenum < 0 {
		return nil, errors.New("invalid page number")
	}	else {
		pager.ptMtx.Lock()
		new_pg, status := pager.pageTable[pagenum]

		if status { 
			lst_page := new_pg.GetList()
			if (lst_page == pager.pinnedList) {
				pg_pin := (new_pg.GetKey()).(*Page)
				pg_pin.Get()
				pager.ptMtx.Unlock()
				return pg_pin, nil
			} else {
				page_unpinned_list := (new_pg.GetKey()).(*Page)
				page_unpinned_list.Get()
				new_pg.PopSelf()
				pager.pinnedList.PushTail(page_unpinned_list)
				pager.ptMtx.Unlock()
				return page_unpinned_list, nil
			}
		} else {
			new_page, err := pager.NewPage(pagenum)
			if err == nil {
				pager.ReadPageFromDisk(new_page, pagenum)
				pager.pinnedList.PushTail(new_page)
				new_page.Get()
				pager.maxPageNum = pager.maxPageNum + 1
				pager.ptMtx.Unlock()
				return new_page, nil
			} else {
				pager.ptMtx.Unlock()
				return nil, errors.New("Could not get new page")
			}
		}
	}
}

// Flush a particular page to disk.
func (pager *Pager) FlushPage(page *Page) {
	if page.IsDirty() {
		page.GetPager().file.WriteAt(*page.GetData(), page.GetPageNum()*4096)
		page.SetDirty(false)
	}
}

// Flushes all dirty pages.
func (pager *Pager) FlushAllPages() {
	flush_link_func := func (link *list.Link) {
		pager.FlushPage((link.GetKey()).(*Page))
	}

	pager.pinnedList.Map(flush_link_func)
	pager.unpinnedList.Map(flush_link_func)
}