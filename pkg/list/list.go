package list

import (
	// "errors"
	// "fmt"
	// "io"
	// "strings"

	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Create a new list.
func NewList() *List {
	empty_list := List{head: nil, tail: nil}
	return &empty_list
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
	// panic("function not yet implemented");
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
	// panic("function not yet implemented");
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	new_elem := Link{list: list, prev: nil, next: list.head, value: value}
	list.head = &new_elem
	return &new_elem
	// panic("function not yet implemented");
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	new_elem := Link{list: list, prev: list.tail, next: nil, value: value}
	list.tail = &new_elem
	return &new_elem
	// panic("function not yet implemented");
}

// Find an element in a list given a boolean function, f, that evaluates to true on the desired element.
func (list *List) Find(f func(*Link) bool) *Link {
	var cur_elem *Link = list.head
	for cur_elem != nil {
		if f(cur_elem) == true {
			return cur_elem
		}
		cur_elem = cur_elem.next
	}
	return nil
	// panic("function not yet implemented");
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	var cur_elem *Link = list.head
	for cur_elem != nil {
		f(cur_elem)
	}
	cur_elem = cur_elem.next
	// panic("function not yet implemented");
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
	// panic("function not yet implemented");
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
	// panic("function not yet implemented");
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
	// panic("function not yet implemented");
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
	// panic("function not yet implemented");
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
	// panic("function not yet implemented");
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	var cur_elem *Link = link.list.head
	for cur_elem != nil {
		if *cur_elem == *link {
			var temp *Link = cur_elem.prev
			cur_elem.prev.next = cur_elem.next
			cur_elem.next.prev = temp
			cur_elem.next = nil
			cur_elem.prev = nil
		}
	}
	// panic("function not yet implemented");
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	panic("function not yet implemented");
}
