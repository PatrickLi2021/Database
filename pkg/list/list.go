package list

import (
	// "errors"
	// "fmt"
	// "io"
	// "strings"

	"errors"
	"fmt"

	repl "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/repl"
)

// List struct.
type List struct {
	head *Link
	tail *Link
}

// Error implements error.
func (*List) Error() string {
	panic("unimplemented")
}

// Create a new list.
func NewList() *List {
	empty_list := List{head: nil, tail: nil}
	return &empty_list
}

// Get a pointer to the head of the list.
func (list *List) PeekHead() *Link {
	return list.head
}

// Get a pointer to the tail of the list.
func (list *List) PeekTail() *Link {
	return list.tail
}

// Add an element to the start of the list. Returns the added link.
func (list *List) PushHead(value interface{}) *Link {
	new_elem := Link{list: list, prev: nil, next: list.head, value: value}
	if list.head == nil || list.tail == nil {
		list.head = &new_elem
		list.tail = &new_elem
	} else {
		list.head.prev = &new_elem
		list.head = &new_elem
	}
	return nil
}

func (list *List) PushTail(value interface{}) *Link {
	new_link := Link{list: list, prev: list.PeekTail(), next: nil, value: value}
	if list.PeekTail() != nil {
		list.PeekTail().next = &new_link
	} else {
		list.head = &new_link
	}
	list.tail = &new_link
	return &new_link
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
}

// Apply a function to every element in the list. f should alter Link in place.
func (list *List) Map(f func(*Link)) {
	var cur_elem *Link = list.head
	for cur_elem != nil {
		f(cur_elem)
		cur_elem = cur_elem.next
	}
}

// Link struct.
type Link struct {
	list  *List
	prev  *Link
	next  *Link
	value interface{}
}

// Error implements error.
func (*Link) Error() string {
	panic("unimplemented")
}

// Get the list that this link is a part of.
func (link *Link) GetList() *List {
	return link.list
}

// Get the link's value.
func (link *Link) GetKey() interface{} {
	return link.value
}

// Set the link's value.
func (link *Link) SetKey(value interface{}) {
	link.value = value
}

// Get the link's prev.
func (link *Link) GetPrev() *Link {
	return link.prev
}

// Get the link's next.
func (link *Link) GetNext() *Link {
	return link.next
}

// Print the value of every node in the list
func (list *List) PrintList() *List {
	var cur_elem *Link = list.head
	for cur_elem != nil {
		fmt.Println(cur_elem.value)
		cur_elem = cur_elem.next
	}
	return nil
}

func (link *Link) PopSelf() {
	if link == nil {
		return
	}
	if link.GetPrev() == nil && link.GetNext() == nil {
		// only link in list
		link.GetList().head = nil
		link.GetList().tail = nil
	}	
	if link.GetPrev() == nil && link.GetNext() != nil {
		// if link is at head of list
		link.GetNext().prev = link.GetPrev()
		link.GetList().head = link.GetNext()
	}

	// if link is at end of list
	if link.GetNext() == nil && link.GetPrev() != nil {
		link.GetPrev().next = link.GetNext()
		link.GetList().tail = link.GetPrev()
	}

	// if link is in middle of list
	if link.GetPrev() != nil && link.GetNext() != nil {
		link.GetPrev().next = link.GetNext()
		link.GetNext().prev = link.GetPrev()
	}
}

func (list *List) Remove(value interface{}) error {
	var found_link *Link = list.Find(func(link_to_compare *Link) bool {
		return value == link_to_compare.value
	})
	if found_link == nil {
		return errors.New("Link not found")
	} else {
		found_link.PopSelf()
	}
	return nil
}

// Checks to see if the input element is in the list
func (list *List) Contains(value interface{}) error {
	var cur_elem *Link = list.head
	for cur_elem != nil {
		if cur_elem.value == value{
			fmt.Print("found!")
			return nil
		}
		cur_elem = cur_elem.next
	}
	return errors.New("Couldn't find link")
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	new_repl := repl.NewRepl()
	new_repl.AddCommand("list_print", func(elt string, r *repl.REPLConfig) error { return list.PrintList() }, "Prints out all of the elements in the list in order, separated by commas (e.g. “0, 1, 2”)")
	new_repl.AddCommand("list_push_head", func(elt string, r *repl.REPLConfig) error { return list.PushHead(elt) }, "Inserts the given element to the List as a string”)")
	new_repl.AddCommand("list_remove", func(elt string, r *repl.REPLConfig) error { return list.Remove(elt) }, "Removes the given element from the list”)")
	new_repl.AddCommand("list_contains", func(elt string, r *repl.REPLConfig) error { return list.Contains(elt) }, "Prints “found!” if the element is in the list, prints “not found” otherwise”)")
	return new_repl
}