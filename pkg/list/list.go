package list

import (
	// "errors"
	// "fmt"
	// "io"
	// "strings"

	"fmt"

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
	if list.head == nil || list.tail == nil {
		list.head = &new_elem
		list.tail = &new_elem
	} else {
		list.head.prev = &new_elem
		list.head = &new_elem
	}
	return &new_elem
	// panic("function not yet implemented");
}

// Add an element to the end of the list. Returns the added link.
func (list *List) PushTail(value interface{}) *Link {
	new_elem := Link{list: list, prev: list.tail, next: nil, value: value}
	if list.head == nil || list.tail == nil {
		list.head = &new_elem
		list.tail = &new_elem
	}
	list.tail.next = &new_elem	
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
		cur_elem = cur_elem.next
	}
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

// Print the value of every node in the list
func (list *List) PrintList(string, repl.REPLConfig) {
	var cur_elem *Link = list.head
	for cur_elem != nil {
		fmt.Println(cur_elem.value)
		cur_elem = cur_elem.next
	}
}

// Remove this link from its list.
func (link *Link) PopSelf() {
	if link == nil {
		return
		// If list contains only 1 node
	} else if link.prev == nil && link.next == nil {
		link.list.head = nil
		link.list.tail = nil
		return
	} else if link.prev == nil {
		link.next.prev = nil
		link.list.head = link.next
		return
	} else if link.next == nil {
		link.prev.next = nil
		link.list.tail = link.prev
	} else {
		var temp *Link = link.prev
		link.prev.next = link.next
		link.next.prev = temp
		link.next = nil
		link.prev = nil
		return
	}

	
	// var cur_elem *Link = link.list.head
	// for cur_elem != nil {
	// 	if cur_elem == link {
	// 		var temp *Link = cur_elem.prev
	// 		cur_elem.prev.next = cur_elem.next
	// 		cur_elem.next.prev = temp
	// 		cur_elem.next = nil
	// 		cur_elem.prev = nil
	// 	}
	// 	cur_elem = cur_elem.next
	// }
	// panic("function not yet implemented");
}

func (link *Link) Remove() {
	// if Find(link) == nil {
	// 	fmt.Println("not found")
	// 	return
	// } else {
	// 	var found_link *Link = list.Find(link)
	// 	found_link.PopSelf()
	// }
}

// List REPL.
func ListRepl(list *List) *repl.REPL {
	// new_repl := repl.NewRepl()
	// new_writer := io.Writer
	// new_repl_config := REPLConfig{writer: new_writer, clientId: uuid.New()}
	// new_repl.AddCommand("list_print", list.PrintList(string, ), "help")

	
	// REPL{commands: make(map[string]func(string, *REPLConfig) error), help: make(map[string]string)}


	panic("function not yet implemented");
}
