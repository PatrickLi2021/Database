package btree

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"errors"

	pager "github.com/csci1270-fall-2023/dbms-projects-handout/pkg/pager"
)

// Split is a supporting data structure to propagate keys up our B+ tree.
type Split struct {
	isSplit bool  // A flag that's set if a split occurs.
	key     int64 // The key to promote.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
	err     error // Used to propagate errors upwards.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// Interface for main node functions.
	search(int64) int64
	insert(int64, int64, bool) Split
	delete(int64)
	get(int64) (int64, bool)

	// Interface for helper functions.
	keyToNodeEntry(int64) (*LeafNode, int64, error)
	printNode(io.Writer, string, string)
	getPage() *pager.Page
	getNodeType() NodeType
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////// Leaf Node Methods /////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) >= key
		},
	)
	return int64(minIndex)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	position := node.search(key)

	if position == node.numKeys && update{
		return Split{err: errors.New("Could not update item in leaf node")}
	} 
	if node.getEntry(position).GetKey() != key && update {
		return Split{err: errors.New("Update key not found in leaf node.")}
	} 
	if !update && position != node.numKeys && node.getKeyAt(position) == key {
		return Split{err: errors.New("Duplicate key found in leaf node.")}
	} 
	if update {
			node.updateValueAt(position, value)
			return Split{}
	} 

	for i := node.numKeys - 1; i >= position; i-- {
		current_key := node.getKeyAt(i)
		current_value := node.getValueAt(i)
		new_entry := BTreeEntry{key: current_key, value: current_value}
		node.modifyEntry(i + 1, new_entry)
	} 
	node.updateNumKeys(node.numKeys + 1)
	new_entry := BTreeEntry{key: key, value: value}
	node.modifyEntry(position, new_entry)

	if node.numKeys > ENTRIES_PER_LEAF_NODE {
		return node.split()
	} 
	return Split{}
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	needNodeIndex := node.search(key) // Search the key
	// Case if the key is not in the node on the last one
	if needNodeIndex >= node.numKeys || node.getKeyAt(needNodeIndex) != key {
		// Not in here!!
		return
	}
	// Shift the keys and values to the left
	for i := needNodeIndex; i < node.numKeys-1; i++ {
		node.updateKeyAt(i, node.getKeyAt(i+1))
		node.updateValueAt(i, node.getValueAt(i+1))
	}
	// Update the number of keys
	node.updateNumKeys(node.numKeys - 1)
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	new_leaf_node, new_leaf_err := createLeafNode(node.page.GetPager())
	if new_leaf_err != nil {
		return Split{err: new_leaf_err}
	}

	defer new_leaf_node.getPage().Put()
	midpoint_position := node.numKeys / 2 

	for i := midpoint_position; i < node.numKeys; i++ {
		current_key := node.getKeyAt(i)
		current_value := node.getValueAt(i)
		new_entry := BTreeEntry{key: current_key, value: current_value}
		new_leaf_node.modifyEntry(new_leaf_node.numKeys, new_entry)
		new_leaf_node.updateNumKeys(new_leaf_node.numKeys + 1)
	}
	node.updateNumKeys(midpoint_position)
	return Split{isSplit: true, key: new_leaf_node.getKeyAt(0), leftPN: node.page.GetPageNum(), rightPN: new_leaf_node.page.GetPageNum(), err: nil}
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	// Find index.
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Thank you Mario! But our key is in another castle!
		return 0, false
	}
	entry := node.getEntry(index)
	return entry.GetValue(), true
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *LeafNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	return node, node.search(key), nil
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
		entry := node.getEntry(cellnum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.GetKey(), entry.GetValue()))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | right sibling @ [%v]\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////// Internal Node Methods ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	// Binary search for the key.
	minIndex := sort.Search(
		int(node.numKeys),
		func(idx int) bool {
			return node.getKeyAt(int64(idx)) > key
		},
	)
	return int64(minIndex)
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	position := node.search(key)
	child_node, err := node.getChildAt(position)
	if err != nil {
		return Split{err: err}
	}
	defer child_node.getPage().Put()
	result := child_node.insert(key, value, update)
	if result.isSplit {
		split := node.insertSplit(result)
		return split
	}
	return Split{err: result.err}
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	split_pos := node.search(split.key)
	for i := node.numKeys - 1; i >= split_pos; i-- {
		node.updateKeyAt(i+1, node.getKeyAt(i))
	}
	node.updateNumKeys(node.numKeys + 1)
	node.updateKeyAt(split_pos, split.key)

	for i := node.numKeys - 1; i > split_pos ; i--  { 
		node.updatePNAt(i + 1, node.getPNAt(i))
	}
	// off by one error fix
	node.updatePNAt(split_pos + 1, split.rightPN)
	if node.numKeys > KEYS_PER_INTERNAL_NODE {
		return node.split()
	} 
	return Split{}
}
	
// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	// Get child.
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return
	}
	defer child.getPage().Put()
	// Delete from child.
	child.delete(key)
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	var split Split
	new_node, new_node_err := createInternalNode(node.page.GetPager())
	if new_node_err != nil {
		// could not create new leaf node
		split.err = new_node_err
		return split
	} 
	// new right internal node created successfully 
	defer new_node.getPage().Put()

	// fill in split struct values
	index_of_key := (node.numKeys -1) / 2 // index of node that should be propagated up

	for i := index_of_key; i < node.numKeys; i++ {
		// modify keys of new node now that space has been created
		new_node.updateKeyAt(new_node.numKeys, node.getKeyAt(i))
		new_node.updatePNAt(new_node.numKeys, node.getPNAt(i))
		new_node.updateNumKeys(new_node.numKeys + 1)
	}
	// off by one for ptrs and nodes 
	new_node.updatePNAt(new_node.numKeys, node.getPNAt(node.numKeys))

	node.updateNumKeys(index_of_key-1)

	split.isSplit = true
	split.err = nil
	split.key = node.getKeyAt(index_of_key-1) 
	split.leftPN = node.page.GetPageNum()
	split.rightPN = new_node.page.GetPageNum()

	return split

}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	// Find the child.
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	node.initChild(child)
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err != nil {
		return &LeafNode{}, 0, err
	}
	defer child.getPage().Put()
	return child.keyToNodeEntry(key)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
		if idx != node.numKeys {
			io.WriteString(w, fmt.Sprintf("\n%v[KEY] %v\n", nextPrefix, node.getKeyAt(idx)))
		}
	}
}