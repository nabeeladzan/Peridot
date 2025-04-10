// main.go
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/nabeeladzan/peridot/internal"
)

const nodeSize = 72 // 4 (ID) + 1 (InUse) + 1 (Padding) + 2 (Padding) + 64 (Value)

// getFree reads the head of the free list from freestore
func getFree(f *os.File) (uint32, error) {
	buf := make([]byte, 4)
	_, err := f.ReadAt(buf, 0)
	if err != nil {
		// If free list is empty, return ^uint32(0)
		return ^uint32(0), nil
	}
	return binary.LittleEndian.Uint32(buf), nil
}

// setFree writes the head of the free list to freestore
func setFree(f *os.File, id uint32) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, id)
	_, err := f.WriteAt(buf, 0)
	return err
}

// writeNode writes a new node, reusing free slot if available
func writeNode(nodestore, freestore *os.File, value string) error {
	freeID, err := getFree(freestore)
	if err != nil {
		return err
	}

	node := internal.Node{InUse: 1}

	// Encode value into fixed 64-byte field
	jsonVal, _ := json.Marshal(value)
	var fixed [64]byte
	copy(fixed[:], jsonVal)
	node.Value = fixed

	var offset int64
	if freeID != ^uint32(0) {
		// Reuse free node
		offset = int64(freeID) * nodeSize
		node.ID = freeID

		// Read the reused node to get its next free ID
		buf := make([]byte, nodeSize)
		_, err := nodestore.ReadAt(buf, offset)
		if err != nil {
			return err
		}
		nextFreeID := binary.LittleEndian.Uint32(buf[8:12]) // first 4 bytes of Value
		// Set new head of free list
		err = setFree(freestore, nextFreeID)
		if err != nil {
			return err
		}
	} else {
		// Append to end
		fi, err := nodestore.Stat()
		if err != nil {
			return err
		}
		offset = fi.Size()
		node.ID = uint32(offset / nodeSize)
	}

	// Serialize node
	buf := make([]byte, nodeSize)
	binary.LittleEndian.PutUint32(buf[0:], node.ID)
	buf[4] = node.InUse
	copy(buf[8:], node.Value[:])

	_, err = nodestore.WriteAt(buf, offset)
	return err
}

// deleteNode marks a node as free and adds it to the free list
func deleteNode(nodestore, freestore *os.File, id uint32) error {
	offset := int64(id) * nodeSize

	// Get current free list head
	currentHead, err := getFree(freestore)
	if err != nil {
		return err
	}

	// Prepare a blank node with InUse=0 and value containing next free ID
	var node internal.Node
	node.ID = id
	node.InUse = 0
	binary.LittleEndian.PutUint32(node.Value[0:], currentHead) // link to next free

	// Serialize
	buf := make([]byte, nodeSize)
	binary.LittleEndian.PutUint32(buf[0:], node.ID)
	buf[4] = node.InUse
	copy(buf[8:], node.Value[:])

	// Write node
	_, err = nodestore.WriteAt(buf, offset)
	if err != nil {
		return err
	}

	// Set new free list head
	return setFree(freestore, id)
}

// readNode reads a node by its ID from the file
func readNode(f *os.File, id uint32) (internal.Node, error) {
	offset := int64(id) * nodeSize
	buf := make([]byte, nodeSize)
	_, err := f.ReadAt(buf, offset)
	if err != nil {
		return internal.Node{}, err
	}

	node := internal.Node{
		ID:    binary.LittleEndian.Uint32(buf[0:4]),
		InUse: buf[4],
	}
	copy(node.Value[:], buf[8:72])
	return node, nil
}

// openStore opens a file with the given name
func openStore(name string) (*os.File, *os.File, error) {
	// if _free return
	// return the file handles
	nodestore, err := os.OpenFile(name+".db", os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("file %s does not exist", name)
	}

	freestore, err := os.OpenFile(name+"_free.db", os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("file %s_free does not exist", name)
	}

	return nodestore, freestore, nil
}

func createStore(name string) (*os.File, *os.File, error) {
	// Create the file handles
	nodestore, err := os.OpenFile(name+".db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s", name)
	}

	freestore, err := os.OpenFile(name+"_free.db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create file %s_free", name+"_free")
	}

	return nodestore, freestore, nil
}

func readStore(f *os.File) ([]internal.Node, error) {
	// Read all nodes from the file
	var nodes []internal.Node
	buf := make([]byte, nodeSize)
	for i := 0; ; i++ {
		_, err := f.ReadAt(buf, int64(i)*nodeSize)
		if err != nil {
			break // EOF or error
		}
		node := internal.Node{
			ID:    binary.LittleEndian.Uint32(buf[0:4]),
			InUse: buf[4],
		}
		copy(node.Value[:], buf[8:72])
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// command list
func comCreate(storename string) (*os.File, *os.File, error) {
	nodestore, freestore, err := createStore(storename)
	if err != nil {
		return nil, nil, err
	}
	return nodestore, freestore, nil
}

func comOpen(storename string) (*os.File, *os.File, error) {
	nodestore, freestore, err := openStore(storename)
	if err != nil {
		return nil, nil, err
	}
	return nodestore, freestore, nil
}

func comClose(storename string) error {
	nodestore, err := os.OpenFile(storename, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("file %s does not exist", storename)
	}
	defer nodestore.Close()

	freestore, err := os.OpenFile(storename+"_free", os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("file %s_free does not exist", storename+"_free")
	}
	defer freestore.Close()

	if err := nodestore.Close(); err != nil {
		return err
	}
	if err := freestore.Close(); err != nil {
		return err
	}
	return nil
}

func comInsert(store *Store, value string) error {
	// Insert a new node into the store
	err := writeNode(store.nodestore, store.freestore, value)
	if err != nil {
		return err
	}
	return nil
}

func comDelete(store *Store, id uint32) error {
	// Delete a node from the store
	err := deleteNode(store.nodestore, store.freestore, id)
	if err != nil {
		return err
	}
	return nil
}

func comReadAll(store *Store) error {
	// Read all nodes from the store
	nodes, err := readStore(store.nodestore)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if node.InUse == 1 {
			fmt.Printf("Node ID: %d, Value: %s\n", node.ID, string(node.Value[:]))
		}
	}
	return nil
}

type Store struct {
	name string
	// file pointer to the node store
	nodestore *os.File
	// file pointer to the free store
	freestore *os.File
}

func findStore(stores []Store, name string) (*Store, error) {
	for _, store := range stores {
		if store.name == name {
			return &store, nil
		}
	}
	return nil, fmt.Errorf("store %s not found", name)
}

func main() {
	fmt.Println("Peridot GraphDB Server")

	// array of store
	var stores []Store

	// detect .db files in the current directory
	files, err := os.ReadDir(".")
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if len(file.Name()) < 3 {
			continue
		}
		if strings.HasSuffix(file.Name(), "_free.db") {
			continue
		}

		if file.Name()[len(file.Name())-3:] == ".db" {
			// comOpen the store
			// remove the .db extension
			nodestore, freestore, err := comOpen(file.Name()[:len(file.Name())-3])
			if err != nil {
				fmt.Println("Error opening store:", err)
				continue
			}
			// append to the stores array
			stores = append(stores, Store{
				name:      file.Name()[:len(file.Name())-3],
				nodestore: nodestore,
				freestore: freestore,
			})
		}
	}

	// CLI for interacting with the database
	for {
		var command string
		// Peridot> prompt
		fmt.Print("Peridot> ")
		fmt.Scanln(&command)
		fmt.Println()
		switch command {
		case "list":
			// list all stores
			fmt.Println("Stores:")
			for _, store := range stores {
				fmt.Println(store.name)
			}
		case "create":
			// create a new store
			var storename string
			fmt.Print("Enter store name: ")
			fmt.Scanln(&storename)
			nodestore, freestore, err := comCreate(storename)
			if err != nil {
				fmt.Println("Error creating store:", err)
				continue
			}
			// append to the stores array
			stores = append(stores, Store{
				name:      storename,
				nodestore: nodestore,
				freestore: freestore,
			})
		case "insert":
			// insert a new node into the store
			var storename, value string
			fmt.Print("Enter store name: ")
			fmt.Scanln(&storename)
			fmt.Print("Enter value: ")
			fmt.Scanln(&value)
			// find the store in the stores array
			store, err := findStore(stores, storename)
			if err != nil {
				fmt.Println("Error finding store:", err)
				continue
			}
			// insert the value into the store
			err = comInsert(store, value)
			if err != nil {
				fmt.Println("Error inserting value:", err)
				continue
			}
			fmt.Println("Inserted value:", value)
		case "delete":
			// delete a node from the store
			var storename string
			var id uint32
			fmt.Print("Enter store name: ")
			fmt.Scanln(&storename)
			fmt.Print("Enter node ID: ")
			fmt.Scanln(&id)
			// find the store in the stores array
			store, err := findStore(stores, storename)
			if err != nil {
				fmt.Println("Error finding store:", err)
				continue
			}
			// delete the node from the store
			err = comDelete(store, id)
			if err != nil {
				fmt.Println("Error deleting node:", err)
				continue
			}
			fmt.Println("Deleted node ID:", id)
		case "read":
			// read all nodes from the store
			var storename string
			fmt.Print("Enter store name: ")
			fmt.Scanln(&storename)
			// find the store in the stores array
			store, err := findStore(stores, storename)
			if err != nil {
				fmt.Println("Error finding store:", err)
				continue
			}
			// read all nodes from the store
			err = comReadAll(store)
			if err != nil {
				fmt.Println("Error reading nodes:", err)
				continue
			}
		case "version":
			// print the version of the server
			fmt.Println("\nPeridot GraphDB Server v0.1")
			fmt.Println("Copyright (c) 2024 Muhammad Nabeel Adzan")
			fmt.Println("All rights reserved.")
			fmt.Println("This is free software; you are free to use it under the terms of the MIT License.")
			fmt.Println("This software is provided 'as is' without warranty of any kind.")
			fmt.Println("See the LICENSE file for more details.\n")
		case "help":
			// print the help message
			fmt.Println("Commands:")
			fmt.Println("list - list all stores")
			fmt.Println("create - create a new store")
			fmt.Println("insert - insert a new node into the store")
			fmt.Println("delete - delete a node from the store")
			fmt.Println("read - read all nodes from the store")
			fmt.Println("version - print the version of the server")
			fmt.Println("help - print this help message")
			fmt.Println("exit - close all stores and exit")
		case "exit":
			// close all stores and exit
			for _, store := range stores {
				err := comClose(store.name)
				if err != nil {
					fmt.Println("Error closing store:", err)
					continue
				}
			}
			fmt.Println("Exiting Peridot GraphDB Server")
			return
		default:
			fmt.Println("Unknown command:", command)
		}

	} // end of while loop
}
