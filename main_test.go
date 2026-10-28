package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSplitInputsPreservesWordsAndOrder(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "source.txt")
	content := "first\tsecond\nthird  fourth"
	if err := os.WriteFile(input, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}

	paths, err := splitInputs([]string{input}, filepath.Join(root, "splits"), 7)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, strings.Fields(string(data))...)
	}
	want := strings.Fields(content)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split words = %v, want %v", got, want)
	}
}

func TestSplitInputsRejectsEmptyInputSet(t *testing.T) {
	root := t.TempDir()
	input := filepath.Join(root, "empty.txt")
	if err := os.WriteFile(input, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := splitInputs([]string{input}, filepath.Join(root, "splits"), 8); err == nil {
		t.Fatal("expected empty input to be rejected")
	}
}
