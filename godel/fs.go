package godel

import (
	"os"
	"strings"
)

func listSubfolders(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	subfolders := []string{}
	for _, entry := range entries {
		if entry.IsDir() {
			subfolders = append(subfolders, entry.Name())
		}
	}

	return subfolders, nil
}

func listFilesFilterFormat(path string, fmt string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	files := []string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			split := strings.Split(entry.Name(), ".")
			if len(split) != 2 {
				continue
			}

			if split[1] != fmt {
				continue
			}

			files = append(files, split[0])
		}
	}

	return files, nil
}
